import re

import psycopg
import psycopg.rows

from db import DB
from util import logger


class FastDBLoader:
    """This is a wrapper class for bulk loading FASTDB.

    This class is for loading the tables:

        diaobject
        diasource
        diaforcedsource

    with side-effects on

        processing_version
        snapshot
        diasource_snapshot
        diaforcedsource_snapshot

    """

    def __init__( self ):
        self._all_tables = [ 'wantedspectra', 'plannedspectra', 'spectruminfo',
                             'host_galaxy', 'root_diaobject', 'diaobject', 'diaobject_root_map',
                             'diasource', 'diaforcedsource',
                             'processing_version_alias', 'processing_version', 'snapshot',
                             'diaobject_snapshot', 'diasource_snapshot', 'diaforcedsource_snapshot',
                            ]

    def disable_indexes_and_fks( self ):
        """This is scary.  It disables all indexes and foreign keys on the tables to be loaded.

        This can greatly improve the bulk loading time.  But, of course,
        it changes the database structure, which is scary.  It writes a
        file "load_snana_fits_reconstruct_indexes_constraints.sql" which
        can be feed through psql to undo the damage.

        """

        tables = self._all_tables
        tableindexes = {}
        indexreconstructs = []
        tableconstraints = {}
        constraintreconstructs = []
        tablepkconstraints = {}
        primarykeys = {}
        pkreconstructs = []

        pkmatcher = re.compile( r'^ *PRIMARY KEY \((.*)\) *$' )
        pkindexmatcher = re.compile( r' USING .* \((.*)\) *$' )

        with DB() as conn:
            cursor = conn.cursor( row_factory=psycopg.rows.dict_row )

            # Find all constraints (including primary keys)
            for table in tables:
                tableconstraints[table] = []
                cursor.execute( f"SELECT table_name, conname, condef, contype "
                                f"FROM "
                                f"  ( SELECT conrelid::regclass::text AS table_name, conname, "
                                f"           pg_get_constraintdef(oid) AS condef, contype "
                                f"    FROM pg_constraint WHERE conparentid=0 "
                                f"  ) subq "
                                f"WHERE table_name='{table}'" )
                rows = cursor.fetchall()
                for row in rows:
                    if row['contype'] == 'p':
                        if table in primarykeys:
                            raise RuntimeError( f"{table} has multiple primary keys!" )
                        match = pkmatcher.search( row['condef'] )
                        if match is None:
                            raise RuntimeError( f"Failed to parse {row['condef']} for primary key" )
                        primarykeys[table] = match.group(1)
                        tablepkconstraints[table] = row['conname']
                        pkreconstructs.insert( 0, ( f"ALTER TABLE {table} ADD CONSTRAINT "
                                                    f"{row['conname']} {row['condef']};" ) )
                    else:
                        tableconstraints[table].append( row['conname'] )
                        constraintreconstructs.insert( 0, ( f"ALTER TABLE {table} ADD CONSTRAINT {row['conname']} "
                                                            f"{row['condef']};" ) )

            # Make sure we found the primary key for all tables
            missing = []
            for table in tables:
                if table not in primarykeys:
                    missing.append( table )
            if len(missing) > 0:
                raise RuntimeError( f'Failed to find primary key for: {[",".join(missing)]}' )

            # Now do table indexes
            for table in tables:
                tableindexes[table] = []
                cursor.execute( f"SELECT * FROM pg_indexes WHERE tablename='{table}'" )
                rows = cursor.fetchall()
                for row in rows:
                    match = pkindexmatcher.search( row['indexdef'] )
                    if match is None:
                        raise RuntimeError( f"Error parsing index def {row['indexdef']}" )
                    if match.group(1) == primarykeys[table]:
                        # The primary key index will be deleted when
                        #  the primary key constraint is deleted
                        continue
                    if row['indexname'] in tableconstraints[table]:
                        # It's possible the index is already present in table constraints,
                        #   as a UNIQUE constraint will also create an index.
                        continue
                    tableindexes[table].append( row['indexname'] )
                    indexdef = row['indexdef']
                    # SCARY HACK ALERT.  The indexes were coming up with ON ONLY in their
                    #   definition, which meant that indexes were only being created on
                    #   the table defintion of a partitioned table, not on the actual
                    #   partitions.  One (probably better) solution would be to parse out
                    #   all partitioned tables and try to create all those indices as
                    #   well.  But, the hack here is I'm just to replace ON ONLY with ON
                    #   in index creation so that the index creation will recurse.  This
                    #   makes me a little afraid, but it seems to work....
                    indexdef = indexdef.replace( "ON ONLY", "ON" )
                    indexreconstructs.insert( 0, f"{indexdef};" )

            # Save the reconstruction
            with open( "load_snana_fits_reconstruct_indexes_constraints.sql", "w" ) as ofp:
                for row in pkreconstructs:
                    ofp.write( f"{row}\n" )
                for row in indexreconstructs:
                    ofp.write( f"{row}\n" )
                for row in constraintreconstructs:
                    ofp.write( f"{row}\n" )

            # Remove non-primary key constrinats
            for table in tableconstraints.keys():
                logger.warning( f"Dropping non-pk constraints from {table}" )
                for constraint in tableconstraints[table]:
                    cursor.execute( f"ALTER TABLE {table} DROP CONSTRAINT {constraint}" )

            # Remove indexes
            for table in tableindexes.keys():
                logger.warning( f"Dropping indexes from {table}" )
                for dex in tableindexes[table]:
                    cursor.execute( f"DROP INDEX {dex}" )

            # Remove primary keys
            for table, constraint in tablepkconstraints.items():
                logger.warning( f"Dropping primary key from {table}" )
                cursor.execute( f"ALTER TABLE {table} DROP CONSTRAINT {constraint}" )

            # OMG
            conn.commit()


    def recreate_indexes_and_fks( self, commandfile='load_snana_fits_reconstruct_indexes_constraints.sql' ):
        """Restore indexes and constraints destroyed by disable_indexes_and_fks()"""

        with open( commandfile ) as ifp:
            commands = ifp.readlines()

        with DB() as conn:
            cursor = conn.cursor( row_factory=psycopg.rows.dict_row )
            for command in commands:
                logger.info( f"Running {command}" )
                cursor.execute( command )

            conn.commit()
