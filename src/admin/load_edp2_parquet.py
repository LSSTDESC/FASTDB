import argparse
import pathlib
import uuid
import textwrap

import numpy as np
from psycopg import sql
import nested_pandas

from db import ( DBCon, RootDiaObject, BaseProcessingVersion, DiaObject, DiaObjectPosition,
                 DiaSource, DiaSourceExtra, DiaForcedSource, DiaForcedSourceExtra )
from util import FDBLogger, asUUID


class EDP2Loader:
    def __init__( self, processing_version, create_pv=False ):
        with DBCon( dictcursor=True ) as pgdb:
            try:
                pgdb.execute( "LOCK TABLE processing_version" )
                pvid = None
                try:
                    pvid = asUUID( processing_version )
                    processing_version = pvid
                    col = 'id'
                except Exception:
                    col = 'description'
                rows = pgdb.execute( sql.SQL( "SELECT * FROM processing_version WHERE {col}={pv}" )
                                     .format( col=sql.Identifier(col), pv=processing_version ) )
                if len(rows) > 0:
                    pvid = rows[0]['id']
                else:
                    rows = pgdb.execute( sql.SQL( "SELECT * FROM processing_version_alias WHERE description={pv}" )
                                         .format( pv=processing_version ) )
                    if len(rows) > 0:
                        pvid = rows[0]['procver_id']

                if pvid is None:
                    if create_pv:
                        pvid = uuid.uuid4()
                        pgdb.execute_nofetch(
                            sql.SQL( "INSERT INTO processing_version(id, description) "
                                     "VALUES( {pid}, {desc} )" )
                            .format( pid=pvid, desc=processing_version ) )
                    else:
                        raise ValueError( f"Unknown processing version {processing_version}" )

                rows = pgdb.execute( sql.SQL( textwrap.dedent(
                    """\
                    SELECT b.*
                    FROM (
                      SELECT DISTINCT ON(_table) base_procver_id, _table
                      FROM base_procver_of_procver
                      WHERE procver_id={pv}
                        AND _table IN ('diaobject', 'diaobject_position', 'diasource', 'diaforcedsource')
                      ORDER BY _table, priority DESC
                    ) bpop
                    INNER JOIN base_processing_version b ON b.id=bpop.base_procver_id
                    """
                ) ).format( pv=pvid ) )
                rowdict = { row['_table'] : row for row in rows }

                missing = { 'diaobject', 'diaobject_position', 'diasource', 'diaforcedsource' } - set( rowdict.keys() )
                if len(missing) > 0:
                    if create_pv:
                        for table in missing:
                            rowdict[table] = { 'id': uuid.uuid4(),
                                               'description': processing_version,
                                               '_table': table }
                            pgdb.execute( sql.SQL( textwrap.dedent(
                                """\
                                INSERT INTO base_processing_version(id,description,_table)
                                VALUES({id},{description},{_table})
                                """
                            ) ).format( **(rowdict[table]) ) )
                            pgdb.execute( sql.SQL( textwrap.dedent(
                                """\
                                INSERT INTO base_procver_of_procver(procver_id, base_procver_id, priority, _table)
                                VALUES ({pvid}, {bpvid}, 0, {table})
                                """
                            ) ).format( pvid=pvid, bpvid=rowdict[table]['id'], table=table ) )
                    else:
                        raise RuntimeError( f"Can't find base provenance id for tables: {missing}" )

                pgdb.commit()

            finally:
                # Make sure to release the lock on processing_version
                pgdb.rollback()

        self.objbpv = BaseProcessingVersion( **(rowdict['diaobject']) )
        self.posbpv = BaseProcessingVersion( **(rowdict['diaobject_position']) )
        self.srcbpv = BaseProcessingVersion( **(rowdict['diasource']) )
        self.frcbpv = BaseProcessingVersion( **(rowdict['diaforcedsource']) )


    def do_one_file( self, filepath ):
        FDBLogger.info( f"Loading from file {filepath}..." )
        df = nested_pandas.read_parquet( filepath )

        # Verify that df.diaObjectId is unique... I really hope it is
        if ( len( df.diaObjectId ) != len( set( df.diaObjectId ) ) ):
            raise RuntimeError( f"diaObjectIds are duplicated within file {filepath}" )

        # Find root objects, create new ones as necessary
        # Do this inside its own block because we want to lock the root table to avoid duplicates
        with DBCon() as dbcon:
            try:
                FDBLogger.info( f"Searching for existing root_diaobject for {len(df)} diaobjects..." )
                knownobjects = { o: { 'ra': ra, 'dec': dec, 'base_procver_id': self.objbpv.id }
                                  for o, ra, dec in zip( df.diaObjectId, df.ra, df.dec ) }
                dbcon.execute_nofetch( "LOCK TABLE root_diaobject" )
                dbcon.execute( "CREATE TEMP TABLE temp_ra_dec(diaobjectid bigint, "
                               "ra double precision, dec double precision)" )
                with dbcon.cursor.copy( "COPY temp_ra_dec(diaobjectid, ra, dec) FROM STDIN" ) as copier:
                    for oid, row in knownobjects.items():
                        copier.write_row( [ oid, row['ra'], row['dec'] ] )
                rows, _cols = dbcon.execute( "SELECT t.diaobjectid, r.id FROM temp_ra_dec t "
                                             "INNER JOIN root_diaobject r "
                                             "  ON q3c_join(t.ra, t.dec, r.ra, r.dec, 1./3600.)" )
                oldroots = { r[0]: r[1] for r in rows }
                currentobjs = set( [ int(i) for i in df.diaObjectId ] )
                missings = set( currentobjs ) - set( oldroots.keys() )
                FDBLogger.info( f"...found {len(oldroots)} existing root ids, making {len(missings)} new ones..." )

                newroots = { m: uuid.uuid4() for m in missings }
                # ...since dictionaries aren't guaranteed sorted, let's not assume that
                #   you even get the keys back in the same order more than once.
                #   (So I don't want "for k in newroots.keys() on more than one line below.)
                objkeys = list( newroots.keys() )
                newrootdata = { 'id': [ newroots[k] for k in objkeys ],
                                'ra': [ knownobjects[k]['ra'] for k in objkeys ],
                                'dec': [ knownobjects[k]['dec'] for k in objkeys ] }
                RootDiaObject.bulk_insert_or_upsert( newrootdata, dbcon=dbcon )
                FDBLogger.info( "...done with root ids." )

            finally:
                dbcon.execute_nofetch( "DROP TABLE IF EXISTS temp_ra_dec" )
                dbcon.commit()
                # Make sure to release that lock!  ...though I think the
                #   dbcon.commit() I just did already released it...
                dbcon.rollback()

        # Figure out all the other stuff to be inserted

        FDBLogger.info( "Building tables to insert objects, sources, forced sources" )

        # diaobject
        objdata = {
            'diaobjectid': df.diaObjectId.to_numpy(),
            'rootid': [ oldroots[o] if o in oldroots else newroots[o] for o in df.diaObjectId ],
            'base_procver_id': np.full( (len(df),), self.objbpv.id )
        }

        # diaobject_position
        posdata = {
            'diaobjectid': df.diaObjectId.to_numpy(),
            'ra': df.ra.to_numpy(),
            'dec': df.dec.to_numpy(),
            'base_procver_id': np.full( (len(df),), self.posbpv.id )
        }

        # diasource
        sourcedf = ( df.loc[ :, ['diaObjectId', 'diaSource'] ]
                     .set_index( 'diaObjectId' )
                     .explode( 'diaSource' )
                     .reset_index() )
        cols = sourcedf.columns.to_list()
        sourcecols = []
        sourceextracols = []
        unknown = set()
        for col in cols:
            if ( ( col in DiaSourceExtra._flags_bits.values() ) or
                 ( col in DiaSourceExtra._pixelflags_bits.values() ) ):
                continue
            whatisthis = True
            if col.lower() in DiaSource.tablemeta().keys():
                sourcecols.append( col )
                whatisthis = False
            if col.lower() in DiaSourceExtra.tablemeta().keys():
                sourceextracols.append( col )
                whatisthis = False
            if whatisthis:
                unknown.add( col )
        if len(unknown) != 0:
            raise ValueError( f"Unknown diaSource columns {unknown}" )

        sourcedata = { c.lower(): getattr(sourcedf, c).to_numpy() for c in sourcecols }
        sourcedata['base_procver_id'] = np.full( (len(sourcedf),), self.srcbpv.id )
        sourceextradata = { c.lower(): getattr(sourcedf, c).to_numpy() for c in sourceextracols }
        sourceextradata['base_procver_id'] = np.full( (len(sourcedf),), self.srcbpv.id )
        sourceextradata['flags'] = np.full( (len(sourcedf),), 0, dtype=np.int32 )
        sourceextradata['pixelflags'] = np.full( (len(sourcedf),), 0, dtype=np.int32 )
        for mask, col in DiaSourceExtra._flags_bits.items():
            if col in cols:
                sourceextradata['flags'] = np.bitwise_and( sourceextradata['flags'],
                                                           sourcedf[col].to_numpy() * mask )
        for mask, col in DiaSourceExtra._pixelflags_bits.items():
            if col in cols:
                sourceextradata['pixelflags'] = np.bitwise_and( sourceextradata['pixelflags'],
                                                                sourcedf[col].to_numpy() * mask )

        # diaforcedsource
        forceddf = ( df.loc[ :, ['diaObjectId', 'diaObjectForcedSource'] ]
                     .set_index( 'diaObjectId' )
                     .explode( 'diaObjectForcedSource' )
                     .reset_index() )
        cols = forceddf.columns.to_list()
        forcedcols = []
        forcedextracols = []
        unknown = set()
        for col in cols:
            whatisthis = True
            if ( ( col in DiaForcedSourceExtra._flags_bits.values() ) or
                 ( col in DiaForcedSourceExtra._pixelflags_bits.values() ) ):
                continue
            if col.lower() in DiaForcedSource.tablemeta().keys():
                forcedcols.append( col )
                whatisthis = False
            if col.lower() in DiaForcedSourceExtra.tablemeta().keys():
                forcedextracols.append( col )
                whatisthis = False
            if whatisthis:
                unknown.add( col )
        if len(unknown) != 0:
            raise ValueError( f"Unknown diaObjectForcedSource columns {unknown}" )

        forceddata = { c.lower(): getattr(forceddf, c).to_numpy() for c in forcedcols }
        forceddata['base_procver_id'] = np.full( (len(forceddf),), self.frcbpv.id )
        forcedextradata = { c.lower(): getattr(forceddf, c).to_numpy() for c in forcedextracols }
        forcedextradata['base_procver_id'] = np.full( (len(forceddf),), self.frcbpv.id )
        forcedextradata['flags'] = np.full( (len(forceddf),), 0, dtype=np.int32 )
        forcedextradata['pixelflags'] = np.full( (len(forceddf),), 0, dtype=np.int32 )
        for mask, col in DiaForcedSourceExtra._flags_bits.items():
            if col in cols:
                forcedextradata['flags'] = np.bitwise_and( forcedextradata['flags'],
                                                           forceddf[col].to_numpy() * mask )
        for mask, col in DiaForcedSourceExtra._pixelflags_bits.items():
            if col in cols:
                forcedextradata['pixelflags'] = np.bitwise_and( forcedextradata['pixelflags'],
                                                           forceddf[col].to_numpy() * mask )


        #  Actually insert.  We want to do this all in one transaction, so we have to
        #    do the copying from the temp table outside bulk_insert_or_upsert (see that
        #    method's docstring, and the note on nocommit).
        FDBLogger.info( f"Inserting {len(objdata['diaobjectid'])} diaobjects, "
                        f"{len(posdata['diaobjectid'])} positions, "
                        f"{len(sourcedata['diasourceid'])} sources, "
                        f"{len(forceddata['diaobjectid'])} forced sources." )
        with DBCon() as dbcon:
            DiaObject.bulk_insert_or_upsert( objdata, dbcon=dbcon, nocommit=True )
            dbcon.execute( "INSERT INTO diaobject SELECT * FROM temp_bulk_upsert" )
            DiaObjectPosition.bulk_insert_or_upsert( posdata, dbcon=dbcon, nocommit=True )
            dbcon.execute( "INSERT INTO diaobject_position SELECT * FROM temp_bulk_upsert" )
            DiaSource.bulk_insert_or_upsert( sourcedata, dbcon=dbcon, nocommit=True )
            dbcon.execute( "INSERT INTO diasource SELECT * FROM temp_bulk_upsert" )
            DiaSourceExtra.bulk_insert_or_upsert( sourceextradata, dbcon=dbcon, nocommit=True )
            dbcon.execute( "INSERT INTO diasource_extra SELECT * FROM temp_bulk_upsert" )
            DiaForcedSource.bulk_insert_or_upsert( forceddata, dbcon=dbcon, nocommit=True )
            dbcon.execute( "INSERT INTO diaforcedsource SELECT * FROM temp_bulk_upsert" )
            DiaForcedSourceExtra.bulk_insert_or_upsert( forcedextradata, dbcon=dbcon, nocommit=True )
            dbcon.execute( "INSERT INTO diaforcedsource_extra SELECT * FROM temp_bulk_upsert" )

            dbcon.commit()

        FDBLogger.info( f"....done loading file {filepath}" )


    def do_directory( self, direc ):
        direc = pathlib.Path( direc )
        subdirs = []
        pqs = []
        for item in direc.iterdir():
            if item.is_dir():
                subdirs.append( item )
            elif ( len(item.name) >= 8 ) and ( item.name[-8:] == '.parquet' ):
                pqs.append( item )

        FDBLogger.info( f"Loading {len(pqs)} parquet files from {direc}..."  )
        for item in pqs:
            self.do_one_file( item )

        for subdir in subdirs:
            self.do_directory( subdir )


# ======================================================================

def main():
    parser = argparse.ArgumentParser( 'load_edp2_parquet.py',
                                      description="Load EDP2 parquest files underneath a directory." )
    parser.add_argument( "direc", help="Top level directory to search for .parquet files (recurses)" )
    parser.add_argument( "-p", "--processing-version", required=True,
                         help="Description (or uuid) of the processing version to import these to." )
    parser.add_argument( "-c", '--create-processing-version', default=False, action='store_true',
                         help=( "Normally, the processing version must already exist.  Set this to create it "
                                "if it doesn't." ) )
    args = parser.parse_args()

    loader = EDP2Loader( args.processing_version, create_pv=args.create_processing_version )
    loader.do_directory( args.direc )


# ======================================================================
if __name__ == "__main__":
    main()
