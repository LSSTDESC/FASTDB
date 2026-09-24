import time
import uuid

import numpy as np
import scipy.stats
from psycopg import sql

from db import DBCon
from util import FDBLogger


def test_rootstress():
    # norigroots = 10000000
    # ninserts = [ 10000 ] # [ 10000, 100000, 10000 ]
    norigroots = 100000
    ninserts = [ 1000 ]
    overlapfrac = 0.1


    # Verify that there are no root ids
    go_nuclear = False
    with DBCon() as con:
        rows, _cols = con.execute( "SELECT COUNT(*) FROM root_diaobject" )
        if rows[0][0] != 0:
            raise RuntimeError( "root_diaobject table isn't empty" )
    go_nuclear = True
        
    try:
        # Create a large number of root diaobjects, then see how slow it is to insert

        rng = np.random.default_rng( 42 )
        # RA and dec uniformly on a sphere

        FDBLogger.info( f"Generating {norigroots} ra/dec..." )
        unitvecs = scipy.stats.uniform_direction( 3 ).rvs( norigroots, random_state=rng )
        decs = 90. - ( 180. / np.pi * np.arccos( unitvecs[:, 2] ) )
        ras = 180. + 180. / np.pi * np.atan2( unitvecs[:, 1], unitvecs[:, 0] )
        FDBLogger.info( f"...generating {norigroots} uuids..." )
        uuids = np.array( [ uuid.uuid4() for i in range(norigroots) ] )
        FDBLogger.info( f"...done generating original roots." )

        FDBLogger.info( f"Inserting {norigroots} original roots..." )
        with DBCon() as con:
            with con.cursor.copy( "COPY root_diaobject(id,ra,dec) FROM stdin" ) as copier:
                for rid, ra, dec in zip( uuids, ras, decs ):
                    copier.write_row( [ rid, ra, dec ] )
            con.commit()
        FDBLogger.info( "...done." )


        for num_to_insert in ninserts:
            nold = int( num_to_insert * overlapfrac )
            nnew = num_to_insert - nold
            FDBLogger.info( f"Generating {nold} positions that pre-exist and {nnew} that (probably) don't" )
            # Pick nold offsets that are between 0 and 1"; do it uniformly because why not
            roffs = np.sqrt( rng.uniform( size=nold ) ) / 3600.
            phis = rng.uniform( 0., 2.*np.pi, size=nold )
            # Match them to randomly selecting original objects
            # (This is not the same as pre-existing objects if this is not the first time through this loop!)
            dexen = np.arange( norigroots, dtype=int )
            rng.shuffle( dexen )
            dexen = dexen[ 0:nold ]
            olddecs = decs[dexen] + roffs * np.cos( phis )
            # ...yeah, edge effects, wahtever
            olddecs[ olddecs > 90.  ] = 89.99999
            olddecs[ olddecs < -90. ] = -89.99999
            oldras = ras[dexen] + roffs * np.sin( phis ) / np.cos( olddecs * np.pi / 180. )
            while any( oldras > 360. ):
                oldras[ oldras > 360. ] -= 360.
            while any( oldras < 0. ):
                oldras[ oldras < 0. ] += 360.
            unitvecs = scipy.stats.uniform_direction( 3 ).rvs( nnew, random_state=rng )
            newdecs = 90. - ( 180. / np.pi * np.arccos( unitvecs[:, 2] ) )
            newras = 180. + 180. / np.pi * np.atan2( unitvecs[:, 1], unitvecs[:, 0] )
            
            newras = np.concatenate( [ oldras, newras ] )
            newdecs = np.concatenate( [ olddecs, newdecs ] )
            # Shuffle them just so the pre-existing ones don't all come first
            dexen = np.arange( len(newras), dtype=int )
            rng.shuffle( dexen )
            newras = newras[ dexen ]
            newdecs = newdecs[ dexen ]

            # Create a temp table with the new ras and decs
            with DBCon() as con:
                FDBLogger.info( f"Uploading {len(newras)} new coordinates to temp table" )
                con.execute_nofetch( "CREATE TEMP TABLE newcoord(id uuid, ra double precision, dec double precision)" )
                with con.cursor.copy( "COPY newcoord(id, ra,dec) FROM stdin" ) as copier:
                    for ra, dec in zip( newras, newdecs ):
                        copier.write_row( [ None, ra, dec ] )

                FDBLogger.info( f"Matching pre-existing root objects to new ones..." )
                # Do this like source_importer.py does
                con.execute_nofetch( sql.SQL( "UPDATE newcoord n SET id=r.id "
                                              "FROM root_diaobject r\n"
                                              "WHERE q3c_radial_query(r.ra, r.dec, n.ra, n.dec, {rad})"
                                             ).format( rad=1./3600. ),
                                     echo=True, explain=True, analyze=True )
                FDBLogger.info( f"...done matching." )
                rows, _cols = con.execute( "SELECT COUNT(*) FROM newcoord WHERE id IS NOT NULL" )
                FDBLogger.info( f"{rows[0][0]} new root objects matched an existing root object" )
                FDBLogger.info( f"Creating temp table that will be added to root diaobjects..." )
                con.execute_nofetch( "CREATE TEMP TABLE temp_new_root_obj"
                                     "(id uuid, ra double precision, dec double precision)" )
                con.execute( "INSERT INTO temp_new_root_obj(id, ra, dec) "
                             "( SELECT gen_random_uuid(), ra, dec FROM newcoord "
                             "  WHERE id IS NULL )" )

                FDBLogger.info( f"Making temp table of new root objects that don't match existing." )
                # con.execute_nofetch( "/*+ IndexScan(r ix_rootdiaobject_q3c) "
                #                      "*/ "
                #                      "CREATE TEMP TABLE temp_new_root_obj AS "
                #                      "SELECT n.ra, n.dec "
                #                      "FROM newcoord n "
                #                      "INNER JOIN root_diaobject r "
                #                      "  ON NOT q3c_join(n.ra, n.dec, r.ra, r.dec, 0.0002777777778)",
                #                      echo=True, explain=True, analyze=True )
                FDBLogger.info( f"...done creating temp table." )
                FDBLogger.info( f"Adding new root objects to root_diaobject..." )
                con.execute( "INSERT INTO root_diaobject(id,ra,dec) ("
                             "  SELECT id,ra,dec FROM temp_new_root_obj )" )
                # con.execute( "INSERT INTO root_diaobject(id,ra,dec) ("
                #              "  SELECT gen_random_uuid(),ra,dec FROM temp_new_root_obj )" )
                FDBLogger.info( f"...done adding new root objects to root_diaobject." )
                FDBLogger.info( f"Committing..." )
                con.commit()
                FDBLogger.info( f"...done" )

                rows, _cols = con.execute( "SELECT COUNT(*) FROM root_diaobject" )
                FDBLogger.info( f"There are now {rows[0][0]} root diaobjects." )
                
                
        import pdb; pdb.set_trace()
        pass

    finally:
        if go_nuclear:
            with DBCon() as con:
                con.execute( "TRUNCATE TABLE root_diaobject CASCADE" )
                con.commit()
