import pytest
import time
import uuid
import healpy

import numpy as np
import scipy.stats
from psycopg import sql

from db import DBCon
from util import FDBLogger


@pytest.fixture
def initial_set_factory():
    def maker( norigroots, seed=42 ):
        # Healpix order: 12 * 4^order is the number of healpix there are.
        # We want to use 31 bits (so we don't have to think about signed vs. unsigned)
        #   log( 2^31 / 12 ) / log(4) = 13.7
        # So do order 13
        # Do nested so we can go to lower order by just bitshifting.
        # healpy uses NSIDE = 2^order, so NSIDE = 8192

        FDBLogger.debug( f"Generating {norigroots} ra/dec..." )
        rng = np.random.default_rng( seed )
        unitvecs = scipy.stats.uniform_direction( 3 ).rvs( norigroots, random_state=rng )
        decs = 90. - ( 180. / np.pi * np.arccos( unitvecs[:, 2] ) )
        ras = 180. + 180. / np.pi * np.atan2( unitvecs[:, 1], unitvecs[:, 0] )
        FDBLogger.debug( "Getting order-13 healpix..." )
        healpixen = healpy.ang2pix( 8192, ras, decs, nest=True, lonlat=True )
        FDBLogger.debug( f"...generating {norigroots} uuids..." )
        uuids = np.array( [ uuid.uuid4() for i in range(norigroots) ] )
        FDBLogger.debug( "...done generating original roots." )

        return uuids, ras, decs, healpixen

    return maker


@pytest.fixture
def initial_table_factory():
    tables_to_del = []

    def maker( uuids, ras, decs, healpixen, tablename, withhealpix=False ):
        table = sql.Identifier( tablename )
        tables_to_del.append( table )
        healpixcolumn = sql.SQL( "  healpix int,\n" ) if withhealpix else sql.SQL( "" )
        q3cindex = sql.Identifier( f"ix_{tablename}_q3c" )
        healpixindex = sql.Identifier( f"ix_{tablename}_healpix" )

        with DBCon() as con:
            con.execute_nofetch( sql.SQL( "CREATE TABLE {table}(\n"
                                          "  id uuid PRIMARY KEY,\n"
                                          "{healpixcolumn}"
                                          "  ra double precision,\n"
                                          "  dec double precision )"
                                         ).format( table=table, healpixcolumn=healpixcolumn ) )
            con.execute_nofetch( sql.SQL( "CREATE INDEX {q3cindex} "
                                          "  ON {table}(q3c_ang2ipix(ra, dec))"
                                         ).format( q3cindex=q3cindex, table=table ) )
            if withhealpix:
                con.execute_nofetch( sql.SQL( "CREATE INDEX {healpixindex} "
                                              "  ON {table}(healpix)"
                                             ).format( healpixindex=healpixindex, table=table ) )
            con.commit()

            FDBLogger.debug( f"Inserting {len(uuids)} original roots into {tablename}..." )
            if withhealpix:
                with con.cursor.copy( sql.SQL( "COPY {table}(id,healpix,ra,dec) FROM stdin"
                                              ).format( table=table )
                                     ) as copier:
                    for rid, healpix, ra, dec in zip( uuids, healpixen, ras, decs ):
                        copier.write_row( [ rid, healpix, ra, dec ] )
            else:
                with con.cursor.copy( sql.SQL( "COPY {table}(id,ra,dec) FROM stdin"
                                              ).format( table=table )
                                     ) as copier:
                    for rid, ra, dec in zip( uuids, ras, decs ):
                        copier.write_row( [ rid, ra, dec ] )

            con.commit()

        FDBLogger.debug( f"...done inserting {len(uuids)} original roots into {tablename}" )

    try:
        yield maker
    finally:
        with DBCon() as con:
            for tab in tables_to_del:
                con.execute( sql.SQL( "DROP TABLE IF EXISTS {table}" ).format( table=tab ) )
            con.commit()


def generate_new_objects( ras, decs, num_to_insert, overlapfrac, radarcsec=1., dbcon=None, seed=137 ):
    """Generate new coordinates, overlapface of which match existing ones within radarcsec.

    Parameters
    ----------
      ras : array of double
        ras that already exist

      decs : array of double
        decs that already exist

      num_to_insert : int
        Number of new objects to generate (including ones that should match)

      overlapfrac: float between 0 and 1
        num_to_insert * overlapfrac of the num_to_insert generated
        coordinates will be close to randomly selected coordiantes from
        (ras, decs).

      radarcsec : float, default 1.
        The overlapfrrac * num_to_insert objects that match existing
        objects will be randomly distributed in a circle (*) around the
        existing objects with this radius in arcseconds.  (*) Things get
        weird at the poles, whatever.

      dbcon : DBCon, default None
        If given, the new objects will be uploaded to temprary table newcoord(id,healipx,ra,dec).

     Returns
     -------
      newras, newdecs, healpixen
         Each is a num_to_insert length numpy array; the first two are double, the last is int.

     """

    nold = int( num_to_insert * overlapfrac )
    nnew = num_to_insert - nold
    FDBLogger.debug( f"Generating {nold} positions that pre-exist and {nnew} that (probably) don't" )

    # Generate nold offsets that are between 0 and 1"; do it uniformly because why not
    rng = np.random.default_rng( seed )
    roffs = np.sqrt( rng.uniform( size=nold ) ) * radarcsec / 3600.
    phis = rng.uniform( 0., 2.*np.pi, size=nold )
    # Match them to randomly selected objects from passed ras, decs
    dexen = np.arange( len(ras), dtype=int )
    rng.shuffle( dexen )
    dexen = dexen[ 0:nold ]

    # Apply offsets to the existing objects we chose
    olddecs = decs[dexen] + roffs * np.cos( phis )
    # ...yeah, coordinate singlarities, whatever
    olddecs[ olddecs >= 89.99999 ] = 89.99999
    olddecs[ olddecs <= -89.99999 ] = -89.99999
    oldras = ras[dexen] + roffs * np.sin( phis ) / np.cos( olddecs * np.pi / 180. )
    # Deal with the small number of things that slopped across RA 0°
    while any( oldras >= 360. ):
        oldras[ oldras >= 360. ] -= 360.
    while any( oldras < 0. ):
        oldras[ oldras < 0. ] += 360.

    # Make nnew new randomly generated positions on the sky
    unitvecs = scipy.stats.uniform_direction( 3 ).rvs( nnew, random_state=rng )
    newdecs = 90. - ( 180. / np.pi * np.arccos( unitvecs[:, 2] ) )
    newras = 180. + 180. / np.pi * np.atan2( unitvecs[:, 1], unitvecs[:, 0] )

    # Build the final list
    newras = np.concatenate( [ oldras, newras ] )
    newdecs = np.concatenate( [ olddecs, newdecs ] )
    # Shuffle them just so the pre-existing ones don't all come first
    dexen = np.arange( len(newras), dtype=int )
    rng.shuffle( dexen )
    newras = newras[ dexen ]
    newdecs = newdecs[ dexen ]
    # Calculate healpix
    healpixen = healpy.ang2pix( 8192, newras, newdecs, nest=True, lonlat=True )

    # Stick them in the temporary table newcoord if we were given a database connection
    if dbcon is not None:
        FDBLogger.debug( f"Uploading {len(newras)} new coordinates to temp table" )
        dbcon.execute_nofetch( "DROP TABLE IF EXISTS newcoord" )
        dbcon.execute_nofetch( "CREATE TEMP TABLE newcoord(id uuid, healpix int, "
                               "ra double precision, dec double precision)" )
        with dbcon.cursor.copy( "COPY newcoord(id, healpix, ra, dec) FROM stdin" ) as copier:
            for healpix, ra, dec in zip( healpixen, newras, newdecs ):
                copier.write_row( [ None, healpix, ra, dec ] )


        FDBLogger.debug( "Uploaded new coordiantes to temp table newcoord" )

    FDBLogger.debug( "Done generating new coordinates" )

    return newras, newdecs, healpixen


def merge( dbcon, desttable, withhealpix=False ):
    table = sql.Identifier( desttable )
    if withhealpix:
        healpixmatch = sql.SQL( "r.healpix=n.healpix\n  AND " )
        healpixcolumn = sql.SQL( "healpix int, " )
        healpixinsert = sql.SQL( "healpix," )
    else:
        healpixmatch = sql.SQL( "" )
        healpixcolumn = sql.SQL( "" )
        healpixinsert = sql.SQL( "" )

    FDBLogger.debug( f"Matching pre-existing root objects in {desttable} to new ones " )
    dbcon.execute_nofetch( sql.SQL( "UPDATE newcoord n SET id=r.id\n"
                                    "FROM {table} r\n"
                                    "WHERE {hp}q3c_radial_query(r.ra, r.dec, n.ra, n.dec, 0.00027777778)"
                                   ).format( table=table, hp=healpixmatch ) )
    FDBLogger.debug( "...done matching." )
    rows, _cols = dbcon.execute( "SELECT COUNT(*) FROM newcoord WHERE id IS NOT NULL" )
    FDBLogger.debug( f"{rows[0][0]} new root objects matched an existing root object" )
    FDBLogger.debug( "Creating temp table that will be added to root diaobjects..." )
    dbcon.execute_nofetch( "DROP TABLE IF EXISTS temp_new_root_obj" )
    dbcon.execute_nofetch( sql.SQL( "CREATE TEMP TABLE temp_new_root_obj"
                                  "(id uuid, {hp} ra double precision, dec double precision)"
                                 ).format( hp=healpixcolumn ) )
    dbcon.execute( sql.SQL( "INSERT INTO temp_new_root_obj(id, {hp} ra, dec) "
                          "( SELECT gen_random_uuid(), {hp} ra, dec FROM newcoord "
                          "  WHERE id IS NULL )"
                         ).format( hp=healpixinsert ) )
    FDBLogger.debug( "...done creating temp table." )
    FDBLogger.debug( f"Adding new root objects to {desttable}..." )
    dbcon.execute( sql.SQL( "INSERT INTO {table}(id,ra,dec) ("
                          "  SELECT gen_random_uuid(),ra,dec FROM temp_new_root_obj )"
                         ).format( table=table ) )
    FDBLogger.debug( f"...done adding new root objects to {desttable}." )
    FDBLogger.debug( "Committing..." )
    dbcon.commit()
    FDBLogger.debug( "...done" )

    rows, _cols = dbcon.execute( sql.SQL( "SELECT COUNT(*) FROM {table}" ).format(table=table) )
    FDBLogger.debug( f"There are now {rows[0][0]} root diaobjects in {desttable}." )

    return rows[0][0]



def test_diagnose_rootstress( initial_set_factory, initial_table_factory ):
    # This one is a bit weird.  It makes its own tables, so we can compare versions.
    # norigroots = 10000000
    # ninserts = [ 10000 ] # [ 10000, 100000, 10000 ]
    norigroots = 100000
    ninserts = [ 1000 ]
    overlapfrac = 0.1

    origuuids, origras, origdecs, orighealpixen = initial_set_factory( norigroots )
    initial_table_factory( origuuids, origras, origdecs, orighealpixen, "test_diagnose_rootstress_withhealpix",
                           withhealpix=True )
    initial_table_factory( origuuids, origras, origdecs, orighealpixen, "test_diagnose_rootstress_nohealpix",
                           withhealpix=False )


    for num_to_insert in ninserts:
        with DBCon( echoqueries=True ) as dbcon:
            newras, newdecs, healpixen = generate_new_objects( origras, origdecs, num_to_insert, overlapfrac,
                                                               dbcon=dbcon )

            # NOTE : try this in both orders so that we make sure that the
            #   second one isn't benefitting from some caching on the PostgreSQL server.

            tnohp0 = time.perf_counter()
            nno = merge( dbcon, "test_diagnose_rootstress_nohealpix", withhealpix=False )
            tnohp1 = time.perf_counter()

            # Clean up the temp table from the results of the merge
            dbcon.execute_nofetch( "UPDATE newcoord SET id=NULL" )
            
            thp0 = time.perf_counter()
            nwith = merge( dbcon, "test_diagnose_rootstress_withhealpix", withhealpix=True )
            thp1 = time.perf_counter()

            FDBLogger.info( f"Added {num_to_insert} new objects with {overlapfrac} overlap." )
            FDBLogger.info( f"Without healpix, ended up with {nno} root diaobjects, "
                            f"took {(tnohp1-tnohp0):.1f} sec. ")
            FDBLogger.info( f"With healpix, ended up with {nwith} root diaobjects, "
                            f"took {(thp1-thp0):.1f} sec. ")
