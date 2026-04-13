import pytest
import datetime
import time
import numbers
import random
import textwrap
import psycopg.errors
import itertools
from psycopg import sql

import db
from util import env_as_bool, FDBLogger, datetime_to_utc
from services.source_importer import SourceImporter
from services.brokerconsumer import FinkConsumer

# Ordering of these tests matters, because they use module scope
# fixtures from tests/fixtures/alertcycle.py (the "alerts*" fixtures).
# Make sure that anything that tests assuming only 30 days is done
# runs before any test that includes a 60 or 90 day fixture.
# (The ordering is very fiddly.)

# NOTE : there is currently no test that checks when object_processing_version
#   and processing_version are different


# ======================================================================
# Specific broker tests are at the beginning because if we run them, we want to run them
#   before the module-level fixtures that load up the database are run.

@pytest.mark.skipif( not env_as_bool('RUN_FINK_TESTS'), reason='RUN_FINK_TESTS is not set' )
def test_fink( procver_collection ):
    bpv, _pv, _pvinfo = procver_collection
    barf = "".join( random.choices( 'abcdefghijklmnopqrstuvwxyz', k=6 ) )
    brokertopic = 'fink_sn_near_galaxy_candidate_lsst'

    try:
        t0 = time.perf_counter()
        fc = FinkConsumer( 'kafka-lsst.fink-broker.org:24499', f'rknop-fastdb-test-{barf}',
                           topics=[ brokertopic ], mongodb_collection_base='test_fink',
                           brokername_for_alerts='Fink', nomsg_sleeptime=1, batch_size=10, cache_alerts=True )
        fc.poll( restart_time=datetime.timedelta(seconds=10), max_restarts=0, notopic_sleeptime=2, max_msgs=10 )
        t1 = time.perf_counter()
        FDBLogger.info( f"Fink poll finished in {t1-t0} seconds." )

        si = SourceImporter( object_base_processing_version=bpv['realtime_diaobject'].id,
                             object_position_base_processing_version=bpv['realtime_diaobject_position_60000'].id,
                             source_base_processing_version=bpv['realtime_diasource'].id,
                             forcedsource_base_processing_version=bpv['realtime_diaforcedsource'].id,
                             collection_base_name='test_fink' )
        si.import_from_mongo()

        with db.MGCon( readonly=True ) as mg:
            alerts = list( mg.collection( "test_fink_alertcache" ).find( {} ) )
            mgsources = list( mg.collection( "test_fink_diasource" ).find( {} ) )
            # Make sure we got some to chew on!
            assert len( alerts ) >= 10
            mgobjects = list( mg.collection( "test_fink_diaobject" ).find( {} ) )
            mginfos = list( mg.collection( "test_fink_brokerinfo" ).find( {} ) )
            assert len(alerts) == len(mginfos)
            assert set( i['diasourceid'] for i in mginfos ).issubset( set( s['diasourceid'] for s in mgsources ) )
            assert ( set( a['msg']['diaSource']['diaSourceId'] for a in alerts )
                     == set( i['diasourceid'] for i in mginfos ) )
            assert ( set( a['msg']['diaObject']['diaObjectId'] for a in alerts )
                     == set( o['diaobjectid'] for o in mgobjects ) )


        with db.DBCon( dictcursor=True ) as pqcon:
            objects = pqcon.execute( "SELECT diaobjectid FROM diaobject" )
            sources = pqcon.execute( "SELECT diasourceid FROM diasource" )
            brokerinfo = pqcon.execute( "SELECT brokername, topic, diasourceid FROM diasource_brokerinfo" )

        assert all( i['brokername'] == 'Fink' for i in brokerinfo )
        assert all( i['topic'] == brokertopic for i in brokerinfo )
        assert set( i['diasourceid'] for i in brokerinfo ) == set( a['msg']['diaSourceId'] for a in alerts )
        assert set( s['diasourceid'] for s in sources ) == set( s['diasourceid'] for s in mgsources )
        assert set( o['diaobjectid'] for o in objects ) == set( a['msg']['diaObject']['diaObjectId'] for a in alerts )
        assert set( o['diaobjectid'] for o in objects ) == set( m['diaobjectid'] for m in mgobjects )


    finally:
        with db.DBCon() as pqcon:
            pqcon.execute( "DELETE FROM diaforcedsource_extra" )
            pqcon.execute( "DELETE FROM diaforcedsource" )
            pqcon.execute( "DELETE FROM diasource_brokerinfo" )
            pqcon.execute( "DELETE FROM diasource_extra" )
            pqcon.execute( "DELETE FROM diasource" )
            pqcon.execute( "DELETE FROM diaobject_position" )
            pqcon.execute( "DELETE FROM diaobject" )
            pqcon.execute( "DELETE FROM root_diaobject" )
            pqcon.execute( "DELETE FROM diasource_import_time" )
            pqcon.commit()

        with db.MGCon() as mg:
            colnames = [ f'test_fink_{s}' for s in
                         [ 'diaobject', 'diasource', 'diasource_extra',
                           'diaforcedsource', 'diaforcedsource_extra',
                           'thumbnails', 'brokerinfo', 'alertcache' ] ]
            knowncollections = list( mg.db.list_collection_names() )
            for c in colnames:
                if c in knowncollections:
                    mg.collection( c ).drop()
            knowncollections = list( mg.db.list_collection_names() )
            assert not any( c in knowncollections for c in colnames )

            mg.collection( 'source_thumbnails' ).delete_many( {} )


# **********************************************************************

@pytest.fixture( scope='module' )
def sourceimporter_args( procver_collection ):
    bpv, _pv, _pvinfo = procver_collection
    return { 'object_base_processing_version': bpv['realtime_diaobject'].id,
             'object_position_base_processing_version': bpv['realtime_diaobject_position_60000'].id,
             'source_base_processing_version': bpv['realtime_diasource'].id,
             'forcedsource_base_processing_version': bpv['realtime_diaforcedsource'].id,
             'collection_base_name': 'fastdb_alertcycle_test' }


@pytest.fixture
def import_first30days_objects( alerts_30days_sent_and_brokermessage_consumed, sourceimporter_args ):
    t1 = alerts_30days_sent_and_brokermessage_consumed

    try:
        si = SourceImporter( **sourceimporter_args )
        nobjs, nroot, npos = si.import_objects( t1=t1 )

        yield nobjs, nroot, npos
    finally:
        with db.DBCon() as conn:
            conn.execute( "DELETE FROM diaobject_position" )
            conn.execute( "DELETE FROM diaobject" )
            conn.execute( "DELETE FROM root_diaobject" )
            conn.commit()


@pytest.fixture
def import_first30days_sources( import_first30days_objects, sourceimporter_args,
                                alerts_30days_sent_and_brokermessage_consumed ):
    t1 = alerts_30days_sent_and_brokermessage_consumed

    try:
        si = SourceImporter( **sourceimporter_args )
        nsrc = si.import_sources( t1=t1 )
        ninfo = si.import_brokerinfo( t1=t1 )
        with db.MGCon() as mg:
            si.import_cutouts( mg, t1=t1 )

        yield nsrc, ninfo
    finally:
        with db.DBCon() as conn:
            conn.execute( "DELETE FROM diasource_brokerinfo" )
            conn.execute( "DELETE FROM diasource_extra" )
            conn.execute( "DELETE FROM diasource" )
            conn.commit()
        with db.MG() as mongoclient:
            collection = db.get_mongo_collection( mongoclient, "source_thumbnails" )
            collection.delete_many( {} )


@pytest.fixture
def import_30days_prvforcedsources( import_first30days_sources, sourceimporter_args,
                                    alerts_30days_sent_and_brokermessage_consumed ):
    t1 = alerts_30days_sent_and_brokermessage_consumed

    try:
        si = SourceImporter( **sourceimporter_args )
        n = si.import_forcedsources( t1=t1 )

        yield n
    finally:
        with db.DBCon() as conn:
            conn.execute( "DELETE FROM diaforcedsource_extra" )
            conn.execute( "DELETE FROM diaforcedsource" )
            conn.commit()


# This next one is messy because it should *only* be used in:
#   * test_import_30days
#   * import_30days_60days
#   * test_import_30days_60days
#
# Reason: the import_30days_60days fixture will do a cleanup that is going to destroy
#   everything that's set up in this fixture, even though this is a module fixture!
#   It's here because we want to be able to test the time stamps in import_30days_60days,
#   after running test_30days, so this fixture has to be a module fixture so its results
#   will persist for those two tests.  It's kidna like making a mini-scope.
@pytest.fixture( scope='module' )
def messy_import_30days( sourceimporter_args, alerts_30days_sent_and_brokermessage_consumed ):
    try:
        si = SourceImporter( **sourceimporter_args )
        nobj, nroot, npos, nsrc, nfrc, ninfo = si.import_from_mongo()

        yield nobj, nroot, npos, nsrc, nfrc, ninfo

    finally:
        # Put this cleanup here just in case things died before we got to the
        #   import_30days_60days fixture that does the same cleanup.
        with db.DBCon() as conn:
            conn.execute( "DELETE FROM diaforcedsource_extra" )
            conn.execute( "DELETE FROM diaforcedsource" )
            conn.execute( "DELETE FROM diasource_brokerinfo" )
            conn.execute( "DELETE FROM diasource_extra" )
            conn.execute( "DELETE FROM diasource" )
            conn.execute( "DELETE FROM diaobject_position" )
            conn.execute( "DELETE FROM diaobject" )
            conn.execute( "DELETE FROM root_diaobject" )
            conn.execute( "DELETE FROM diasource_import_time" )
            conn.commit()
        with db.MG() as mg:
            col = db.get_mongo_collection( mg, 'source_thumbnails' )
            col.delete_many({})


# Import days 30-90 after importing days 0-30, and update the diasource_import_time table
# Fixture yields the numbers from the import of days 30-90 (also include fixture
#   import_30days if you want those counts too).
@pytest.fixture
def import_30days_60days( sourceimporter_args, messy_import_30days,
                          alerts_30days_sent_and_brokermessage_consumed,
                          alerts_60moredays_sent_and_brokermessage_consumed ):
    t0 = alerts_30days_sent_and_brokermessage_consumed
    t1 = alerts_60moredays_sent_and_brokermessage_consumed
    assert t1 > t0

    try:
        with db.DBCon() as pqconn:
            timp30 = pqconn.execute( "SELECT t FROM diasource_import_time WHERE collection='fastdb_alertcycle_test'" )
            timp30 = timp30[0][0][0]
        assert timp30 > t0
        # This next line, I think, ensures that the tests are using the fixtures in the
        #   fiddly right order
        assert timp30 < t1

        si = SourceImporter( **sourceimporter_args )
        nobj, nroot, npos, nsrc, nprvfrc, ninfo = si.import_from_mongo()

        with db.DBCon() as pqconn:
            timp90 = pqconn.execute( "SELECT t FROM diasource_import_time WHERE collection='fastdb_alertcycle_test'" )
            timp90 = timp90[0][0][0]
            assert timp90 > timp30
            assert timp90 > t1

        yield nobj, nroot, npos, nsrc, nprvfrc, ninfo

    finally:
        with db.DBCon() as conn:
            conn.execute( "DELETE FROM diaforcedsource_extra" )
            conn.execute( "DELETE FROM diaforcedsource" )
            conn.execute( "DELETE FROM diasource_brokerinfo" )
            conn.execute( "DELETE FROM diasource_extra" )
            conn.execute( "DELETE FROM diasource" )
            conn.execute( "DELETE FROM diaobject_position" )
            conn.execute( "DELETE FROM diaobject" )
            conn.execute( "DELETE FROM root_diaobject" )
            conn.execute( "DELETE FROM diasource_import_time" )
            conn.commit()
        with db.MG() as mg:
            col = db.get_mongo_collection( mg, 'source_thumbnails' )
            col.delete_many({})


# Import days 30-90 without importing days 0-30.
# This uses the timestamps returned by some of the other fixtures
@pytest.fixture
def import_only_next60days( sourceimporter_args,
                            alerts_30days_sent_and_brokermessage_consumed,
                            alerts_60moredays_sent_and_brokermessage_consumed
                           ):
    t0 = alerts_30days_sent_and_brokermessage_consumed
    t1 = alerts_60moredays_sent_and_brokermessage_consumed

    try:
        si = SourceImporter( **sourceimporter_args )
        nobj, nroot, npos = si.import_objects( t0=t0, t1=t1 )
        nsrc = si.import_sources( t0=t0, t1=t1 )
        nfrc = si.import_forcedsources( t0=t0, t1=t1 )
        ninfo = si.import_brokerinfo( t0=t0, t1=t1 )
        with db.MGCon() as mg:
            si.import_cutouts( mg, t0=t0, t1=t1 )

        yield nobj, nroot, npos, nsrc, nfrc, ninfo

    finally:
        with db.DBCon() as conn:
            conn.execute( "DELETE FROM diaforcedsource_extra" )
            conn.execute( "DELETE FROM diaforcedsource" )
            conn.execute( "DELETE FROM diasource_brokerinfo" )
            conn.execute( "DELETE FROM diasource_extra" )
            conn.execute( "DELETE FROM diasource" )
            conn.execute( "DELETE FROM diaobject_position" )
            conn.execute( "DELETE FROM diaobject" )
            conn.execute( "DELETE FROM root_diaobject" )
            conn.commit()
        with db.MG() as mongoclient:
            collection = db.get_mongo_collection( mongoclient, "source_thumbnails" )
            collection.delete_many( {} )


# To test the t1 limit, try to import only the first 30 days of alerts (based on the
#   timestamp we get back from the fixture that brokermessageconsumes them)
#   even when all 90 days of alerts have been brokermessageconsumed.
@pytest.fixture
def import_only_30days_after_90days_consumed( sourceimporter_args,
                                              alerts_30days_sent_and_brokermessage_consumed,
                                              alerts_60moredays_sent_and_brokermessage_consumed ):
    t1 = alerts_30days_sent_and_brokermessage_consumed

    try:
        si = SourceImporter( **sourceimporter_args )
        nobj, nroot, npos, nsrc, nfrc, ninfo = si.import_from_mongo( t1=t1 )

        yield nobj, nroot, npos, nsrc, nfrc, ninfo

    finally:
        with db.DBCon() as conn:
            conn.execute( "DELETE FROM diaforcedsource_extra" )
            conn.execute( "DELETE FROM diaforcedsource" )
            conn.execute( "DELETE FROM diasource_brokerinfo" )
            conn.execute( "DELETE FROM diasource_extra" )
            conn.execute( "DELETE FROM diasource" )
            conn.execute( "DELETE FROM diaobject_position" )
            conn.execute( "DELETE FROM diaobject" )
            conn.execute( "DELETE FROM root_diaobject" )
            conn.execute( "DELETE FROM diasource_import_time" )
            conn.commit()
        with db.MG() as mg:
            col = db.get_mongo_collection( mg, 'source_thumbnails' )
            col.delete_many({})


# **********************************************************************

def check_database_contents( lastdayoffset, firstdayoffset=None, dbcon=None ):
    with db.DBCon( dbcon ) as conn:
        try:
            # Figure out the last ay of alerts that should have been sent, classified, consumed, and imported
            rows, _cols = conn.execute( "SELECT MIN(midpointmjdtai) FROM diasource" )
            throughday = rows[0][0] + lastdayoffset
            startday = rows[0][0] + firstdayoffset if firstdayoffset is not None else None

            # Select out all the sources, objects, and forced soruces that should be included
            q = sql.SQL( textwrap.dedent(
                """
                SELECT diaobjectid, diasourceid, midpointmjdtai INTO TEMP TABLE tmp_expected_sources
                FROM ppdb_diasource
                WHERE midpointmjdtai<={throughday}
                """
            ) ).format( throughday=throughday )
            if startday is not None:
                q += sql.SQL( "  AND midpointmjdtai>={startday}" ).format( startday=startday )
            conn.execute( q )

            q = sql.SQL( textwrap.dedent(
                """
                SELECT DISTINCT ON(diaobjectid) diaobjectid
                INTO TEMP TABLE tmp_expected_objects
                FROM tmp_expected_sources
                ORDER BY diaobjectid
                """ ) )
            conn.execute( q )

            q = sql.SQL( textwrap.dedent(
                """
                SELECT DISTINCT ON (f.diaforcedsourceid) f.diaobjectid, f.diaforcedsourceid
                INTO TEMP TABLE tmp_expected_forcedsources
                FROM ppdb_diaforcedsource f
                INNER JOIN tmp_expected_sources t ON t.diaobjectid=f.diaobjectid
                WHERE f.midpointmjdtai<=t.midpointmjdtai-1
                ORDER BY f.diaforcedsourceid
                """
            ) )
            conn.execute( q )

            rows, _cols = conn.execute( "SELECT diaobjectid FROM tmp_expected_objects" )
            expected_objects = set( r[0] for r in rows )

            rows, _cols = conn.execute( "SELECT diasourceid FROM tmp_expected_sources" )
            expected_sources = set( r[0] for r in rows )
            expected_brokerinfos = set(
                itertools.chain( *[ [ ( r[0], b ) for b in [ 'FakeBroker-Nugent', 'FakeBroker-Random' ]
                                     ] for r in rows  ] )
            )

            rows, _cols = conn.execute( "SELECT diaforcedsourceid FROM tmp_expected_forcedsources" )
            expected_forcedsources = set( r[0] for r in rows )

            # Make soure we found the right objects

            rows, _cols = conn.execute( "SELECT DISTINCT ON (diaobjectid) diaobjectid "
                                        "FROM diasource ORDER BY diaobjectid" )
            found_objects = set( r[0] for r in rows )
            assert found_objects == expected_objects

            rows, _cols = conn.execute( "SELECT diaobjectid FROM diaobject" )
            found_objects = set( r[0] for r in rows )
            assert found_objects == expected_objects

            rows, _cols = conn.execute( "SELECT diasourceid FROM diasource" )
            found_sources = set( r[0] for r in rows )
            if firstdayoffset is None:
                assert found_sources == expected_sources
            else:
                # There will be extra sources because of the previous array
                assert expected_sources.issubset( found_sources )

            rows, _cols = conn.execute( "SELECT diasourceid FROM diasource_extra" )
            found_sources_extra = set( r[0] for r in rows )
            assert found_sources_extra == found_sources

            rows, _cols = conn.execute( "SELECT diaforcedsourceid FROM diaforcedsource" )
            found_forcedsources = set( r[0] for r in rows )
            assert found_forcedsources == expected_forcedsources

            rows, _cols = conn.execute( "SELECT diaforcedsourceid FROM diaforcedsource_extra" )
            found_forcedsources_extra = set( r[0] for r in rows )
            assert found_forcedsources_extra == expected_forcedsources

            rows, _cols = conn.execute( "SELECT diasourceid, brokername FROM diasource_brokerinfo" )
            found_brokerinfos = set( (r[0], r[1]) for r in rows )
            assert found_brokerinfos == expected_brokerinfos

            # ... it will be a miracle if this works for all the float and double fileds, because things
            #   have been sent via kafka and imported to mongo and gone through json and all the rest.
            #   I guess it's possible all of that could happen without floating point roundoff
            #   changing things slightly....   I think mongo uses bjson, and avro is binary.
            #   The real key is going to be what happened in sourceimporter when things were
            #   copied up to postscript.  I *think* we used binary there too.  We'll see.
            #   ...looks like it works.  Guess it was all binary and no floating roundoff happened.
            srccols = sql.SQL(',').join( sql.SQL("( ( {s} IS NULL AND {p} IS NULL ) or ( {s}={p} ) ) AS {c}")
                                         .format( s=sql.Identifier("s", c), p=sql.Identifier("p", c),
                                                  c=sql.Identifier(c) )
                                         for c in [ 'visit', 'band', 'midpointmjdtai',
                                                    'psfflux', 'psffluxerr',
                                                    'ra', 'dec', 'raerr', 'decerr', 'ra_dec_cov' ] )
            srcexcols = sql.SQL(',').join( sql.SQL("( ( {s} IS NULL AND {p} IS NULL ) or ( {s}={p} ) ) AS {c}")
                                           .format( s=sql.Identifier("s", c), p=sql.Identifier("p", c),
                                                    c=sql.Identifier(c) )
                                           for c in [ 'detector', 'x', 'y', 'xerr', 'yerr',
                                                      'x_y_cov', 'psflnl', 'psfchi2', 'psfndata', 'snr',
                                                      # 'scienceflux', 'sciencefluxerr',
                                                      'templateflux', 'templatefluxerr',
                                                      'reliability', 'ixx', 'iyy', 'ixxpsf', 'iyypsf',
                                                      'ixypsf', 'flags', 'pixelflags',
                                                      'apflux', 'apfluxerr', 'bboxsize',
                                                      'parentdiasourceid' ] )
            frccols = sql.SQL(',').join( sql.SQL("( ( {s} IS NULL AND {p} IS NULL ) or ( {s}={p} ) ) AS {c}")
                                         .format( s=sql.Identifier("s", c), p=sql.Identifier("p", c),
                                                  c=sql.Identifier(c) )
                                         for c in [ 'visit', 'band', 'midpointmjdtai',
                                                    'psfflux', 'psffluxerr', 'ra', 'dec' ] )
            frcexcols = sql.SQL(',').join( sql.SQL("( ( {s} IS NULL AND {p} IS NULL ) or ( {s}={p} ) ) AS {c}")
                                           .format( s=sql.Identifier("s", c), p=sql.Identifier("p", c),
                                                    c=sql.Identifier(c) )
                                           for c in [ 'detector',
                                                      'scienceflux', 'sciencefluxerr' ] )

            rows, _cols = conn.execute( sql.SQL( "SELECT s.diasourceid, {cols} FROM diasource s "
                                                 "INNER JOIN ppdb_diasource p ON s.diasourceid=p.diasourceid"
                                                ).format( cols=srccols ) )
            assert all( all( r ) for r in rows )

            rows, _cols = conn.execute( sql.SQL( "SELECT s.diasourceid, {cols} FROM diasource_extra s "
                                                 "INNER JOIN ppdb_diasource p ON s.diasourceid=p.diasourceid"
                                                ).format( cols=srcexcols ) )
            assert all( all( r ) for r in rows )

            rows, _cols = conn.execute( sql.SQL( "SELECT s.diaforcedsourceid, {cols} FROM diaforcedsource s "
                                                 "INNER JOIN ppdb_diaforcedsource p "
                                                 "  ON s.diaforcedsourceid=p.diaforcedsourceid"
                                                ).format( cols=frccols ) )
            # NOTE : we have a special case here, because the fakebroker set psfflux to None for
            #   the first prvDiaForcedSource of diaSource 198154000011.  Depending on the order in
            #   which alerts arrived, that means that psfflux *may* have been loaded in as null
            #   instead of the ppdb value.
            assert all( all( r ) for r in rows if r[0] != 198154000000 )

            rows, _cols = conn.execute( sql.SQL( "SELECT s.diaforcedsourceid, {cols} FROM diaforcedsource_extra s "
                                                 "INNER JOIN ppdb_diaforcedsource p "
                                                 "  ON s.diaforcedsourceid=p.diaforcedsourceid"
                                                ).format( cols=frcexcols ) )
            assert all( all( r ) for r in rows )


            cols = sql.SQL(",").join( sql.SQL("( ( {o} IS NULL and {p} IS NULL ) OR ( {o}={p} ) ) AS {c}")
                                      .format( o=sql.Identifier("o", c), p=sql.Identifier("p", c),
                                               c=sql.Identifier(c) )
                                      for c in [ 'ra', 'dec', 'raerr', 'decerr', 'ra_dec_cov' ] )
            rows, _cols = conn.execute( sql.SQL( "SELECT o.diaobjectid, {cols} FROM diaobject_position o "
                                                 "INNER JOIN ppdb_diaobject p ON p.diaobjectid=o.diaobjectid"
                                                ).format( cols=cols ) )
            assert all( all( r ) for r in rows )


        finally:
            conn.execute( "DROP TABLE IF EXISTS tmp_expected_objects" )
            conn.execute( "DROP TABLE IF EXISTS tmp_expected_sources" )
            conn.execute( "DROP TABLE IF EXISTS tmp_expected_diaforcedsources" )
            # I don't think this commit is necessary.  If sombody who
            #   called us keeps using the database connection, then the
            #   tables will be dropped for them because... well, because
            #   we just dropped them in this connection.  Otherwise,
            #   when the database connection closes, the temp tables
            #   will go away automatically, even if nobody commits.
            #   conn.commit()



# **********************************************************************
# Tests on importation of the first 30 days

def test_read_mongo_objects( alerts_30days_sent_and_brokermessage_consumed, sourceimporter_args ):
    si = SourceImporter( **sourceimporter_args )
    # First: make sure it finds everyting with no time cut
    with db.DBCon() as conn:
        si.read_mongo_objects( conn )
        rows, _cols = conn.execute( "SELECT * FROM temp_diaobject_import" )
        assert len(rows) == 12

    # Second: make sure it finds everything with a top time cut of now
    #   (which is assuredly after when things were inserted)
    with db.DBCon() as conn:
        # (sanity test)
        with pytest.raises( psycopg.errors.UndefinedTable ):
            rows, _cols = conn.execute( "SELECT * FROM temp_diaobject_import" )
        conn.con.rollback()
        si.read_mongo_objects( conn, t1=datetime.datetime.now( tz=datetime.UTC ) )
        rows, _cols = conn.execute( "SELECT * FROM temp_diaobject_import" )
        assert len(rows) == 12

    # Third: make sure it finds nothing with a bottom time cut of now
    with db.DBCon() as conn:
        si.read_mongo_objects( conn, t0=datetime.datetime.now( tz=datetime.UTC ) )
        rows, _cols = conn.execute( "SELECT * FROM temp_diaobject_import" )
        assert len(rows) == 0

    # Testing between times is hard, because I belive all of the things
    # saved will have the same time cut!  So, resort to just giving
    # a ridiculously early t0 and make sure we get everything
    # Third: make sure it finds nothing with a bottom time cut of now
    with db.DBCon() as conn:
        si.read_mongo_objects( conn,
                               t0=datetime.datetime( 2000, 1, 1, 0, 0, 0, tzinfo=datetime.UTC ),
                               t1=datetime.datetime.now( tz=datetime.UTC ) )
        rows, _cols = conn.execute( "SELECT * FROM temp_diaobject_import" )
        assert len(rows) == 12

    # TODO : look at other fields?


def test_read_mongo_sources( alerts_30days_sent_and_brokermessage_consumed, sourceimporter_args ):
    # Not going to test time cuts here because it's the same code path that
    #   was already tested intest_read_mongo_objects

    si = SourceImporter( **sourceimporter_args )
    with db.DBCon( dictcursor=True ) as conn:
        si.read_mongo_sources( conn )
        rows = conn.execute( "SELECT diasourceid FROM temp_diasource_import" )
        assert len(rows) == 77
        srcids = set( [ r['diasourceid'] for r in rows ] )
        # All should be unique because of the $group in the mongo pipeline
        assert len(srcids) == 77
        rows = conn.execute( "SELECT diasourceid FROM temp_diasource_extra_import" )
        assert len(rows) == 77
        assert set( r['diasourceid'] for r in rows ) == srcids

    # Make sure it matches what was in mongo
    with db.MGCon() as mg:
        col = mg.collection( 'fastdb_alertcycle_test_diasource' )
        docs = col.find( {}, projection={ 'diasourceid': 1 } )
        mgsourceids = set( [ d['diasourceid'] for d in docs ] )
        assert mgsourceids == srcids
        col = mg.collection( 'fastdb_alertcycle_test_diasource_extra' )
        docs = col.find( {}, projection= { 'diasourceid': 1 } )
        mgextraids = set( [ d['diasourceid'] for d in docs ] )
        assert mgextraids == srcids

    # TODO : more stringent tests?


def test_read_mongo_previous_forced_sources( alerts_30days_sent_and_brokermessage_consumed, sourceimporter_args ):
    si = SourceImporter( **sourceimporter_args )
    with db.DBCon( dictcursor=True ) as conn:
        si.read_mongo_prvforcedsources( conn )
        rows = conn.execute( "SELECT diaforcedsourceid FROM temp_prvdiaforcedsource_import" )
        assert len(rows) == 148
        frcids = set( r['diaforcedsourceid'] for r in rows )
        rows = conn.execute( "SELECT diaforcedsourceid FROM temp_prvdiaforcedsource_extra_import" )
        assert len(rows) == 148
        assert set( r['diaforcedsourceid'] for r in rows ) == frcids

    # Make sure it matches what was in mongo
    with db.MGCon() as mg:
        col = mg.collection( 'fastdb_alertcycle_test_diaforcedsource' )
        docs = col.find( {}, projection={ 'diaforcedsourceid': 1 } )
        mgfrcids = set( d['diaforcedsourceid'] for d in docs )
        assert mgfrcids == frcids
        col = mg.collection( 'fastdb_alertcycle_test_diaforcedsource_extra' )
        docs = col.find( {}, projection={ 'diaforcedsourceid': 1 } )
        mgextraids = set( d['diaforcedsourceid'] for d in docs )
        assert mgextraids == frcids


def test_read_mongo_brokerinfo( alerts_30days_sent_and_brokermessage_consumed, sourceimporter_args ):
    si = SourceImporter( **sourceimporter_args )
    with db.DBCon( dictcursor=True ) as conn:
        si.read_mongo_brokerinfo( conn )
        pginfos = conn.execute( "SELECT brokername, topic, diasourceid, prv_diasourceid, prv_diaforcedsourceid, info "
                                "FROM temp_diasource_brokerinfo_import" )
        assert len(pginfos) == 154
        pginfoids = set( ( r['brokername'], r['topic'], r['diasourceid'] ) for r in pginfos )
        assert len(pginfoids) == 154

    # Make sure it matches what was in mongo
    with db.MGCon() as mg:
        col = mg.collection( 'fastdb_alertcycle_test_brokerinfo' )
        docs = list( col.find( {} ) )
        mginfoids = set( ( d['brokername'], d['topic'], d['diasourceid'] ) for d in docs )
        assert pginfoids == mginfoids
        for doc in docs:
            pginfo = [ p for p in pginfos if ( ( p['brokername'] == doc['brokername'] ) and
                                               ( p['topic'] == doc['topic'] ) and
                                               ( p['diasourceid'] == doc['diasourceid'] ) ) ]
            assert len(pginfo) == 1
            pginfo = pginfo[0]
            assert pginfo['prv_diasourceid'] == doc['prv_diasourceid']
            assert pginfo['prv_diaforcedsourceid'] == doc['prv_diaforcedsourceid']
            assert pginfo['info'] == doc['info']


def test_import_objects( import_first30days_objects ):
    nobj, nroot, npos = import_first30days_objects
    assert nobj == 12
    assert nroot == 12
    assert npos == 12
    with db.DB() as conn:
        cursor = conn.cursor()
        cursor.execute( "SELECT * FROM diaobject" )
        objrows = cursor.fetchall()
        objcols = { cursor.description[i].name: i for i in range( len(cursor.description) ) }
        assert len(objrows) == 12

        cursor.execute( "SELECT * FROM diaobject_position" )
        posrows = cursor.fetchall()
        poscols = { cursor.description[i].name: i for i in range( len(cursor.description) ) }
        assert set( p[poscols['diaobjectid']] for p in posrows ) == set( o[objcols['diaobjectid']] for o in objrows )
        assert all( p[poscols['ra']] is not None for p in posrows )
        assert all( p[poscols['dec']] is not None for p in posrows )

        cursor.execute( "SELECT id FROM root_diaobject" )
        rootids = [ r[0] for r in cursor.fetchall() ]
        assert len(rootids) == 12

        # Make sure that all the object rootids are distinct
        assert set( r[objcols['rootid']] for r in objrows ) == set( rootids )

    # TODO : look at more?  Compare ppdb_diaobject to diaobject?


def test_import_sources( import_first30days_sources ):
    nsrc, ninfo = import_first30days_sources
    assert nsrc == 77
    assert ninfo == 154
    with db.DBCon( dictcursor=True ) as conn:
        sources = conn.execute( "SELECT * FROM diasource" )
        extras = conn.execute( "SELECT * FROM diasource_extra" )
        brokerinfos = conn.execute( "SELECT * FROM diasource_brokerinfo" )

    # Some hardcoded numbers because we know what's in the test set of SNANA-imported PPDB tables
    assert len( sources ) == 77
    assert len( extras ) == len( sources )
    assert set( [ e['diasourceid'] for e in extras ] ) == set( [ s['diasourceid'] for s in sources ] )
    assert min( s['midpointmjdtai'] for s in sources ) == pytest.approx( 60278.029, abs=0.01 )
    assert max( s['midpointmjdtai'] for s in sources ) ==  pytest.approx( 60303.211, abs=0.01 )

    # Compare what's in mongo to what's in postgres

    with db.MGCon() as mg:
        assert mg.collection( "source_thumbnails" ).count_documents({}) == nsrc
        mgsources = list( mg.collection( "fastdb_alertcycle_test_diasource" ).find({}) )
        mgsources_extra = list( mg.collection( "fastdb_alertcycle_test_diasource_extra" ).find({}) )
        mgbrokerinfo = list( mg.collection( "fastdb_alertcycle_test_brokerinfo" ).find({}) )

        for source in sources:
            msource = [ s for s in mgsources if s['diasourceid'] == source['diasourceid'] ]
            # There will be multiple matches because of previous sources.  We'll just compare against the first.
            assert len(msource) > 0
            msource = msource[0]
            assert all( source[k] == ( pytest.approx( msource[k], rel=1e-6 )
                                       if isinstance( msource[k], numbers.Real )
                                       else msource[k] )
                        for k in msource.keys() if k not in ( '_id', 'savetime', 'msg_diasourceid' ) )

        for source_extra in extras:
            mextra = [ e for e in mgsources_extra if e['diasourceid'] == source_extra['diasourceid'] ]
            # Again, multiple matches because of previous
            assert len(mextra) > 0
            mextra = mextra[0]
            assert all( source_extra[k] == ( pytest.approx( mextra[k], rel=1e-6 )
                                             if isinstance( mextra[k], numbers.Real )
                                             else mextra[k] )
                        for k in mextra.keys() if k  not in ( '_id', 'savetime' ) )

        for info in brokerinfos:
            minfo = [ i for i in mgbrokerinfo if ( ( i['brokername'], i['topic'], i['diasourceid'] ) ==
                                                   ( info['brokername'], info['topic'], info['diasourceid'] ) ) ]
            assert len(minfo) == 1
            minfo = minfo[0]
            assert ( set( k for k in info.keys() if k not in ( 'importtime', 'receivedtime', 'base_procver_id' ) )
                     == set( k for k in minfo.keys() if k not in ( '_id', 'savetime' ) ) )
            assert datetime_to_utc( info['receivedtime'] ) == datetime_to_utc( minfo['savetime'] )
            assert all( info[k] == minfo[k] for k in [ 'prv_diasourceid', 'prv_diaforcedsourceid' ] )
            assert all( info['info'][k] == minfo['info'][k]
                        for k in ['brokerName', 'classifierName', 'classifierVersion' ] )
            assert all( i['classId'] == m['classId']
                        for i, m in zip( info['info']['classifications'], minfo['info']['classifications'] ) )
            assert all( i['probability'] == pytest.approx( m['probability'], rel=1e-6 )
                        for i, m in zip( info['info']['classifications'], minfo['info']['classifications'] ) )


def test_import_prvforcedsources( import_30days_prvforcedsources ):
    assert import_30days_prvforcedsources == 148
    with db.DB() as conn:
        cursor = conn.cursor()
        cursor.execute( "SELECT * FROM diaforcedsource" )
        rows = cursor.fetchall()
    assert len(rows) == 148

    # TODO : More


def test_import_30days( messy_import_30days, alerts_30days_sent_and_brokermessage_consumed ):
    t0 = alerts_30days_sent_and_brokermessage_consumed
    now = datetime.datetime.now( tz=datetime.UTC )
    nobj, nroot, npos, nsrc, nfrc, ninfo = messy_import_30days
    assert nobj == 12
    assert nroot == 12
    assert npos == 12
    assert nsrc == 77
    assert ninfo == 154
    assert nfrc == 148

    with db.DBCon( dictcursor=True) as pqconn:
        check_database_contents( 30, dbcon=pqconn )
        # Check the hardcoded numbers we have because we know what's in the SNANA-loaded test PPDB
        tablecounts = { 'diaobject': nobj,
                        'root_diaobject': nroot,
                        'diaobject_position': nobj,
                        'diasource': nsrc,
                        'diasource_extra': nsrc,
                        'diasource_brokerinfo': ninfo,
                        'diaforcedsource': nfrc,
                        'diaforcedsource_extra': nfrc
                       }
        for table, num in tablecounts.items():
            q = sql.SQL( "SELECT COUNT(*) FROM {table}" ).format( table=sql.Identifier( table ) )
            assert num == pqconn.execute( q )[0]['count']

        t1 = pqconn.execute( "SELECT t FROM diasource_import_time "
                              "WHERE collection='fastdb_alertcycle_test'" )[0]['t']

        assert t1 < now
        assert t1 > t0


# **********************************************************************
# Now make sure that if we import 30 days, then import 60 days, we get what's expected

def test_import_30days_60days( messy_import_30days, import_30days_60days, test_user ):
    nobj30, nroot30, npos30, nsrc30, nprvfrc30, ninfo30 = messy_import_30days
    nobj60, nroot60, npos60, nsrc60, nprvfrc60, ninfo60 = import_30days_60days
    assert nobj60 == 25
    assert nroot60 == 25
    assert npos60 == 25
    assert nsrc60 == 104
    assert nprvfrc60 == 707
    assert ninfo60 == 208

    with db.DBCon( dictcursor=True ) as pqconn:
        check_database_contents( 90, dbcon=pqconn )
        tablecounts = { 'diaobject': nobj30 + nobj60,
                        'root_diaobject': nroot30 + nroot60,
                        'diaobject_position': npos30 + npos60,
                        'diasource': nsrc30 + nsrc60,
                        'diasource_extra': nsrc30 + nsrc60,
                        'diasource_brokerinfo': ninfo30 + ninfo60,
                        'diaforcedsource': nprvfrc30 + nprvfrc60,
                        'diaforcedsource_extra': nprvfrc30 + nprvfrc60 }
        for table, num in tablecounts.items():
            q = sql.SQL( "SELECT COUNT(*) FROM {table}" ).format( table=sql.Identifier( table ) )
            assert num == pqconn.execute( q )[0]['count']

        objects = pqconn.execute( "SELECT * FROM diaobject" )
        roots = pqconn.execute( "SELECT * FROM root_diaobject" )
        sources = pqconn.execute( "SELECT * FROM diasource" )

    assert min( r['midpointmjdtai'] for r in sources ) == pytest.approx( 60278.029, abs=0.01 )
    assert max( r['midpointmjdtai'] for r in sources ) == pytest.approx( 60362.3266, abs=0.01 )
    assert set( r['id'] for r in roots ) == set( o['rootid'] for o in objects )

    with db.MG() as mongoclient:
        collection = db.get_mongo_collection( mongoclient, "source_thumbnails" )
        assert collection.count_documents( {} ) == nsrc30 + nsrc60


# **********************************************************************
# Test importating the following 60 days when the first 30 days
#   have NOT been imported.  This will test the time cutoffs, and
#   also test that previous sources pulls in things that didn't
#   get pulled in with the direct source import.

def test_import_only_next60days( import_only_next60days ):
    nobj, nroot, npos, nsrc, nfrc, ninfo = import_only_next60days
    assert nobj == 29
    assert nroot == 29
    assert npos == 29
    assert nsrc == 152
    assert nfrc == 770
    assert ninfo == 208

    with db.DBCon( dictcursor=True ) as pqconn:
        check_database_contents( 90, 30, dbcon=pqconn )
        tablecounts = { 'diaobject': nobj,
                        'root_diaobject': nobj,
                        'diaobject_position': nobj,
                        'diasource': nsrc,
                        'diasource_extra': nsrc,
                        'diasource_brokerinfo': ninfo,
                        'diaforcedsource':  nfrc,
                        'diaforcedsource_extra': nfrc
                       }
        for table, num in tablecounts.items():
            q = sql.SQL( "SELECT COUNT(*) FROM {table}" ).format( table=sql.Identifier( table ) )
            assert num == pqconn.execute( q )[0]['count']

        sources = pqconn.execute( "SELECT * FROM diasource" )

    # The min mjd should be greater than the max mjd from test_import_30days
    assert min( r['midpointmjdtai'] for r in sources ) == pytest.approx( 60278.2469, abs=0.01 )
    assert max( r['midpointmjdtai'] for r in sources ) == pytest.approx( 60362.3266, abs=0.01 )

    with db.MG() as mongoclient:
        collection = db.get_mongo_collection( mongoclient, "source_thumbnails" )
        # Only the sources imported directly will have thumbnails; previous sources will not
        # That's why this is less than nsrc
        assert collection.count_documents( {} ) == 104


# **********************************************************************
# Test that even if all 90 days have been consumed from the brokers,
#   if we give the right time cutoff we only import the first 30 days.

def test_import_only_30days_after_90days_consumed( import_only_30days_after_90days_consumed,
                                                   alerts_30days_sent_and_brokermessage_consumed,
                                                   alerts_60moredays_sent_and_brokermessage_consumed ):
    nobj, nroot, npos, nsrc, nfrc, ninfo = import_only_30days_after_90days_consumed
    t30consume = alerts_30days_sent_and_brokermessage_consumed
    t60consume = alerts_60moredays_sent_and_brokermessage_consumed
    now = datetime.datetime.now( tz=datetime.UTC )

    assert nroot == 12
    assert nobj == 12
    assert npos == 12
    assert nsrc == 77
    assert ninfo == 154
    assert nfrc == 148

    # Let's really make sure all 90 days were consumed
    with db.MGCon() as mg:
        assert mg.collection( 'fastdb_alertcycle_test_brokerinfo' ).count_documents( {} ) == 362

    with db.DBCon() as pqconn:
        check_database_contents( 30, dbcon=pqconn )
        tablecounts = { 'diaobject': nobj,
                        'root_diaobject': nroot,
                        'diaobject_position': nobj,
                        'diasource': nsrc,
                        'diasource_extra': nsrc,
                        'diasource_brokerinfo': ninfo,
                        'diaforcedsource': nfrc,
                        'diaforcedsource_extra': nfrc
                       }
        for table, num in tablecounts.items():
            q = sql.SQL( "SELECT COUNT(*) FROM {table}" ).format( table=sql.Identifier( table ) )
            assert num == pqconn.execute( q )[0][0][0]

        t1 = pqconn.execute( "SELECT t FROM diasource_import_time WHERE collection='fastdb_alertcycle_test'" )
        t1 = t1[0][0][0]
        assert t1 < now
        assert t1 < t60consume
        assert t1 >= t30consume



# **********************************************************************
# The test_user fixture is in the next two fixtures not because it's
#   needed for the test, but because this is a convenient test for
#   loading up a database for use developing the web ap.  See the developers documentation for FASTDB.

@pytest.mark.skipif( env_as_bool('RUN_FULL90DAYS'), reason='RUN_FULL90DAYS is set' )
def test_full90days_fast( alerts_90days_sent_received_and_imported, snana_fits_ppdb_loaded ):
    nobj, nroot, npos, nsrc, nfrc, ninfo = alerts_90days_sent_received_and_imported
    assert nobj == 37
    assert nroot == nobj
    assert npos == nobj
    assert nsrc == 181
    assert nfrc == 855
    assert ninfo == 2 * nsrc

    with db.MG() as mongoclient:
        collection = db.get_mongo_collection( mongoclient, "source_thumbnails" )
        assert collection.count_documents( {} ) == nsrc

    check_database_contents( 90 )


@pytest.mark.skipif( not env_as_bool('RUN_FULL90DAYS'), reason='RUN_FULL90DAYS is not set' )
def test_full90days( fully_do_alerts_90days_sent_received_and_imported ):
    nobj, nroot, npos, nsrc, nfrc, ninfo = fully_do_alerts_90days_sent_received_and_imported
    assert nobj == 37
    assert nroot == nobj
    assert npos == nobj
    assert nsrc == 181
    assert nfrc == 855
    assert ninfo == 2 * nsrc

    with db.DBCon( dictcursor=True ) as con:
        assert con.execute( "SELECT COUNT(*) FROM diaobject" )[0]['count'] == nobj
        assert con.execute( "SELECT COUNT(*) FROM diaobject_position" )[0]['count'] == nobj
        assert con.execute( "SELECT COUNT(*) FROM root_diaobject" )[0]['count'] == nobj
        assert con.execute( "SELECT COUNT(*) FROM diasource" )[0]['count'] == nsrc
        assert con.execute( "SELECT COUNT(*) FROM diasource_extra" )[0]['count'] == nsrc
        assert con.execute( "SELECT COUNT(*) FROM diaforcedsource" )[0]['count'] == nfrc
        assert con.execute( "SELECT COUNT(*) FROM diaforcedsource_extra" )[0]['count'] == nfrc
        assert con.execute( "SELECT COUNT(*) FROM diasource_brokerinfo" )[0]['count'] == ninfo

    with db.MG() as mongoclient:
        collection = db.get_mongo_collection( mongoclient, "source_thumbnails" )
        assert collection.count_documents( {} ) == nsrc

    check_database_contents( 90 )
