import pytest
import os
import time
import math
import datetime
import random
import multiprocessing
import yaml

from services.projectsim import AlertSender
from services.brokerconsumer import (
    BrokerConsumer,
    BrokerConsumerLauncher,
    FinkConsumer,
    AMPELConsumer,
    AntaresConsumer,
    PittGoogleConsumer
)
from util import FDBLogger, env_as_bool
import db


# This is a fixture that will send all alerts from days 30-90, but NOT
# from days 0-30.  Reason to do this: so that we get sources that will
# only show up in prvDiaSources.
#
# DO NOT USE THIS FIXTURE TOGETHER WITH ANYTHING FROM alertcycle.py
# (other than fakebroker and barf), or chaos will ensue.
@pytest.fixture( scope='module' )
def alerts_30_to_90_sent_and_classified( snana_fits_ppdb_loaded, barf, fakebroker ):
    try:
        with db.DBCon() as dbcon:
            # First, tell the system that it's already sent the first 30 days of alerts so that
            #  it won't send them.
            rows, _cols = dbcon.execute( "SELECT MIN(midpointmjdtai) AS minmjd FROM ppdb_diasource" )
            t = rows[0][0] + 30
            dbcon.execute( "INSERT INTO ppdb_alerts_sent(senttime, diaobjectid, visit) "
                           "SELECT NOW(), diaobjectid, visit FROM ppdb_diasource "
                           "WHERE midpointmjdtai<%(t)s",
                           { 't': t } )
            dbcon.commit()

        sender = AlertSender( 'kafka-server', f'alerts-{barf}', make_cutouts=True )
        nsent = sender( addeddays=60, reallysend=True )
        assert nsent == 104
        FDBLogger.info( "Sleeping 10 seconds to give fakebroker time to catch  up..." )
        time.sleep( 10 )
        FDBLogger.info( "...I hope fakebroker did its stuff." )
        # ****
        # For debugging, reduce output spam
        fakebroker.terminate()
        # ****
        yield nsent, t
    finally:
        with db.DBCon() as dbcon:
            dbcon.execute( "DELETE FROM ppdb_alerts_sent" )
            dbcon.commit()


def check_mongodb( collection_base_name, tfirstalert, cached_alerts=False ):
    base = collection_base_name
    with db.MGCon( readonly=True ) as mg:
        expectedcollections = set( [ f'{base}_{s}' for s in
                                     [ 'diaobject', 'diasource', 'diasource_extra',
                                       'diaforcedsource', 'diaforcedsource_extra',
                                       'thumbnails', 'brokerinfo' ] ] )
        knowncollections = set( mg.db.list_collection_names() )
        assert expectedcollections.issubset( knowncollections )
        if cached_alerts:
            # Because it has no index, it won't actually get created unless
            #   something is written to it.
            assert f'{base}_alertcache' in knowncollections

        # 208 objects, only 29 unique
        msgcursor = mg.collection( f"{base}_diaobject" ).find( {}, projection={'diaobjectid': 1 } )
        objids = [ c['diaobjectid'] for c in msgcursor ]
        assert len( objids ) == 208
        assert len( set(objids) ) == 29

        # Same number of cached alerts if we cached alerts
        nalerts = mg.collection( f'{base}_alertcache' ).count_documents( {} )
        assert nalerts == ( len( objids ) if cached_alerts else 0 )

        # 208 sources + 1326 previous sources, only 152 unique.
        msgcursor = mg.collection( f'{base}_diasource' ).find( {}, projection={'diasourceid': 1 } )
        srcids = [ c['diasourceid'] for c in msgcursor ]
        assert len( srcids ) == 208 + 1326
        msgcursor = mg.collection( f'{base}_diasource_extra' ).find( {}, projection={'diasourceid': 1 } )
        extsrcids = [ c['diasourceid'] for c in msgcursor ]
        assert len( extsrcids ) == len( srcids )
        srcids = set( srcids )
        extsrcids = set( extsrcids )
        assert len( srcids ) == 152
        assert extsrcids == srcids

        # 4044 previous forced sources, only 770 unique
        msgcursor = mg.collection( f'{base}_diaforcedsource' ).find( {}, projection={'diaforcedsourceid': 1} )
        frcedids = [ c['diaforcedsourceid'] for c in msgcursor ]
        assert len( frcedids ) == 4044
        msgcursor = mg.collection( f'{base}_diaforcedsource_extra' ).find( {}, projection={'diaforcedsourceid': 1} )
        extfrcedids = [ c['diaforcedsourceid'] for c in msgcursor ]
        assert len( extfrcedids ) == len( frcedids )
        frcedids = set( frcedids )
        extfrcedids = set( extfrcedids )
        assert len( frcedids ) == 770
        assert extfrcedids == frcedids

        for broker in [ "FakeBroker-Nugent", "FakeBroker-Random" ]:
            msgcursor = mg.collection( f'{base}_brokerinfo' ).find( { "brokername": broker },
                                                                    projection={'diasourceid': 1} )
            bksrcids = [ c['diasourceid'] for c in msgcursor ]
            assert len( bksrcids ) == 104
            bksrcids = set( bksrcids)
            assert len( bksrcids ) == 104
            assert bksrcids.issubset( srcids )

        # Make sure that the artifical NULLs that fakebroker inserted got properly converted to NaN
        # (This is really here to make sure that we'll be putting the JSON importer through its workout
        # when we test sourceimporter.)
        coll = mg.collection( f'{base}_diaforcedsource' )
        msgcursor = coll.find( { 'msg_diasourceid': 198154000011 } )
        num_nans = 0
        num_nones = 0
        for c in msgcursor:
            if math.isnan( c['psfflux'] ) and math.isnan( c['psffluxerr'] ):
                num_nans += 1
            if ( c['psfflux'] is None ) and ( c['psffluxerr'] is None ):
                num_nones = 0
        # Two instances, one for each fakebroker classifier
        assert num_nans == 2
        assert num_nones == 0

        # Slower: make sure lots of stuff matches what's in the alertcache
        FDBLogger.info( "Verifying that saved info matches cached alerts..." )
        if cached_alerts:
            cachedalerts = list( mg.collection( f"{base}_alertcache" ).find( {} ) )
            objects = list( mg.collection( f"{base}_diaobject" ).find( {} ) )
            sources = list( mg.collection( f"{base}_diasource" ).find( {} ) )
            forcedsources = list( mg.collection( f"{base}_diaforcedsource" ).find( {} ) )
            brokerinfos = list( mg.collection( f"{base}_brokerinfo" ).find( {} ) )

            assert ( set( c['msg']['diaObject']['diaObjectId'] for c in cachedalerts )
                     == set( o['diaobjectid'] for o in objects ) )
            allsources = set( c['msg']['diaSourceId'] for c in cachedalerts )
            assert allsources.issubset( set( s['diasourceid'] for s in sources ) )
            assert allsources == set( b['diasourceid'] for b in brokerinfos )
            allforcedsources = set()
            for c in cachedalerts:
                if c['msg']['prvDiaSources'] is not None:
                    allsources = allsources.union( set( m['diaSourceId'] for m in c['msg']['prvDiaSources'] ) )
                if c['msg']['prvDiaForcedSources'] is not None:
                    allforcedsources = allforcedsources.union( set( m['diaForcedSourceId']
                                                                    for m in c['msg']['prvDiaForcedSources'] ) )
            assert allsources == set( s['diasourceid'] for s in sources )
            assert allforcedsources == set( f['diaforcedsourceid'] for f in forcedsources )

            # TODO : check that the actual contents of the various collections match the contents
            #   of the alert cache.  (Here we just check brokerinfo.)

            for b in brokerinfos:
                cs = [ c for c in cachedalerts if c['msg']['diaSourceId'] == b['diasourceid'] ]
                assert ( ( b['brokername'], b['topic'], b['diasourceid'] )
                         in set( ( c['brokername'], c['topic'], c['msg']['diaSourceId'] ) for c in cs ) )
                for c in cs:
                    if ( b['brokername'], b['topic'] ) == ( c['brokername'], c['topic'] ):
                        assert b['diaobjectid'] is not None
                        assert b['diaobjectid'] == c['msg']['diaObject']['diaObjectId']
                        assert b['info'] == { k:v for k, v in c['msg'].items()
                                              if k not in BrokerConsumer._standard_lsst_alert_fields }
                    if b['prv_diasourceid'] is None:
                        assert c['msg']['prvDiaSources'] is None
                    else:
                        assert b['prv_diasourceid'] == [ m['diaSourceId'] for m in c['msg']['prvDiaSources'] ]
                    if b['prv_diaforcedsourceid'] is None:
                        assert c['msg']['prvDiaForcedSources'] is None
                    else:
                        assert b['prv_diaforcedsourceid'] == [ m['diaForcedSourceId']
                                                               for m in c['msg']['prvDiaForcedSources'] ]

            for c in cachedalerts:
                assert ( ( c['brokername'], c['topic'], c['msg']['diaSourceId'] )
                         in set( ( b['brokername'], b['topic'], b['diasourceid'] ) for b in brokerinfos ) )

        FDBLogger.info( "...done verifying that saved info matches cached alerts." )

    # Make sure sources and previous sources match what's expected
    #  (Sadly, because of how this test works, there won't be any
    #   sources in previous sources that weren't also in sources.
    #   that'd be nice to be able to check all functionality.)

    FDBLogger.info( "Verifying that consumed source ids match alerts sent..." )
    with db.DB() as conn:
        cursor = conn.cursor()
        cursor.execute( "SELECT s.diasourceid FROM ppdb_alerts_sent p\n"
                        "INNER JOIN ppdb_diasource s ON p.diaobjectid=s.diaobjectid AND p.visit=s.visit\n"
                        "WHERE s.midpointmjdtai>=%(t)s",
                        { 't': tfirstalert } )
        srcexpected = set( row[0] for row in cursor.fetchall() )
        assert len( srcexpected ) == 104

        # For LSST, diaobjectid is not reliable and cannot be used this way.
        # But, its OK for the sample data set we have. And, since when I
        #   wrote this all, I as assuming that we had to use
        #   (diaobjectid, visit), because DP1 was missing either
        #   diasourceid or diaforcesdourceid (I forget which), I used
        #   that.  Sigh.
        cursor.execute( "SELECT DISTINCT ON(s.diaobjectid, s.visit) s.diasourceid\n"
                        "FROM ppdb_alerts_sent a\n"
                        "INNER JOIN ppdb_diasource sprime ON a.diaobjectid=sprime.diaobjectid\n"
                        "                                AND a.visit=sprime.visit\n"
                        "INNER JOIN ppdb_diasource s ON s.diaobjectid=sprime.diaobjectid\n"
                        "                           AND s.midpointmjdtai<sprime.midpointmjdtai\n"
                        "WHERE sprime.midpointmjdtai>=%(t)s\n"
                        "GROUP BY s.diaobjectid, s.visit\n",
                        { 't': tfirstalert } )
        srcexpected = srcexpected.union( row[0] for row in cursor.fetchall() )
        assert srcexpected == srcids

        cursor.execute( "SELECT DISTINCT ON(f.diaobjectid, f.visit) f.diaforcedsourceid\n"
                        "FROM ppdb_alerts_sent a\n"
                        "INNER JOIN ppdb_diasource s ON a.diaobjectid=s.diaobjectid\n"
                        "                           AND a.visit=s.visit\n"
                        "INNER JOIN ppdb_diaforcedsource f ON f.diaobjectid=s.diaobjectid\n"
                        "                                 AND f.midpointmjdtai<=s.midpointmjdtai-1\n"
                        "WHERE s.midpointmjdtai>=%(t)s\n"
                        "GROUP BY f.diaobjectid, f.visit\n",
                        { 't': tfirstalert } )
        prvfrcedexpected = set( row[0] for row in cursor.fetchall() )
        assert prvfrcedexpected == frcedids

    # TODO : more checks?

    FDBLogger.info( "...done verifying that consumed source ids match alerts sent" )


def cleanup_mongodb( collection_base_name ):
    with db.MGCon() as mg:
        colnames = [ f'{collection_base_name}_{s}' for s in
                     [ 'diaobject', 'diasource', 'diasource_extra',
                       'diaforcedsource', 'diaforcedsource_extra',
                       'thumbnails', 'brokerinfo', 'alertcache' ] ]
        for c in colnames:
            if c in mg.db.list_collection_names():
                mg.collection( c ).drop()
        assert not any( c in mg.db.list_collection_names() for c in colnames )


def test_BrokerConsumer( barf, alerts_30_to_90_sent_and_classified ):
    brokertopic = f'classifications-{barf}'
    nsent, tfirstalert = alerts_30_to_90_sent_and_classified
    assert nsent == 104

    try:
        # First, make sure it times out properly if it never sees a topic
        t0 = time.perf_counter()
        bc = BrokerConsumer( 'kafka-server', f'test_BrokerConsumer_{barf}-0', topics='this_topic_does_not_exist',
                             brokername_key='brokerName', nomsg_sleeptime=1, mongodb_collection_base='fastdb_test' )
        bc.poll( restart_time=datetime.timedelta(seconds=3), max_restarts=2, notopic_sleeptime=2 )
        assert time.perf_counter() - t0 < 10

        # (Make sure nothing got saved in mongo.)
        with db.MGCon() as mg:
            colnames = [ f'fastdb_test_{s}' for s in
                         [ 'diaobject', 'diasource', 'diasource_extra',
                           'diaforcedsource', 'diaforcedsource_extra',
                           'thumbnails', 'brokerinfo', 'alertcache' ] ]
            assert all( mg.collection(c).count_documents({}) == 0 for c in colnames )

        # Now make sure it can really poll
        t0 = time.perf_counter()
        bc = BrokerConsumer( 'kafka-server', f'test_BrokerConsumer_{barf}-1', topics=brokertopic,
                             brokername_key='brokerName', nomsg_sleeptime=1, mongodb_collection_base='fastdb_test' )
        bc.poll( restart_time=datetime.timedelta(seconds=10), max_restarts=0, notopic_sleeptime=2 )
        assert time.perf_counter() - t0 < 20

        # Check that the mongo database got populated
        check_mongodb( 'fastdb_test', tfirstalert )

        cleanup_mongodb( 'fastdb_test' )

        # Make sure stuff gets saved if we try to cache alerts
        t0 = time.perf_counter()
        bc = BrokerConsumer( 'kafka-server', f'test_BrokerConsumer_{barf}-2', topics=brokertopic,
                             brokername_key='brokerName', nomsg_sleeptime=1, mongodb_collection_base='fastdb_test',
                             cache_alerts=True )
        bc.poll( restart_time=datetime.timedelta(seconds=10), max_restarts=0, notopic_sleeptime=2 )
        assert time.perf_counter() - t0 < 20
        check_mongodb( 'fastdb_test', tfirstalert, cached_alerts=True )

    finally:
        cleanup_mongodb( 'fastdb_test' )


# This next test depends on the file brokerconsumer.yaml in this
#   directory, and assumes that this directory at the location in the
#   dockerfile created by docker-compose.yaml at the root of the
#   git checkout.
#   (i.e., it looks for file /code/tests/services/brokerconsumer.yaml).
def test_BrokerConsumerLauncher( barf, alerts_30_to_90_sent_and_classified ):
    _nsent, tfirstalert = alerts_30_to_90_sent_and_classified

    proc = None
    try:
        def launch_launcher( barf2=None ):
            bcl = BrokerConsumerLauncher( '/code/tests/services/brokerconsumer.yaml', barf=barf, barf2=barf2,
                                          logtag='BrokerConsumerLauncher', verbose=True )
            bcl()

        # Yes, we're doing processes within processes
        # (BrokerConsumerLauncher launches its own subprocesses).  We're
        # doing this because the prodution working mode is going to be
        # BrokerConsumerLauncher running on a server somewhere, and we
        # need to be able to send it a TERM (or INT or whatever) signal
        # and have it shut down cleanly (including a clean shutdown of
        # all its subprocesses).  So, set up the same structure here.
        # (Really need more robust testing to make sure the clean
        # shutdown happened, other than just looking at logs....)


        # First, let it run its full course.  The config tells it to
        # restart every 10s, and has a max_restarts of 2, so it should
        # run for 20s, plus whatever startup overhead there is.
        # There may be a few more seconds because of waiting for the
        # subprocesses' consume_timeout (which is 1s) to time out...
        # and whatever overhead there is in getting started
        # (subscriptions, etc).

        proc = multiprocessing.Process( target=launch_launcher )
        FDBLogger.info( "Starting BrokerConsumerLauncher" )
        t0 = time.monotonic()
        proc.start()
        proc.join()
        t1 = time.monotonic()
        FDBLogger.info( f"BrokerConsumerLauncher exited after {t1-t0} seconds." )
        proc.close()
        proc=None
        assert t1 - t0 > 20
        assert t1 - t0 < 25
        check_mongodb( 'fastdb_test', tfirstalert )

        cleanup_mongodb( 'fastdb_test' )

        # Now, launch it, but send it a TERM after 5s.  It should thus
        # only run 5s... plus startup overhead, additional time for the
        # BCL process to tell its own subprocesses to die, which could
        # be a round trip of a second or two.

        proc = multiprocessing.Process( target=launch_launcher, args=[f'{barf}-1'] )
        FDBLogger.info( "Starting BrokerConsumerLauncher" )
        t0 = time.monotonic()
        proc.start()
        FDBLogger.info( "Sleeping 5s for BrokerConsumerLauncher to do its thing" )
        time.sleep( 5 )
        # Kill the BrokerConsumerLauncher
        FDBLogger.info( "Sending TERM to BrokerConsumerLauncher" )
        proc.terminate()
        proc.join()
        t1 = time.monotonic()
        FDBLogger.info( f"BrokerConsumerLauncher exited after {t1-t0} seconds." )
        proc.close()
        proc = None
        assert t1 - t0 > 5
        assert t1 - t0 < 10
        check_mongodb( 'fastdb_test', tfirstalert )

    finally:
        if proc is not None:
            proc.kill()
        cleanup_mongodb( 'fastdb_test' )


# TODO : write tests that use the "60days" fixtures?


# ======================================================================
# ======================================================================
# Tests of individual brokers
#
# Disabled by default because these depend on external servbers with
#  topics that have alerts in them

@pytest.mark.skipif( not env_as_bool('RUN_FINK_TESTS'), reason='RUN_FINK_TESTS is not set' )
def test_fink():
    barf = "".join( random.choices( 'abcdefghijklmnopqrstuvwxyz', k=6 ) )
    groupid = f'rknop-fastdb-test-{barf}'
    # 'fink_sn_near_galaxy_candidate_lsst' is a live topic
    # 'ftransfer_lsst_2026-03-20_872471' is a topic Mohammed made that will only live through Mar 27
    # brokertopic = 'fink_sn_near_galaxy_candidate_lsst'
    # schema_topic = None
    # schemaless = False
    # schema_in_key = True
    brokertopic = 'ftransfer_lsst_2026-03-20_872471'
    schema_topic = 'ftransfer_lsst_2026-03-20_872471_schema'
    schemaless = False
    schema_in_key = True

    expectedcollections = [ f'fastdb_fink_test_{s}' for s in
                            [ 'diaobject', 'diasource', 'diasource_extra',
                              'diaforcedsource', 'diaforcedsource_extra',
                              'thumbnails', 'brokerinfo', 'alertcache' ] ]

    try:
        t0 = time.perf_counter()
        fc = FinkConsumer( groupid=groupid, topics=brokertopic, schema_topic=schema_topic,
                           schemaless=schemaless, schema_in_key=schema_in_key,
                           mongodb_collection_base='fastdb_fink_test',
                           consume_timeout=1, nomsg_sleeptime=1, batch_size=10,
                           cache_alerts=True )
        fc.poll( restart_time=datetime.timedelta(seconds=10), max_restarts=0, notopic_sleeptime=2, max_msgs=10 )
        t1 = time.perf_counter()
        FDBLogger.info( f"Fink poll finished in {t1-t0} seconds." )

        with db.MGCon() as mg:
            assert all( i in mg.db.list_collection_names() for i in expectedcollections )
            nalerts = mg.collection( 'fastdb_fink_test_alertcache' ).count_documents({})
            assert nalerts >= 10
            col = mg.collection( 'fastdb_fink_test_brokerinfo' )
            assert col.count_documents({}) == nalerts
            srcids = set()
            for doc in col.find( {} ):
                assert doc['brokername'] == 'Fink'
                assert doc['topic'] == brokertopic
                srcids.add( doc['diasourceid'] )

            col = mg.collection( 'fastdb_fink_test_diasource' )
            assert srcids.issubset( set( c['diasourceid'] for c in col.find({}) ) )

            # Check other stuff?

        # Uncomment these next two to manually inspect the saved mongo collections
        import pdb; pdb.set_trace()
        pass

    finally:
        with db.MGCon() as mg:
            for col in expectedcollections:
                mg.collection( col ).drop()


@pytest.mark.skipif( not env_as_bool('RUN_FINK_TESTS'), reason='RUN_FINK_TESTS is not set' )
def test_fink_launcher():
    barf = "".join( random.choices( 'abcdefghijklmnopqrstuvwxyz', k=6 ) )

    expectedcollections = [ f'fastdb_fink_launcher_test_{s}' for s in
                            [ 'diaobject', 'diasource', 'diasource_extra',
                              'diaforcedsource', 'diaforcedsource_extra',
                              'thumbnails', 'brokerinfo', 'alertcache' ] ]

    proc = None
    try:
        def launch_broker():
            bcl = BrokerConsumerLauncher( '/code/tests/services/brokerconsumer_fink.yaml', barf=barf,
                                          logtag='FinkBrokerConsumerLauncher', verbose=True )
            bcl()

        proc = multiprocessing.Process( target=launch_broker )
        proc.start()
        FDBLogger.info( "Sleeping 10s for BrokerConsumerLauncher to do its thing" )
        time.sleep( 10 )
        FDBLogger.info( "Sending TERM to BrokerConsumerLauncher" )
        proc.terminate()
        proc.join()
        FDBLogger.info( "Closing BrokerConsumerLauncher" )
        proc.close()
        proc = None

        with db.MGCon() as mg:
            assert all( i in mg.db.list_collection_names() for i in expectedcollections )
            nalerts = mg.collection( 'fastdb_fink_launcher_test_alertcache' ).count_documents({})
            assert nalerts >= 10
            col = mg.collection( 'fastdb_fink_launcher_test_brokerinfo' )
            assert col.count_documents({}) == nalerts
            srcids = set()
            for doc in col.find({}):
                assert doc['brokername'] == "LaunchedFink"
                assert doc['topic'] == 'fink_sn_near_galaxy_candidate_lsst'
                srcids.add( doc['diasourceid'] )

            col = mg.collection( 'fastdb_fink_launcher_test_diasource' )
            assert srcids.issubset( set( c['diasourceid'] for c in col.find({}) ) )

    finally:
        if proc is not None:
            proc.kill()
        with db.MGCon() as mg:
            for col in expectedcollections:
                mg.collection( col ).drop()




@pytest.mark.skipif( not env_as_bool('RUN_PITTGOOGLE_TESTS'), reason='RUN_PITTGOOGLE_TESTS is not set' )
def test_pittgoogle():
    barf = "".join( random.choices( 'abcdefghijklmnopqrstuvwxyz', k=6 ) )
    brokertopic = 'loop'
    groupid = f'fastdb-test-{barf}'
    os.environ['GOOGLE_CLOUD_PROJECT'] = 'fastdb-test-20251103'
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/secrets/fastdb-test-20251103-5a0f5182da01.json'

    expectedcollections = [ f'fastdb_test_pittgoogle_{s}' for s in
                            [ 'diaobject', 'diasource', 'diasource_extra',
                              'diaforcedsource', 'diaforcedsource_extra',
                              'thumbnails', 'brokerinfo', 'alertcache' ] ]

    try:
        t0 = time.perf_counter()
        pgb = PittGoogleConsumer( groupid=groupid, max_workers=2, batch_size=10, consume_timeout=2,
                                  survey='lsst', topic_name=brokertopic, cache_alerts=True,
                                  schemafile='/fastdb/share/avsc/lsst.v10_0.alert.avsc',
                                  mongodb_collection_base='fastdb_test_pittgoogle' )
        FDBLogger.info( "Running PittGoogleBroker.poll() for 10s...." )
        pgb.poll( restart_time=datetime.timedelta( seconds=10 ), max_restarts=1 )
        dt = time.perf_counter() - t0
        FDBLogger.info( f"Returned from PittGoogleBroker.poll(), it handled {pgb.tot_n_messages_consumed} messages.  "
                        f"Creation plus poll time: {dt:.2f} sec." )

        with db.MGCon() as mg:
            assert all( i in mg.db.list_collection_names() for i in expectedcollections )
            nalerts = mg.collection( 'fastdb_test_pittgoogle_alertcache' ).count_documents({})
            assert nalerts == pgb.tot_n_messages_consumed
            assert nalerts > 3
            col = mg.collection( 'fastdb_test_pittgoogle_brokerinfo' )
            assert col.count_documents({}) == nalerts
            srcids = set()
            for doc in col.find( {} ):
                assert doc['brokername'] == 'Pitt-Google'
                assert doc['topic'] == f'lsst-{brokertopic}'
                srcids.add( doc['diasourceid'] )

            col = mg.collection( 'fastdb_test_pittgoogle_diasource' )
            assert srcids.issubset( set( c['diasourceid'] for c in col.find({}) ) )

            # Check other stuff?

    finally:
        with db.MGCon() as mg:
            for col in expectedcollections:
                mg.collection( col ).drop()


@pytest.mark.skipif( not env_as_bool('RUN_PITTGOOGLE_TESTS'), reason='RUN_PITTGOOGLE_TESTS is not set' )
def test_pittgoogle_launcher():
    os.environ['GOOGLE_CLOUD_PROJECT'] = 'fastdb-test-20251103'
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/secrets/fastdb-test-20251103-5a0f5182da01.json'

    barf = "".join( random.choices( 'abcdefghijklmnopqrstuvwxyz', k=6 ) )
    expectedcollections = [ f'pittgoogle_launcher_test_{s}' for s in
                            [ 'diaobject', 'diasource', 'diasource_extra',
                              'diaforcedsource', 'diaforcedsource_extra',
                              'thumbnails', 'brokerinfo', 'alertcache' ] ]
    proc = None

    try:
        def check_pittgoogle_mongodb():
            with db.MGCon() as mg:
                assert all( i in mg.db.list_collection_names() for i in expectedcollections )
                nalerts = mg.collection( 'pittgoogle_launcher_test_alertcache' ).count_documents({})
                assert nalerts >= 3
                col = mg.collection( 'pittgoogle_launcher_test_brokerinfo' )
                assert col.count_documents({}) == nalerts
                srcids = set()
                for doc in col.find({}):
                    assert doc['brokername'] == "Pitt-Google"
                    assert doc['topic'] == 'lsst-loop'
                    srcids.add( doc['diasourceid'] )

                col = mg.collection( 'pittgoogle_launcher_test_diasource' )
                assert srcids.issubset( set( c['diasourceid'] for c in col.find({}) ) )

        def launch_broker( barf2=None ):
            bcl = BrokerConsumerLauncher( '/code/tests/services/brokerconsumer_pittgoogle.yaml',
                                          logtag='PittGoogleConsumerLauncher',
                                          barf=barf, barf2=barf2, verbose=True )
            bcl()

        # First, test, allowing the broker to run through 2 10s restarts
        #   (configured in brokerconsumer_pittgoogle.yaml) and exit
        #   itself.  It should run for ~40s : the 2 10s restarts,
        #   (though they may be 12s because it only check the clock
        #   every 2s, so it might just miss something), plus the 20s
        #   grace period it gives subprocesses to shut down.

        proc = multiprocessing.Process( target=launch_broker )
        t0 = time.perf_counter()
        proc.start()
        FDBLogger.info( "Waiting for BrokerConsumerLauncher to exit." )
        proc.join()
        dt = time.perf_counter() - t0
        FDBLogger.info( f"BrokerConsumerLauncher exited after {dt:.2f} seconds" )
        proc.close()
        proc = None
        assert dt > 38
        assert dt < 44
        check_pittgoogle_mongodb()

        # Now, tell BrokerConsumerLauncher to die after 10s.  The main
        #   process has a loop inside that waits 2s between polling to
        #   see if there are heartbeats from the processes that actually
        #   listen to the brokers.  It catches SIGINT and SIGTERM to
        #   just set a flag, and it also checks that flag during the
        #   same ever-2-second timeout.  So, it should run at most 14+20
        #   seconds (allowing for slop on both sides, and the 20s grace
        #   period it gives for all subprocesses to cleanly exit).

        proc = multiprocessing.Process( target=launch_broker, args=[f'{barf}-1'] )
        t0 = time.perf_counter()
        proc.start()
        FDBLogger.info( "Sleeping 30s for BrokerConsumerLauncher to do its thing" )
        time.sleep( 30 )
        FDBLogger.info( "Sending TERM to BrokerConsumerLauncher" )
        proc.terminate()
        proc.join()
        dt = time.perf_counter - t0
        FDBLogger.info( "Closing BrokerConsumerLauncher" )
        proc.close()
        proc = None
        assert dt > 30
        assert dt < 35
        check_pittgoogle_mongodb()

    finally:
        if proc is not None:
            proc.kill()
        with db.MGCon() as mg:
            for col in expectedcollections:
                mg.collection( col ).drop()



@pytest.mark.skipif( not env_as_bool('RUN_AMPEL_TESTS'), reason='RUN_AMPEL_TESTS is not set' )
def test_ampel_justpull():
    brokertopic = 'ampel.lsst.extragalactic-transients'
    barf = "".join( random.choices( 'abcdefghijklmnopqrstuvwxyz', k=6 ) )
    # TODO : to actually run this test, you have to have to have a file with format
    #   username: <username>
    #   password: <password>
    # with Scimma credentials
    scimmacredfilename = os.getenv( 'SCIMMACREDS', None )
    if scimmacredfilename is None:
        raise RuntimeError( "You need a file with SCiMMA credentials; see test source" )
    creds = yaml.safe_load( open(scimmacredfilename) )
    if ( 'username' not in creds ) or ( 'password' not in creds ):
        raise RuntimeError( f"{scimmacredfilename} must have both username and password" )
    username = creds['username']
    password = creds['password']
    groupid = f'{username}-fastdb-test-{barf}'

    try:
        t0 = time.perf_counter()
        ac = AMPELConsumer( groupid=groupid, topics=brokertopic, mongodb_collection_base='fastdb_test_ampel',
                            consume_timeout=1, nomsg_sleeptime=1, batch_size=10, cache_alerts=True, no_wrangle=True,
                            username=username, password=password )
        ac.poll( restart_time=datetime.timedelta(seconds=10), max_restarts=0, notopic_sleeptime=2, max_msgs=10 )
        t1 = time.perf_counter()
        FDBLogger.info( f"AMPEL poll finished in {t1-t0} seconds." )

        with db.MGCon() as mg:
            assert 'fastdb_test_ampel_alertcache' in mg.db.list_collection_names()
            col = mg.collection( 'fastdb_test_ampel_alertcache' )
            assert col.count_documents({}) >= 10

        # Uncomment these next two to manually inspect the saved mongo collections
        # import pdb; pdb.set_trace()
        # pass

    finally:
        with db.MGCon() as mg:
            mg.collection( 'fastdb_test_ampel_alertcache' ).drop()


@pytest.mark.skipif( not env_as_bool('RUN_ANTARES_TESTS'), reason='RUN_ANTARES_TESTS is not set' )
def test_antares():
    barf = "".join( random.choices( 'abcdefghijklmnopqrstuvwxyz', k=6 ) )
    groupid = f'rknop-fastdb-test-{barf}'
    brokertopic = None
    # TODO : to actually run this test, you have to have to have a file with format
    #   username: <username>
    #   password: <password>
    # with antares
    antarescredfilename = os.getenv( 'ANTARESCREDS', None )
    if antarescredfilename is None:
        raise RuntimeError( "You need a file with ANTARES cedentials; see test source" )
    creds = yaml.safe_load( open(antarescredfilename) )
    if ( 'username' not in creds ) or ( 'password' not in creds ):
        raise RuntimeError( f'{antarescredfilename} must have both username and password' )
    username = creds['username']
    password = creds['password']

    try:
        t0 = time.perf_counter()
        ac = AntaresConsumer( groupid=groupid, topics=brokertopic, mongodb_collection_base='fastdb_antares_test',
                              consume_timeout=1, nomsg_sleeptime=1, batch_size=10, cache_alerts=True, no_wrangle=True,
                              username=username, password=password )
        # REmove this with a call to poll once I've figured out how to use ANTARES
        ac.create_connection()
        t1 = time.perf.counter()
        FDBLogger.info( f"Created ANTARES connection in {t1-t0} seconds" )
        import pdb; pdb.set_trace()
        pass

    finally:
        with db.MGCon() as mg:
            mg.collection( 'fastdb_antares_test' ).drop()
