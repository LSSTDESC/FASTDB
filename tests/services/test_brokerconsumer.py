import pytest
import time
import math
import datetime
import random
import multiprocessing

from services.brokerconsumer import BrokerConsumer, BrokerConsumerLauncher, FinkConsumer
from util import logger, env_as_bool
import db


def check_mongodb( collection_base_name ):
    base = collection_base_name
    with db.MGCon( readonly=True ) as mg:
        expectedcollections = [ f'{base}_{s}' for s in
                                [ 'diaobject', 'diasource', 'diasource_extra',
                                  'diaforcedsource', 'diaforcedsource_extra',
                                  'thumbnails', 'brokerinfo' ] ]
        assert all( i in mg.db.list_collection_names() for i in expectedcollections )

        # 154 sources + 770 previous sources, only 77 unique.  (154 = 2*77... two broker classifiers)
        #   (Sadly, we're not really testing previous source import here, becasue of how
        #   the fixtures are put together, the only previous sources we had were in earlier alerts.)
        msgcursor = mg.collection( f'{base}_diasource' ).find( {}, projection={'diasourceid': 1 } )
        srcids = [ c['diasourceid'] for c in msgcursor ]
        assert len( srcids ) == 154 + 770
        msgcursor = mg.collection( f'{base}_diasource_extra' ).find( {}, projection={'diasourceid': 1 } )
        extsrcids = [ c['diasourceid'] for c in msgcursor ]
        assert len( extsrcids ) == len( srcids )
        srcids = set( srcids )
        extsrcids = set( extsrcids )
        assert len( srcids ) == 77
        assert extsrcids == srcids

        # 1382 previous forced sources, only 148 unique
        msgcursor = mg.collection( f'{base}_diaforcedsource' ).find( {}, projection={'diaforcedsourceid': 1} )
        frcedids = [ c['diaforcedsourceid'] for c in msgcursor ]
        assert len( frcedids ) == 1382
        msgcursor = mg.collection( f'{base}_diaforcedsource_extra' ).find( {}, projection={'diaforcedsourceid': 1} )
        extfrcedids = [ c['diaforcedsourceid'] for c in msgcursor ]
        assert len( extfrcedids ) == len( frcedids )
        frcedids = set( frcedids )
        extfrcedids = set( extfrcedids )
        assert len( frcedids ) == 148
        assert extfrcedids == frcedids

        for broker in [ "FakeBroker-Nugent", "FakeBroker-Random" ]:
            msgcursor = mg.collection( f'{base}_brokerinfo' ).find( { "brokername": broker },
                                                                    projection={'diasourceid': 1} )
            bksrcids = [ c['diasourceid'] for c in msgcursor ]
            assert len( bksrcids ) == len( srcids )
            bksrcids = set( bksrcids)
            assert len( bksrcids ) == len( srcids )
            # This next one would not be true if we had actually imported anything from previous sources
            assert bksrcids == srcids

        # Make sure that the artifical NULLs that fakebroker inserted got properly converted to NaN
        # (This is really here to make sure that we'll be putting the JSON importer through its workout
        # when we test sourceimporter.)
        coll = mg.collection( f'{base}_diaforcedsource' )
        msgcursor = coll.find( { 'msg_diasourceid': 155218500013 } )
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

    # Make sure sources and previous sources match what's expected
    #  (Sadly, because of how this test works, there won't be any
    #   sources in previous sources that weren't also in sources.
    #   that'd be nice to be able to check all functionality.)

    with db.DB() as conn:
        cursor = conn.cursor()
        cursor.execute( "SELECT s.diasourceid FROM ppdb_alerts_sent p\n"
                        "INNER JOIN ppdb_diasource s ON p.diaobjectid=s.diaobjectid AND p.visit=s.visit" )
        srcexpected = set( row[0] for row in cursor.fetchall() )

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
                        "GROUP BY s.diaobjectid, s.visit\n" )
        srcexpected = srcexpected.union( row[0] for row in cursor.fetchall() )
        assert srcexpected == srcids

        cursor.execute( "SELECT DISTINCT ON(f.diaobjectid, f.visit) f.diaforcedsourceid\n"
                        "FROM ppdb_alerts_sent a\n"
                        "INNER JOIN ppdb_diasource s ON a.diaobjectid=s.diaobjectid\n"
                        "                           AND a.visit=s.visit\n"
                        "INNER JOIN ppdb_diaforcedsource f ON f.diaobjectid=s.diaobjectid\n"
                        "                                 AND f.midpointmjdtai<=s.midpointmjdtai-1\n"
                        "GROUP BY f.diaobjectid, f.visit\n" )
        prvfrcedexpected = set( row[0] for row in cursor.fetchall() )
        assert prvfrcedexpected == frcedids

    # TODO : more checks?


def cleanup_mongodb( collection_base_name ):
    with db.MGCon() as mg:
        colnames = [ f'{collection_base_name}_{s}' for s in
                     [ 'diaobject', 'diasource', 'diasource_extra',
                       'diaforcedsource', 'diaforcedsource_extra',
                       'thumbnails', 'brokerinfo' ] ]
        for c in colnames:
            if c in mg.db.list_collection_names():
                mg.collection( c ).drop()
        assert not any( c in mg.db.list_collection_names() for c in colnames )


def test_BrokerConsumer( barf, alerts_30days_sent_and_classified ):
    brokertopic = f'classifications-{barf}'

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
                           'thumbnails', 'brokerinfo' ] ]
            assert all( mg.collection(c).count_documents({}) == 0 for c in colnames )

        # Now make sure it can really poll
        t0 = time.perf_counter()
        bc = BrokerConsumer( 'kafka-server', f'test_BrokerConsumer_{barf}-1', topics=brokertopic,
                             brokername_key='brokerName', nomsg_sleeptime=1, mongodb_collection_base='fastdb_test' )
        bc.poll( restart_time=datetime.timedelta(seconds=10), max_restarts=0, notopic_sleeptime=2 )
        assert time.perf_counter() - t0 < 20

        # Check that the mongo database got populated
        check_mongodb( 'fastdb_test' )

    finally:
        cleanup_mongodb( 'fastdb_test' )


# This next test depends on the file brokerconsumer.yaml in this
#   directory, and assumes that this directory at the location in the
#   dockerfile created by docker-compose.yaml at the root of the
#   git checkout.
#   (i.e., it looks for file /code/tests/services/brokerconsumer.yaml).
def test_BrokerConsumerLauncher( barf, alerts_30days_sent_and_classified ):
    proc = None
    try:
        def launch_launcher():
            bcl = BrokerConsumerLauncher( '/code/tests/services/brokerconsumer.yaml', barf=barf,
                                          logtag='BrokerConsumerLauncher', verbose=True )
            bcl()

        proc = multiprocessing.Process( target=launch_launcher )
        proc.start()
        # Give it 10 seconds to do its stuff
        logger.info( "Sleeping 10s for BrokerConsumerLauncher to do its thing" )
        time.sleep( 10 )
        # Kill the BrokerConsumerLauncher
        logger.info( "Sending TERM to BrokerConsumerLauncher" )
        proc.terminate()
        proc.join()
        logger.info( "Closing BrokerConsumerLauncher" )
        proc.close()
        proc = None

        # Check that the mongo database got populated
        check_mongodb( 'fastdb_test' )

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
    brokertopic = 'fink_sn_near_galaxy_candidate_lsst'

    try:
        t0 = time.perf_counter()
        fc = FinkConsumer( grouptag=barf, fink_topic=brokertopic, mongodb_collection_base='fastdb_fink_test',
                           nomsg_sleeptime=1, batch_size=10 )
        fc.poll( restart_time=datetime.timedelta(seconds=10), max_restarts=0, notopic_sleeptime=2, max_msgs=10 )
        t1 = time.perf_counter()
        logger.info( f"Fink poll finished in {t1-t0} seconds." )

        with db.MGCon() as mg:
            expectedcollections = [ f'fastdb_fink_test_{s}' for s in
                                    [ 'diaobject', 'diasource', 'diasource_extra',
                                      'diaforcedsource', 'diaforcedsource_extra',
                                      'thumbnails', 'brokerinfo' ] ]
            assert all( i in mg.db.list_collection_names() for i in expectedcollections )
            col = mg.collection( 'fastdb_fink_test_brokerinfo' )
            assert col.count_documents({}) >= 10
            srcids = set()
            for doc in col.find( {} ):
                assert doc['brokername'] == 'Fink'
                assert doc['topic'] == brokertopic
                srcids.add( doc['diasourceid'] )

            col = mg.collection( 'fastdb_fink_test_diasource' )
            assert set( c['diasourceid'] for c in col.find({}) ) == srcids

            # Check other stuff?

    finally:
        with db.MGCon() as mg:
            db.collection( 'fastdb_fink_test' ).drop()
