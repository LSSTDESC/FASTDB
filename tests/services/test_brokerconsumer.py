import pytest
import os
import time
import datetime
import random
import multiprocessing

from services.brokerconsumer import BrokerConsumer, BrokerConsumerLauncher, FinkConsumer
from util import logger, env_as_bool
import db


def check_mongodb( mongoclient, dbname, collection ):
    brokermessages = getattr( mongoclient, dbname )

    assert collection in brokermessages.list_collection_names()

    coll = getattr( brokermessages, collection )

    # 77 diasources in the database, two classifiers per alert = 154 broker messages
    assert coll.count_documents({}) == 154

    # Pull out the diaSourceId from all the messages, make sure we got the right amount
    mgcursor = coll.find( {}, projection={ 'diaobjectid': 1, 'diasource.visit': 1 } )
    srcids = [ f"{c['diaobjectid']}_{c['diasource']['visit']}" for c in mgcursor ]
    assert len(srcids) == 154
    srcids = set( srcids )
    assert len(srcids) == 77

    # Pull out the previous diasources

    msgcursor = coll.aggregate( [ { "$unwind": "$prvdiasources" } ] )
    prvsrcids = [ f"{c['prvdiasources']['diaobjectid']}_{c['prvdiasources']['visit']}" for c in msgcursor ]
    assert len(prvsrcids) == 770
    # But they're mostly redundant
    prvsrcids = set( prvsrcids )
    assert len(prvsrcids) == 65

    msgcursor = coll.aggregate( [ { "$unwind": "$prvdiasources_extra" } ] )
    prvsrcextraids = [ f"{c['prvdiasources_extra']['diaobjectid']}_{c['prvdiasources_extra']['visit']}"
                       for c in msgcursor ]
    assert len(prvsrcextraids) == 770
    prvsrcextraids = set( prvsrcextraids )
    assert prvsrcextraids == prvsrcids

    # Pull out the previous diaforcedsources

    msgcursor = coll.aggregate( [ { "$unwind": "$prvdiaforcedsources" } ] )
    prvfrcedids = [ f"{c['prvdiaforcedsources']['diaobjectid']}_{c['prvdiaforcedsources']['visit']}"
                    for c in msgcursor ]
    assert len(prvfrcedids) == 1382
    prvfrcedids = set( prvfrcedids )
    assert len(prvfrcedids) == 148

    msgcursor = coll.aggregate( [ { "$unwind": "$prvdiaforcedsources_extra" } ] )
    prvfrcedextraids = [ f"{c['prvdiaforcedsources_extra']['diaobjectid']}_{c['prvdiaforcedsources_extra']['visit']}"
                         for c in msgcursor ]
    assert len(prvfrcedextraids) == 1382
    prvfrcedextraids = set( prvfrcedextraids )
    assert prvfrcedextraids == prvfrcedids

    msgcursor = list( coll.find( { "brokername": "FakeBroker-Nugent" },
                                 projection={ 'diasource.diasourceid': 1, 'brokername': 1 } ) )
    assert all( c['brokername'] == 'FakeBroker-Nugent' for c in msgcursor )
    nugentsrces = [ c['diasource']['diasourceid'] for c in msgcursor ]
    assert len(nugentsrces) == 77
    nugentsrces = set( nugentsrces )
    assert len(nugentsrces) == 77
    msgcursor = list( coll.find( { "brokername": "FakeBroker-Random" },
                                 projection={ 'diasource.diasourceid': 1, 'brokername': 1 } ) )
    assert all( c['brokername'] == 'FakeBroker-Random' for c in msgcursor )
    randomsrces = [ c['diasource']['diasourceid'] for c in msgcursor ]
    assert len(randomsrces) == 77
    randomsrces = set( randomsrces )
    assert randomsrces == nugentsrces

    # Make sure sources and previous sources match what's expected
    #  (Sadly, because of how this test works, there won't be any
    #   sources in previous sources that weren't also in sources.
    #   that'd be nice to be able to check all functionality.)

    with db.DB() as conn:
        cursor = conn.cursor()
        cursor.execute( "SELECT diaobjectid, visit FROM ppdb_alerts_sent" )
        alertssent = set( f"{row[0]}_{row[1]}" for row in cursor.fetchall() )
        assert alertssent == srcids

        cursor.execute( "SELECT DISTINCT ON(s.diaobjectid, s.visit) s.diaobjectid, s.visit\n"
                        "FROM ppdb_alerts_sent a\n"
                        "INNER JOIN ppdb_diasource sprime ON a.diaobjectid=sprime.diaobjectid\n"
                        "                                AND a.visit=sprime.visit\n"
                        "INNER JOIN ppdb_diasource s ON s.diaobjectid=sprime.diaobjectid\n"
                        "                           AND s.midpointmjdtai<sprime.midpointmjdtai\n"
                        "GROUP BY s.diaobjectid, s.visit\n" )
        prvsrcexpected = set( f"{row[0]}_{row[1]}" for row in cursor.fetchall() )
        assert prvsrcexpected == prvsrcids

        cursor.execute( "SELECT DISTINCT ON(f.diaobjectid, f.visit) f.diaobjectid, f.visit\n"
                        "FROM ppdb_alerts_sent a\n"
                        "INNER JOIN ppdb_diasource s ON a.diaobjectid=s.diaobjectid\n"
                        "                           AND a.visit=s.visit\n"
                        "INNER JOIN ppdb_diaforcedsource f ON f.diaobjectid=s.diaobjectid\n"
                        "                                 AND f.midpointmjdtai<=s.midpointmjdtai-1\n"
                        "GROUP BY f.diaobjectid, f.visit\n" )
        prvfrcedexpected = set( f"{row[0]}_{row[1]}" for row in cursor.fetchall() )
        assert prvfrcedexpected == prvfrcedids

    # TODO : more checks?


def cleanup_mongodb( mongoclient_rw, dbname, collection ):
    brokermessages = getattr( mongoclient_rw, dbname )
    if collection in brokermessages.list_collection_names():
        coll = getattr( brokermessages, collection )
        coll.drop()
    assert collection not in brokermessages.list_collection_names()


def test_BrokerConsumer( barf, alerts_30days_sent_and_classified, mongoclient, mongoclient_rw ):
    brokertopic = f'classifications-{barf}'
    dbname = os.getenv( 'MONGODB_DBNAME' )
    assert dbname is not None
    collection = f'fastdb_{barf}'

    try:
        # First, make sure it times out properly if it never sees a topic
        t0 = time.perf_counter()
        bc = BrokerConsumer( 'kafka-server', f'test_BrokerConsumer_{barf}-0', topics='this_topic_does_not_exist',
                             brokername_key='brokerName', mongodb_collection=collection, nomsg_sleeptime=1 )
        bc.poll( restart_time=datetime.timedelta(seconds=3), max_restarts=2, notopic_sleeptime=2 )
        assert time.perf_counter() - t0 < 10

        # Now make sure it can really poll
        t0 = time.perf_counter()
        bc = BrokerConsumer( 'kafka-server', f'test_BrokerConsumer_{barf}-1', topics=brokertopic,
                             brokername_key='brokerName', mongodb_collection=collection, nomsg_sleeptime=1 )
        bc.poll( restart_time=datetime.timedelta(seconds=10), max_restarts=0, notopic_sleeptime=2 )
        assert time.perf_counter() - t0 < 20

        # Check that the mongo database got populated
        check_mongodb( mongoclient, dbname, collection )

    finally:
        cleanup_mongodb( mongoclient_rw, dbname, collection )


# This next test depends on the file brokerconsumer.yaml in this
#   directory, and assumes that this directory at the location in the
#   dockerfile created by docker-compose.yaml at the root of the
#   git checkout.
#   (i.e., it looks for file /code/tests/services/brokerconsumer.yaml).
def test_BrokerConsumerLauncher( barf, alerts_30days_sent_and_classified, mongoclient, mongoclient_rw ):
    dbname = os.getenv( 'MONGODB_DBNAME' )
    assert dbname is not None
    collection = f'fastdb_{barf}'

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
        check_mongodb( mongoclient, dbname, collection )

    finally:
        if proc is not None:
            proc.kill()
        cleanup_mongodb( mongoclient_rw, dbname, collection )


# TODO : write tests that use the "60days" fixtures?


# ======================================================================
# ======================================================================
# Tests of individual brokers
#
# Disabled by default because these depend on external servbers with
#  topics that have alerts in them

@pytest.mark.skipif( not env_as_bool('RUN_FINK_TESTS'), reason='RUN_FINK_TESTS is not set' )
def test_fink( mongoclient, mongoclient_rw ):
    barf = "".join( random.choices( 'abcdefghijklmnopqrstuvwxyz', k=6 ) )
    brokertopic = 'fink_sn_near_galaxy_candidate_lsst'
    dbname = os.getenv( 'MONGODB_DBNAME' )
    assert dbname is not None
    collection = f'fastdb_{barf}'

    try:
        t0 = time.perf_counter()
        fc = FinkConsumer( grouptag=barf, fink_topic=brokertopic, mongodb_collection=collection,
                           nomsg_sleeptime=1, batch_size=10 )
        fc.poll( restart_time=datetime.timedelta(seconds=10), max_restarts=0, notopic_sleeptime=2, max_msgs=10 )
        t1 = time.perf_counter()
        logger.info( f"Fink poll finished in {t1-t0} seconds." )

        mdb = getattr( mongoclient, dbname )
        assert collection in mdb.list_collection_names()
        coll = getattr( mdb, collection )

        assert coll.count_documents({}) >= 10
        for doc in coll.find( {} ):
            assert doc['brokername']== 'Fink'
            # Check other stuff?

    finally:
        mdb = getattr( mongoclient_rw, dbname )
        coll = getattr( mdb, collection )
        coll.delete_many( {} )
