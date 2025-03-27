import os
import time
import datetime
import multiprocessing

from services.brokerconsumer import BrokerConsumer, BrokerConsumerLauncher
from util import logger
import db


def check_mongodb( mongoclient, dbname, collection ):
    brokermessages = getattr( mongoclient, dbname )

    assert collection in brokermessages.list_collection_names()

    coll = getattr( brokermessages, collection )

    # 77 diasources in the database, two classifiers per alert = 154 broker messages
    assert coll.count_documents({}) == 154

    # Pull out the diaSourceId from all the messages, make sure they're as expected
    # (Based on the Postgres ppdb_alerts_sent table.)
    mgcursor = coll.find( {}, projection={ 'msg.diaSource.diaSourceId': 1 } )
    srcids = [ c['msg']['diaSource']['diaSourceId'] for c in mgcursor ]
    assert len(srcids) == 154
    srcids = set( srcids )
    assert len(srcids) == 77
    with db.DB() as conn:
        cursor = conn.cursor()
        cursor.execute( "SELECT diasourceid FROM ppdb_alerts_sent" )
        alertssent = set( row[0] for row in cursor.fetchall() )
    assert alertssent == srcids

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
                             mongodb_collection=collection, nomsg_sleeptime=1 )
        bc.poll( restart_time=datetime.timedelta(seconds=3), max_restarts=2, notopic_sleeptime=2 )
        assert time.perf_counter() - t0 < 10

        # Now make sure it can really poll
        t0 = time.perf_counter()
        bc = BrokerConsumer( 'kafka-server', f'test_BrokerConsumer_{barf}-1', topics=brokertopic,
                             mongodb_collection=collection, nomsg_sleeptime=1 )
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
