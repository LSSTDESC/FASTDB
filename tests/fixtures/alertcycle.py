import pytest
import sys
import os
import pathlib
import random
import time
import datetime
import multiprocessing

from services.projectsim import AlertSender
from services.brokerconsumer import BrokerConsumer
from util import logger
import db

sys.path.insert( 0, pathlib.Path(__file__).parent )
from fakebroker import FakeBroker

# IMPORTANT : note that these are module-scope fixtures.  Write your tests accordingly.
#
# In particular, if you're checking the state of the kafka server, or of the database,
# it will be different based on which fixtures have run.  So, if you have a test
# later in your module that includes an earlier fixture, and expects the database
# to be what it is after just that earlier fixture, it will fail.  Do things
# in order.
#
# (All of this was done for efficiency, because spinning up the alert
# sender, fake broker, and consumer, and sleeping enough for them to all
# do their thing, takes time.)


# This fixture generates a random string of letters that should be used
# for all Kafka topics and group ids.  Because it's not easy to clean
# up the kafka server (remove topics, remove group ids), tests can't
# really clean up after themselves.  Using randomized names for
# topics and group ids should prevent repeated use of fixtures
# from colliding with each other.  It does mean cruft will build
# up on the kafka server, but hopefully the test docker compose
# environment won't be up all that long anyway.
#
# ...this is maybe not a good fixture name given the global pytest
# namespace for fixtures.  It should probably be alertcycle_barf.  Oh
# well, hopefully I don't regret it.  If I need random barf somewhere
# else, I'll need to use a longer fixture name.
@pytest.fixture( scope='module' )
def barf():
    return "".join( random.choices( 'abcdefghijklmnopqrstuvwxyz', k=6 ) )


@pytest.fixture( scope='module' )
def fakebroker( barf ):
    proc = None

    try:
        broker = FakeBroker( "kafka-server:9092", [ f"alerts-{barf}" ],
                             "kafka-server:9092", f"classifications-{barf}",
                             group_id=f"fakebroker-{barf}", notopic_sleeptime=1,
                             reset=False, verbose=False )
        proc = multiprocessing.Process( target=broker )
        logger.info( "Starting fakebroker." )
        proc.start()

        yield True

    finally:
        if proc is not None:
            logger.info( "Terminating fakebroker" )
            proc.terminate()
            proc.join()


@pytest.fixture( scope='module' )
def alerts_30days_sent( snana_fits_ppdb_loaded, barf ):
    sender = AlertSender( 'kafka-server', f"alerts-{barf}" )
    nsent = sender( addeddays=30, reallysend=True )
    assert nsent == 77

    yield datetime.datetime.now( tz=datetime.UTC )

    with db.DB() as con:
        cursor = con.cursor()
        # ... this isn't exactly right.  It's conceptually possible that
        # other fixtures will have added things to this table.  But, at least
        # as of this writing, we know that didn't happen.
        cursor.execute( "DELETE FROM ppdb_alerts_sent" )
        con.commit()


# This one is a bit slow because it has a built in sleep.
@pytest.fixture( scope='module' )
def alerts_30days_sent_and_classified( alerts_30days_sent, fakebroker ):
    # Give the broker a few seconds to wake up from sleep,
    # see the topic, pull the messages, and do its thing.
    logger.info( "Sleeping 10 seconds to give fakebroker time to catch up..." )
    time.sleep( 10 )
    logger.info( "...I hope fakebroker did its stuff!" )

    yield datetime.datetime.now( tz=datetime.UTC )

    # ...I don't think I need to do cleanup here.  The fakebroker
    # will have loaded up a kafka topic, but we can't clean up
    # those anyway (which is why we use random barf).


# This one is slow because it includes the previous one, and has its own sleeps
@pytest.fixture( scope='module' )
def alerts_30days_sent_and_brokermessage_consumed( barf, alerts_30days_sent_and_classified ):
    mongodb_dbname = None
    mongodb_collection = None

    try:
        brokertopic = f'classifications-{barf}'
        mongodb_dbname = os.getenv( 'MONGODB_DBNAME' )
        mongodb_collection = f'fastdb_{barf}'

        bc = BrokerConsumer( 'kafka-server', f'BrokerConsumer-{barf}', topics=brokertopic,
                             mongodb_collection=mongodb_collection, nomsg_sleeptime=1 )
        bc.poll( restart_time=datetime.timedelta(seconds=3), max_restarts=1, notopic_sleeptime=2 )

        yield datetime.datetime.now( tz=datetime.UTC )

    finally:
        # Clear out the mongodb collection that BrokerConsumer will have filled
        if ( mongodb_dbname is not None ) and ( mongodb_collection is not None ):
            with db.MG() as mongoclient:
                brokermessages = getattr( mongoclient, mongodb_dbname )
                if mongodb_collection in brokermessages.list_collection_names():
                    coll = getattr( brokermessages, mongodb_collection )
                    coll.drop()
                assert mongodb_collection not in brokermessages.list_collection_names()


# The purpose of this fixture is to send out more alerts after
#   all of the alerts from the first 30 days have been consumed
#   by a BrokerConsumer
@pytest.fixture( scope='module' )
def alerts_60moredays_sent( snana_fits_ppdb_loaded, alerts_30days_sent_and_brokermessage_consumed, barf ):
    sender = AlertSender( 'kafka-server', f"alerts-{barf}" )
    nsent = sender( addeddays=60, reallysend=True )
    assert nsent == 104

    yield datetime.datetime.now( tz=datetime.UTC )


@pytest.fixture( scope='module' )
def alerts_60moredays_sent_and_brokermessage_consumed( barf, alerts_60moredays_sent ):
    logger.info( "Sleeping 10 seconds to give fakebroker time to catch up..." )
    time.sleep( 10 )
    logger.info( "...I hope fakebroker did its stuff!" )

    try:
        brokertopic = f'classifications-{barf}'
        mongodb_collection = f'fastdb_{barf}'

        # Using the same group_id as the last BrokerConsumer, so it should
        #   pick up messages where the last one left off... if kafka
        #   works as I understand.
        bc = BrokerConsumer( 'kafka-server', f'BrokerConsumer-{barf}', topics=brokertopic,
                             mongodb_collection=mongodb_collection, nomsg_sleeptime=1 )
        bc.poll( restart_time=datetime.timedelta(seconds=3), max_restarts=1, notopic_sleeptime=2 )

        yield datetime.datetime.now( tz=datetime.UTC )

    finally:
        # Don't clear out the mongodb collection, because the
        # alerts_30days_sent_and_brokermessage_consumed fixture
        # (which is required by the alerts_60moredays_sent fixture
        # that this fixtured rquire) will do that cleanup.
        pass
