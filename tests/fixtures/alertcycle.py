import pytest
import sys
import pathlib
import time
import multiprocessing

from services.projectsim import AlertSender
from util import logger
import db

sys.path.insert( 0, pathlib.Path(__file__).parent )
from fakebroker import FakeBroker


# This is a factory fixture.  Call
# the returned object as a function
# to spin up a fake broker.
@pytest.fixture
def fakebroker_factory():
    storage = {}

    def run_fakebroker( topic_barf, group_id='fakebroker' ):
        broker = FakeBroker( "kafka-server:9092", [ f"alerts-{topic_barf}" ],
                             "kafka-server:9092", f"classifications-{topic_barf}",
                             group_id=group_id, notopic_sleeptime=1,
                             reset=False, verbose=True )
        storage['proc'] = multiprocessing.Process( target=broker, daemon=True )
        storage['proc'].start()

    yield run_fakebroker

    storage['proc'].terminate()
    storage['proc'].join()


# Another factory
@pytest.fixture
def alerts_30days_sent_factory( snana_fits_ppdb_loaded ):
    def send_alerts( topic_barf ):
        sender = AlertSender( 'kafka-server', f"alerts-{topic_barf}" )
        nsent = sender( addeddays=30, reallysend=True )
        assert nsent == 77

    yield send_alerts

    with db.DB() as con:
        cursor = con.cursor()
        # ... this isn't exactly right.  It's conceptually possible that
        # other fixtures will have added things to this table.  But, at least
        # as of this writing, we know that didn't happen.
        cursor.execute( "DELETE FROM ppdb_alerts_sent" )
        con.commit()


# Another factory
# This one is a bit slow because it has a built in sleep.
@pytest.fixture
def alerts_30days_sent_and_classified_factory( alerts_30days_sent_factory, fakebroker_factory ):
    def send_and_classify_alerts( topic_barf, group_id='fakebroker' ):
        # Passing a group_id with a randomized string on it is strongly recommended.  Otherwise,
        #   there are sometimes failures get failures.  By using a group_id that has been used in
        #   the past, that group_id has subscriptions already on the kafka server.  Because topics
        #   are randomized, I wouldn't think this would matter, but evidently it does?  It seems
        #   that if there are existing subscriptions, changing them increses the latency... or
        #   something.

        # If I start the broker first, it sometimes fails.  The broker initially doesn't see the
        #   topic, because it starts before the topic is created by sender.  Then the broker sees
        #   the topic, but before the sender has fully flushed.  But the broker never gets messages
        #   from the topic.  Why?  It should work.  Latency?  Race condition about stored topic
        #   offsets?  ... I bet it's that.  I need to think about my resetting, perhaps.  Not a big
        #   deal for the fakebroker, but for the brokerconsumer.py I should worry about it.

        # Kafka is a bit mysterious.

        # Send the alerts
        sender = alerts_30days_sent_factory( topic_barf )
        # Start the fake broker
        broker = fakebroker_factory( topic_barf, group_id=group_id )

        # Give the broker a few seconds to wake up from sleep,
        # see the topic, pull the messages, and do its thing.
        logger.info( "Sleeping 10 seconds to give fakebroker time to catch up..." )
        time.sleep( 10 )
        logger.info( "...I hope fakebroker did its stuff!" )

        # Going to just assume they're there.  See
        # tests/test_fakebroker.py for a test that includes this fixture
        # and makes sure it worked right.
        return sender, broker

    return send_and_classify_alerts
