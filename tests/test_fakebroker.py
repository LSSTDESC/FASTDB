import io
import random

import fastavro

import util
import db
from kafka_consumer import KafkaConsumer


# testception.  This is a test of something that lives in tests and
# exists for other tests.  But, we do want to make sure it works.  This
# also tests the alerts_30days_sent_and_classified_factory fixture.  (Of
# course, all of these things are tested in other places, but if you
# want to isloate down to just testing fakebroker and that fixture, this
# test is here.)
def test_fakebroker( barf, snana_fits_ppdb_loaded, alerts_30days_sent_and_classified ):
    schema = util.get_alert_schema()
    brokertopic = f'classifications-{barf}'
    private_barf = "".join( random.choices( 'abcdefghijklmnopqrstuvwzyx', k=6 ) )

    # See if the fakebroker's messages are on the server
    consumer = KafkaConsumer( 'kafka-server', f'test_fakebroker_{private_barf}', schema['brokermessage_schema_file'],
                              consume_nmsgs=20, logger=util.logger )
    assert brokertopic in consumer.topic_list()
    consumer.subscribe( [ brokertopic ], reset=True )
    msgs = []
    consumer.poll_loop( handler=lambda m: msgs.extend( m ), stopafternsleeps=2 )
    # Should be 77×2 messages, as there are two classifiers in the fake broker
    assert len(msgs) == 154

    # Make sure the broker messages can be parsed with the right schema, and that
    #  they cover the right source ids
    brokeralerts = [ fastavro.schemaless_reader( io.BytesIO(m.value()), schema['brokermessage'] )
                     for m in msgs ]
    with db.DB() as con:
        cursor = con.cursor()
        cursor.execute( "SELECT s.diasourceid "
                        "FROM ppdb_diasource s "
                        "INNER JOIN ppdb_alerts_sent a ON s.diasourceid=a.diasourceid" )
        dbids = [ row[0] for row in cursor.fetchall() ]
    assert set( a['diaSource']['diaSourceId'] for a in brokeralerts ) == set( dbids )
