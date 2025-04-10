import sys
import io
import pytest
import datetime
import random
import logging

import fastavro

from kafka_consumer import KafkaConsumer
import db
import util
from services.projectsim import AlertReconstructor, AlertSender

_logger = logging.getLogger( __file__ )
_logout = logging.StreamHandler( sys.stderr )
_logger.addHandler( _logout )
_formatter = logging.Formatter( '[%(asctime)s - %(levelname)s] - %(message)s',
                                datefmt='%Y-%m-%d %H:%M:%S' )
_logout.setFormatter( _formatter )
_logger.propagate = False
# _logger.setLevel( logging.INFO )
_logger.setLevel( logging.DEBUG )


def test_reconstruct_alert( snana_fits_ppdb_loaded ):
    recon = AlertReconstructor()

    alert = recon.reconstruct( 169694900014 )

    assert alert['alertId'] == 169694900014

    assert alert['diaObject']['diaObjectId'] == 1696949
    assert alert['diaObject']['ra'] == pytest.approx( 210.234375, abs=1e-5 )
    assert alert['diaObject']['dec'] == pytest.approx( 4.031936, abs=1e-5 )
    assert alert['diaSource']['diaSourceId'] == 169694900014
    assert alert['diaSource']['diaObjectId'] == alert['diaObject']['diaObjectId']
    assert alert['diaSource']['ra'] == pytest.approx( alert['diaObject']['ra'], abs=1e-5 )
    assert alert['diaSource']['dec'] == pytest.approx( alert['diaObject']['dec'], abs=1e-5 )
    assert alert['diaSource']['midpointMjdTai'] == pytest.approx( 60371.3728, abs=0.0001 )
    assert alert['diaSource']['psfFlux'] == pytest.approx( 18833.877, abs=0.1 )
    assert alert['diaSource']['psfFluxErr'] == pytest.approx( 1107.8115, abs=0.1 )
    assert alert['diaSource']['band'] == 'Y'
    assert alert['diaSource']['snr'] == pytest.approx( alert['diaSource']['psfFlux']
                                                       / alert['diaSource']['psfFluxErr'], rel=1e-3 )
    assert len( alert['prvDiaSources'] ) == 12
    assert len( alert['prvDiaForcedSources'] ) == 7

    assert all( a['midpointMjdTai'] < alert['diaSource']['midpointMjdTai'] for a in alert['prvDiaSources'] )
    assert all( a['midpointMjdTai'] >= alert['diaSource']['midpointMjdTai'] -365 for a in alert['prvDiaSources'] )
    assert all( a['midpointMjdTai'] < alert['diaSource']['midpointMjdTai'] -1 for a in alert['prvDiaForcedSources'] )
    assert all( a['midpointMjdTai'] >= alert['diaSource']['midpointMjdTai'] -365 for a in alert['prvDiaForcedSources'] )

    # Try reconstructing with a different lookback time

    recon = AlertReconstructor( prevsrc=10, prevfrced=17, prevfrced_gap=10 )
    alert = recon.reconstruct( 169694900014 )

    assert len( alert['prvDiaSources'] ) == 9
    assert len( alert['prvDiaForcedSources'] ) == 4

    assert all( a['midpointMjdTai'] < alert['diaSource']['midpointMjdTai'] for a in alert['prvDiaSources'] )
    assert all( a['midpointMjdTai'] >= alert['diaSource']['midpointMjdTai'] -10 for a in alert['prvDiaSources'] )
    assert all( a['midpointMjdTai'] < alert['diaSource']['midpointMjdTai'] -10 for a in alert['prvDiaForcedSources'] )
    assert all( a['midpointMjdTai'] >= alert['diaSource']['midpointMjdTai'] -17 for a in alert['prvDiaForcedSources'] )


def test_alertsender_find_alerts( snana_fits_ppdb_loaded ):
    try:
        sender = AlertSender( 'kafka-server', 'null' )

        # First: no alerts sent, addeday = 30, should get first 30 days of alert
        sourceids = sender.find_alerts_to_send( addeddays=30 )
        with db.DB() as con:
            cursor = con.cursor()
            cursor.execute( "SELECT MIN(midpointmjdtai) FROM ppdb_diasource" )
            minmjd = cursor.fetchone()[0]
            cursor.execute( "SELECT midpointmjdtai FROM ppdb_diasource WHERE diasourceid=ANY(%(ids)s)",
                            { 'ids': sourceids } )
            sourcemjds = [ row[0] for row in cursor.fetchall() ]
            assert len( sourcemjds ) == len( sourceids )
            assert len( sourcemjds ) == 77
            assert all ( s >= minmjd for s in sourcemjds )
            assert all ( s <= minmjd + 30 for s in sourcemjds )
            # Turns out that the max of the 77 that are in the test test has mjd 60303.211,
            #   and the first has mjd 60278.029
            assert max( sourcemjds ) - ( minmjd + 30 ) < 5

        # Second: no alert sent, throughday = 60288
        sourceids = sender.find_alerts_to_send( throughday=60288 )
        with db.DB() as con:
            cursor = con.cursor()
            cursor.execute( ( "SELECT diasourceid,midpointmjdtai "
                              "FROM ppdb_diasource "
                              "WHERE diasourceid=ANY(%(ids)s) "
                              "ORDER BY midpointmjdtai" ), { 'ids': sourceids } )
            rows = cursor.fetchall()
            early_sourceids = [ row[0] for row in rows ]
            sourcemjds = [ row[1] for row in rows ]
            maxmjd = max( sourcemjds )
            assert len( sourcemjds ) == len( sourceids )
            assert set( early_sourceids ) == set( sourceids )
            assert len( sourcemjds ) == 38
            assert all( s >= minmjd for s in sourcemjds )
            assert all( s <= 60288 for s in sourcemjds )

        # Third: set the second half of the source ids we got from the last query
        #   as having their alerts sent.  Then ask for alert to send with
        #   addeday +1.  Make sure we get the first half of sources, plus the
        #   handful of sources that are 60288<mjd≤60289
        with db.DB() as con:
            cursor = con.cursor()
            now = datetime.datetime.now( tz=datetime.UTC )
            for sid in early_sourceids[19:]:
                cursor.execute( "INSERT INTO ppdb_alerts_sent(diasourceid,senttime) VALUES(%(id)s,%(t)s)",
                                { 'id': sid, 't': now } )
            con.commit()

        sourceids = sender.find_alerts_to_send( addeddays=1 )
        with db.DB() as con:
            cursor = con.cursor()
            cursor.execute( ( "SELECT diasourceid,midpointmjdtai "
                              "FROM ppdb_diasource "
                              "WHERE diasourceid=ANY(%(ids)s) "
                              "ORDER BY midpointmjdtai" ), { 'ids': sourceids } )
            rows = cursor.fetchall()
            new_sourceids = [ row[0] for row in rows ]
            sourcemjds = [ row[1] for row in rows ]
            assert len( new_sourceids ) == len( sourceids )
            assert set( new_sourceids) == set( sourceids )
            assert all( s < maxmjd + 1 for s in sourcemjds )
            assert all( s in new_sourceids for s in early_sourceids[:19] )
            assert all( s not in new_sourceids for s in early_sourceids[19:] )
            assert len( new_sourceids ) == 19 + 2

    finally:
        with db.DB() as con:
            cursor = con.cursor()
            cursor.execute( "DELETE FROM ppdb_alerts_sent" )
            con.commit()



def test_send_alerts( snana_fits_ppdb_loaded ):
    try:
        schema = util.get_alert_schema()

        # Because there's no easy way to tell the kafka server to delete a topic, we can't really
        #   clean up after this test.  So, instead, we're going to sort of simulate it by using
        #   random kafka topics.  That way, if you rerun the test, the kafka topic will start empty.
        #   Over time, the test kafka server will build up cruft, but hopefully the test docker
        #   compose environment will never live _that_ long.
        barf = "".join( random.choices( 'abcdefghijklmnopqrstuvwxyz', k=6 ) )
        topic = f"test_send_alerts_{barf}"
        sender = AlertSender( 'kafka-server', topic )

        # Send 30 days worth of alerts... only don't really send
        nsent = sender( addeddays=30 )
        assert nsent == 0
        # The topic should not even exist:
        consumer = KafkaConsumer( 'kafka-server', f'test_send_alerts_{barf}', schema['alert_schema_file'],
                                  consume_nmsgs=10, logger=_logger )
        assert topic not in consumer.topic_list()

        # Now really send
        _logger.info( f"test_send_alert sending to kafka topic {topic}" )
        nsent = sender( addeddays=30, reallysend=True )
        assert nsent == 77
        # The topic should now exist
        assert topic in consumer.topic_list()

        # There should be 77 messages on the topic
        consumer.subscribe( [ topic ], reset=True )
        msgs = []
        consumer.poll_loop( handler=lambda m: msgs.extend( m ), stopafternsleeps=2 )
        assert len(msgs) == 77

        # Make sure that the "alerts sent" table matches what's in the messages
        alertids = [ fastavro.schemaless_reader(io.BytesIO(m.value()), schema['alert'])['diaSource']['diaSourceId']
                     for m in msgs ]
        with db.DB() as con:
            cursor = con.cursor()
            cursor.execute( "SELECT diasourceid FROM ppdb_alerts_sent" )
            dbids = [ row[0] for row in cursor.fetchall() ]
        assert set( dbids ) == set( alertids )

    finally:
        with db.DB() as con:
            cursor = con.cursor()
            cursor.execute( "DELETE FROM ppdb_alerts_sent" )
            con.commit()


# The purpose of this next test is to see timings when sending
#   lots of alerts.  To *really* test this, we need a bigger
#   test set....
def test_send_all_alerts( snana_fits_ppdb_loaded ):
    try:
        schema = util.get_alert_schema()

        # See comment in test_send_aelrts
        barf = "".join( random.choices( 'abcdefghijklmnopqrstuvwxyz', k=6 ) )
        topic = f"test_send_alerts_{barf}"

        consumer = KafkaConsumer( 'kafka-server', f'test_send_all_alerts_{barf}', schema['alert_schema_file'],
                                  consume_nmsgs=100, logger=_logger )
        assert topic not in consumer.topic_list()

        sender = AlertSender( 'kafka-server', topic )
        nsent = sender( throughday=70000, reallysend=True )
        assert nsent == 1862

        assert topic in consumer.topic_list()
        consumer.subscribe( [ topic ], reset=True )
        msgs = []
        consumer.poll_loop( handler=lambda m: msgs.extend( m ), stopafternsleeps=2 )
        assert len(msgs) == 1862

        # Make sure all diasourceids show up
        alertids = set( fastavro.schemaless_reader(io.BytesIO(m.value()), schema['alert'])['diaSource']['diaSourceId']
                        for m in msgs )
        with db.DB() as con:
            cursor = con.cursor()
            cursor.execute( "SELECT diasourceid FROM ppdb_diasource" )
            dbids = set( row[0] for row in cursor.fetchall() )
        assert dbids == alertids

        # Make sure all alerts show up in the alerts_sent table
        with db.DB() as con:
            cursor = con.cursor()
            cursor.execute( "SELECT s.diasourceid FROM ppdb_diasource s "
                            "LEFT JOIN ppdb_alerts_sent a ON s.diasourceid=a.diasourceid "
                            "WHERE a.diasourceid IS NULL" )
            assert len( cursor.fetchall() ) == 0

    finally:
        with db.DB() as con:
            cursor = con.cursor()
            cursor.execute( "DELETE FROM ppdb_alerts_sent" )
            con.commit()
