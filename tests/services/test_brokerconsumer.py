import os
import random
import datetime

from services.brokerconsumer import BrokerConsumer # , BrokerConsumerLauncher
import db


def test_BrokerConsumer( alerts_30days_sent_and_classified_factory, mongoclient, mongoclient_rw ):
    barf = "".join( random.choices( 'abcdefghijklmnopqrstuvwxyz', k=6 ) )
    brokertopic = f'classifications-{barf}'
    dbname = os.getenv( 'MONGODB_DBNAME' )
    assert dbname is not None
    collection = f'fastdb_{barf}'

    _sender, _broker = alerts_30days_sent_and_classified_factory( barf )

    try:
        bc = BrokerConsumer( 'kafka-server', f'test_BrokerConsumer_{barf}', topics=brokertopic,
                             mongodb_collection=collection )
        bc.poll( restart_time=datetime.timedelta(seconds=10), max_restarts=0 )

        # Check that the mongo database got populated
        brokermessages = getattr( mongoclient, dbname )

        assert collection in brokermessages.list_collection_names()

        coll = getattr( brokermessages, collection )

        # 77 diasources in the database, two classifiers per alert = 154 broker messages
        assert coll.count_documents({}) == 154

        # Pull out the diaSourceId from all the messages, make sure they're as expected
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

    finally:
        brokermessages = getattr( mongoclient_rw, dbname )
        if collection in brokermessages.list_collection_names():
            coll = getattr( brokermessages, collection )
            coll.drop()
        assert collection not in brokermessages.list_collection_names()
