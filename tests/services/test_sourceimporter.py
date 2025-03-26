import datetime

import db
from services.source_importer import SourceImporter


def test_read_mongo_objects( barf, alerts_30days_sent_and_brokermessage_consumed, procver ):
    collection_name = f'fastdb_{barf}'

    si = SourceImporter( procver.id )
    with db.MG() as mongoclient:
        collection = db.get_mongo_collection( mongoclient, collection_name )
        with db.DB() as pqconn:
            si.read_mongo_objects( pqconn, collection, datetime.datetime.now( tz=datetime.UTC ) )

            cursor = pqconn.cursor()
            cursor.execute( "SELECT * FROM temp_diaobject_import" )
            rows = cursor.fetchall()

    assert len(rows) == 12

    # TODO : look at other fields?


def test_read_mongo_sources( barf, alerts_30days_sent_and_brokermessage_consumed, procver ):
    collection_name = f'fastdb_{barf}'

    si = SourceImporter( procver.id )
    with db.MG() as mongoclient:
        collection = db.get_mongo_collection( mongoclient, collection_name )
        with db.DB() as pqconn:
            si.read_mongo_sources( pqconn, collection, datetime.datetime.now( tz=datetime.UTC ) )

            cursor = pqconn.cursor()
            cursor.execute( "SELECT * FROM temp_diasource_import" )
            rows = cursor.fetchall()

    assert len(rows) == 77

    # TODO : more stringent tests
