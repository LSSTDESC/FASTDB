import pytest
import datetime

import db
from services.source_importer import SourceImporter


@pytest.fixture
def import_first30days_objects( barf, alerts_30days_sent_and_brokermessage_consumed, procver ):
    collection_name = f'fastdb_{barf}'

    try:
        si = SourceImporter( procver.id )
        with db.MG() as mongoclient:
            collection = db.get_mongo_collection( mongoclient, collection_name )
            si.import_objects_from_collection( collection )

        yield True
    finally:
        with db.DB() as conn:
            cursor = conn.cursor()
            # We can be cavalier here becasue diaobject was supposed to be empty when we started
            cursor.execute( "DELETE FROM diaobject" )
            conn.commit()


@pytest.fixture
def import_first30days_sources( barf, import_first30days_objects, procver ):
    collection_name = f'fastdb_{barf}'

    try:
        si = SourceImporter( procver.id )
        with db.MG() as mongoclient:
            collection = db.get_mongo_collection( mongoclient, collection_name )
            si.import_sources_from_collection( collection )

        yield True
    finally:
        with db.DB() as conn:
            cursor = conn.cursor()
            cursor.execute( "DELETE FROM diasource" )
            conn.commit()


@pytest.fixture
def import_30days_prvsources( barf, import_first30days_sources, procver ):
    collection_name = f'fastdb_{barf}'

    try:
        si = SourceImporter( procver.id )
        with db.MG() as mongoclient:
            collection = db.get_mongo_collection( mongoclient, collection_name )
            si.import_prvsources_from_collection( collection )

        yield True
    finally:
        # Don't have to clean up; import_first30days_sources will clean up the diasources table
        pass


@pytest.fixture
def import_30days_prvforcedsources( barf, import_first30days_sources, procver ):
    collection_name = f'fastdb_{barf}'

    try:
        si = SourceImporter( procver.id )
        with db.MG() as mongoclient:
            collection = db.get_mongo_collection( mongoclient, collection_name )
            si.import_prvforcedsources_from_collection( collection )

        yield True
    finally:
        with db.DB() as conn:
            cursor = conn.cursor()
            cursor.execute( "DELETE FROM diaforcedsource" )
            conn.commit()


def test_read_mongo_objects( barf, alerts_30days_sent_and_brokermessage_consumed, procver ):
    collection_name = f'fastdb_{barf}'

    si = SourceImporter( procver.id )
    with db.MG() as mongoclient:
        collection = db.get_mongo_collection( mongoclient, collection_name )

        # First: make sure it finds everyting with no time cut
        with db.DB() as pqconn:
            si.read_mongo_objects( pqconn, collection )
            cursor = pqconn.cursor()
            cursor.execute( "SELECT * FROM temp_diaobject_import" )
            rows = cursor.fetchall()
        assert len(rows) == 12

        # Second: make sure it finds everything with a top time cut of now
        #   (which is assuredly after when things were inserted)
        with db.DB() as pqconn:
            si.read_mongo_objects( pqconn, collection, t1=datetime.datetime.now( tz=datetime.UTC ) )
            cursor = pqconn.cursor()
            cursor.execute( "SELECT * FROM temp_diaobject_import" )
            rows = cursor.fetchall()
        assert len(rows) == 12

        # Third: make sure it finds nothing with a bottom time cut of now
        with db.DB() as pqconn:
            si.read_mongo_objects( pqconn, collection, t0=datetime.datetime.now( tz=datetime.UTC ) )
            cursor = pqconn.cursor()
            cursor.execute( "SELECT * FROM temp_diaobject_import" )
            rows = cursor.fetchall()
        assert len(rows) == 0

        # Testing between times is hard, because I belive all of the things
        # saved will have the same time cut!  So, resort to just giving
        # a ridiculously early t0 and make sure we get everything
        # Third: make sure it finds nothing with a bottom time cut of now
        with db.DB() as pqconn:
            si.read_mongo_objects( pqconn, collection,
                                   t0=datetime.datetime( 2000, 1, 1, 0, 0, 0, tzinfo=datetime.UTC ),
                                   t1=datetime.datetime.now( tz=datetime.UTC ) )
            cursor = pqconn.cursor()
            cursor.execute( "SELECT * FROM temp_diaobject_import" )
            rows = cursor.fetchall()
        assert len(rows) == 12


    # TODO : look at other fields?


def test_read_mongo_sources( barf, alerts_30days_sent_and_brokermessage_consumed, procver ):
    collection_name = f'fastdb_{barf}'

    # Not going to test time cuts here because it's the same code path that
    #   was already tested intest_read_mongo_objects

    si = SourceImporter( procver.id )
    with db.MG() as mongoclient:
        collection = db.get_mongo_collection( mongoclient, collection_name )
        with db.DB() as pqconn:
            si.read_mongo_sources( pqconn, collection )
            cursor = pqconn.cursor()
            cursor.execute( "SELECT * FROM temp_diasource_import" )
            rows = cursor.fetchall()

    assert len(rows) == 77

    # TODO : more stringent tests


def test_read_mongo_previous_sources( barf, alerts_30days_sent_and_brokermessage_consumed, procver ):
    collection_name = f'fastdb_{barf}'

    si = SourceImporter( procver.id )
    with db.MG() as mongoclient:
        collection = db.get_mongo_collection( mongoclient, collection_name )
        with db.DB() as pqconn:
            si.read_mongo_prvsources( pqconn, collection )
            cursor = pqconn.cursor()
            cursor.execute( "SELECT * FROM temp_prvdiasource_import" )
            coldex = { desc[0]: i for i, desc in enumerate(cursor.description) }
            rows = cursor.fetchall()

        assert len(rows) == 65

        # Check that the mongo aggregation stuff in read_mongo_provsources is
        #   right by doing it long-form in python

        pulledsourceids = set( row[coldex['diasourceid']] for row in rows )
        assert len( pulledsourceids ) == len(rows)
        prvsources = {}

        for src in collection.find( {} ):
            if src['msg']['prvDiaSources'] is not None:
                for prvsrc in src['msg']['prvDiaSources']:
                    if prvsrc['diaSourceId'] not in prvsources:
                        prvsources[ prvsrc['diaSourceId'] ] = prvsrc

        assert set( prvsources.keys() ) == pulledsourceids

    # TODO: check more fields


def test_read_mongo_previous_forced_sources( barf, alerts_30days_sent_and_brokermessage_consumed, procver ):
    collection_name = f'fastdb_{barf}'

    si = SourceImporter( procver.id )
    with db.MG() as mongoclient:
        collection = db.get_mongo_collection( mongoclient, collection_name )
        with db.DB() as pqconn:
            si.read_mongo_prvforcedsources( pqconn, collection )
            cursor = pqconn.cursor()
            cursor.execute( "SELECT * FROM temp_prvdiaforcedsource_import" )
            coldex = { desc[0]: i for i, desc in enumerate(cursor.description) }
            rows = cursor.fetchall()

        assert len(rows) == 148

        # Check that the mongo aggregation stuff in read_mongo_provsources is
        #   right by doing it long-form in python

        pulledsourceids = set( row[coldex['diaforcedsourceid']] for row in rows )
        assert len( pulledsourceids ) == len(rows)
        prvsources = {}

        for src in collection.find( {} ):
            if src['msg']['prvDiaForcedSources'] is not None:
                for prvsrc in src['msg']['prvDiaForcedSources']:
                    if prvsrc['diaForcedSourceId'] not in prvsources:
                        prvsources[ prvsrc['diaForcedSourceId'] ] = prvsrc

        assert set( prvsources.keys() ) == pulledsourceids

    # TODO: check more fields


def test_import_objects( import_first30days_objects ):
    with db.DB() as conn:
        cursor = conn.cursor()
        cursor.execute( "SELECT * FROM diaobject" )
        rows = cursor.fetchall()
    assert len(rows) == 12

    # TODO : look at more?  Compare ppdb_diaobject to diaobject?


def test_import_sources( import_first30days_sources ):
    with db.DB() as conn:
        cursor = conn.cursor()
        cursor.execute( "SELECT * FROM diasource" )
        rows = cursor.fetchall()
    assert len(rows) == 77

    # TODO :more


def test_import_prvsources( import_30days_prvsources ):
    with db.DB() as conn:
        cursor = conn.cursor()
        cursor.execute( "SELECT * FROM diasource" )
        rows = cursor.fetchall()
    # There won't be any new sources, because all sources that might
    #   have been a previous already got imported by
    #   import_sources.
    #
    # TODO: run a test where some of the previous sources weren't
    #   already there.  (This means different fixture setup,
    #   e.g. starting 30 days in and going 30 days forward.)
    assert len(rows) == 77

    # TODO : More


def test_import_provforcedsources( import_30days_prvforcedsources ):
    with db.DB() as conn:
        cursor = conn.cursor()
        cursor.execute( "SELECT * FROM diaforcedsource" )
        rows = cursor.fetchall()
    assert len(rows) == 148

    # TODO : More
