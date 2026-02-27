import pytest
import os
import datetime
import time
import random
from psycopg import sql

import db
from util import env_as_bool, FDBLogger
from services.source_importer import SourceImporter
from services.brokerconsumer import FinkConsumer

# Ordering of these tests matters, because they use module scope
# fixtures from tests/fixtures/alertcycle.py (the "alerts*" fixtures).
# Make sure that anything that tests assuming only 30 days is done
# runs before any test that includes a 60 or 90 day fixture.
# (The ordering is very fiddly.)

# NOTE : there is currently no test that checks when object_processing_version
#   and processing_version are different


# ======================================================================
# Specific broker tests are at the beginning because if we run them, we want to run them
#   before the module-level fixtures that load up the database are run.

@pytest.mark.skipif( not env_as_bool('RUN_FINK_TESTS'), reason='RUN_FINK_TESTS is not set' )
def test_fink( mongoclient_rw, procver_collection ):
    mongoclient = mongoclient_rw
    bpv, _pv = procver_collection
    barf = "".join( random.choices( 'abcdefghijklmnopqrstuvwxyz', k=6 ) )
    brokertopic = 'fink_sn_near_galaxy_candidate_lsst'
    dbname = os.getenv( 'MONGODB_DBNAME' )
    assert dbname is not None
    collection_name = f'fastdb_{barf}'

    try:
        t0 = time.perf_counter()
        fc = FinkConsumer( grouptag=barf, fink_topic=brokertopic, mongodb_collection=collection_name,
                           nomsg_sleeptime=1, batch_size=10 )
        fc.poll( restart_time=datetime.timedelta(seconds=10), max_restarts=0, notopic_sleeptime=2, max_msgs=10 )
        t1 = time.perf_counter()
        FDBLogger.info( f"Fink poll finished in {t1-t0} seconds." )

        collection = db.get_mongo_collection( mongoclient, collection_name )
        si = SourceImporter( bpv['realtime'].id,
                             bpv['realtime_diaobject_position_60000'].id,
                             bpv['realtime_diasource'].id,
                             bpv['realtime_diaforcedsource'].id,
                             None )
        si.import_from_mongo( collection )

        allmongo = list( collection.find( {} ) )

        with db.DBCon( dictcursor=True ) as pqcon:
            # allobjects = pqcon.execute( "SELECT * FROM diaobject" )
            allsources = pqcon.execute( "SELECT * FROM diasource" )
            allbrokerinfo = pqcon.execute( "SELECT * FROM diasource_brokerinfo" )

        assert len( allsources ) >= 10
        assert len( allbrokerinfo ) == len( allmongo )
        # import pdb; pdb.set_trace()
        # pass
        # MORE

    finally:
        with db.DBCon() as pqcon:
            pqcon.execute( "DELETE FROM diaforcedsource_extra" )
            pqcon.execute( "DELETE FROM diaforcedsource" )
            pqcon.execute( "DELETE FROM diasource_brokerinfo" )
            pqcon.execute( "DELETE FROM diasource_extra" )
            pqcon.execute( "DELETE FROM diasource" )
            pqcon.execute( "DELETE FROM diaobject_position" )
            pqcon.execute( "DELETE FROM diaobject" )
            pqcon.execute( "DELETE FROM root_diaobject" )
            pqcon.execute( "DELETE FROM diasource_import_time" )
            pqcon.commit()

        collection = db.get_mongo_collection( mongoclient, collection_name )
        collection.delete_many( {} )
        collection = db.get_mongo_collection( mongoclient, "source_thumbnails" )
        collection.delete_many( {} )


# **********************************************************************

@pytest.fixture
def import_first30days_objects( barf, alerts_30days_sent_and_brokermessage_consumed, procver_collection ):
    bpv, _pv = procver_collection
    collection_name = f'fastdb_{barf}'
    t1 = alerts_30days_sent_and_brokermessage_consumed

    try:
        si = SourceImporter( bpv['realtime'].id,
                             bpv['realtime_diaobject_position_60000'].id,
                             bpv['realtime_diasource'].id,
                             bpv['realtime_diaforcedsource'].id,
                             None )
        with db.MG() as mongoclient:
            collection = db.get_mongo_collection( mongoclient, collection_name )
            nobjs, nroot, npos = si.import_objects_from_collection( collection, t1=t1 )

        yield nobjs, nroot, npos
    finally:
        with db.DBCon() as conn:
            conn.execute( "DELETE FROM diaobject_position" )
            conn.execute( "DELETE FROM diaobject" )
            conn.execute( "DELETE FROM root_diaobject" )
            conn.commit()


@pytest.fixture
def import_first30days_sources( barf, import_first30days_objects, procver_collection,
                                alerts_30days_sent_and_brokermessage_consumed ):
    bpv, _pv = procver_collection
    collection_name = f'fastdb_{barf}'
    t1 = alerts_30days_sent_and_brokermessage_consumed

    try:
        si = SourceImporter( bpv['realtime'].id,
                             bpv['realtime_diaobject_position_60000'].id,
                             bpv['realtime_diasource'].id,
                             bpv['realtime_diaforcedsource'].id,
                             None )
        with db.MG() as mongoclient:
            collection = db.get_mongo_collection( mongoclient, collection_name )
            nsrc, nprvsrc = si.import_sources_from_collection( collection, t1=t1 )
            ninfo = si.import_brokerinfo_from_collection( collection, t1=t1 )
            si.import_cutouts_from_collection( collection, t1=t1 )

        yield nsrc, nprvsrc, ninfo
    finally:
        with db.DBCon() as conn:
            conn.execute( "DELETE FROM diasource_brokerinfo" )
            conn.execute( "DELETE FROM diasource_extra" )
            conn.execute( "DELETE FROM diasource" )
            conn.commit()
        with db.MG() as mongoclient:
            collection = db.get_mongo_collection( mongoclient, "source_thumbnails" )
            collection.delete_many( {} )


@pytest.fixture
def import_30days_prvforcedsources( barf, import_first30days_sources, procver_collection,
                                    alerts_30days_sent_and_brokermessage_consumed ):
    bpv, _pv = procver_collection
    collection_name = f'fastdb_{barf}'
    t1 = alerts_30days_sent_and_brokermessage_consumed

    try:
        si = SourceImporter( bpv['realtime'].id,
                             bpv['realtime_diaobject_position_60000'].id,
                             bpv['realtime_diasource'].id,
                             bpv['realtime_diaforcedsource'].id,
                             None )
        with db.MG() as mongoclient:
            collection = db.get_mongo_collection( mongoclient, collection_name )
            n = si.import_forcedsources_from_collection( collection, t1=t1 )

        yield n
    finally:
        with db.DBCon() as conn:
            conn.execute( "DELETE FROM diaforcedsource_extra" )
            conn.execute( "DELETE FROM diaforcedsource" )
            conn.commit()


# This next one is messy because it should *only* be used in:
#   * test_import_30days
#   * import_30days_60days
#   * test_import_30days_50days
#
# Reason: the import_30days_60days fixture will do a cleanup that is going to destroy
#   everything that's set up in this fixture, even though this is a module fixture!
#   It's here because we want to be able to test the time stamps in import_30days_60days,
#   after running test_30days, so this fixture has to be a module fixture so its results
#   will persist for those two tests.  It's kidna like making a mini-scope.
@pytest.fixture( scope='module' )
def messy_import_30days( barf, procver_collection, alerts_30days_sent_and_brokermessage_consumed ):
    bpv, _pv = procver_collection
    collection_name = f'fastdb_{barf}'

    try:
        si = SourceImporter( bpv['realtime'].id,
                             bpv['realtime_diaobject_position_60000'].id,
                             bpv['realtime_diasource'].id,
                             bpv['realtime_diaforcedsource'].id,
                             None )
        with db.MG() as mongoclient:
            collection = db.get_mongo_collection( mongoclient, collection_name )
            nobj, nroot, npos, nsrc, nprvsrc, nfrc, ninfo = si.import_from_mongo( collection )

        yield nobj, nroot, npos, nsrc, nprvsrc, nfrc, ninfo

    finally:
        # Put this cleanup here just in case things died before we got to the
        #   import_30days_60days fixture that does the same cleanup.
        with db.DBCon() as conn:
            conn.execute( "DELETE FROM diaforcedsource_extra" )
            conn.execute( "DELETE FROM diaforcedsource" )
            conn.execute( "DELETE FROM diasource_brokerinfo" )
            conn.execute( "DELETE FROM diasource_extra" )
            conn.execute( "DELETE FROM diasource" )
            conn.execute( "DELETE FROM diaobject_position" )
            conn.execute( "DELETE FROM diaobject" )
            conn.execute( "DELETE FROM root_diaobject" )
            conn.execute( "DELETE FROM diasource_import_time" )
            conn.commit()
        with db.MG() as mg:
            col = db.get_mongo_collection( mg, 'source_thumbnails' )
            col.delete_many({})


# Import days 30-90 after importing days 0-30, and update the diasource_import_time table
# Fixture yields the numbers from the import of days 30-90 (also include fixture
#   import_30days if you want those counts too).
@pytest.fixture
def import_30days_60days( barf, procver_collection, messy_import_30days,
                           alerts_30days_sent_and_brokermessage_consumed,
                           alerts_60moredays_sent_and_brokermessage_consumed ):
    bpv, _pv = procver_collection
    collection_name = f'fastdb_{barf}'
    t0 = alerts_30days_sent_and_brokermessage_consumed
    t1 = alerts_60moredays_sent_and_brokermessage_consumed
    assert t1 > t0

    try:
        with db.DBCon() as pqconn:
            timp30 = pqconn.execute( "SELECT t FROM diasource_import_time WHERE collection=%(col)s",
                                     { "col": collection_name } )[0][0][0]
        assert timp30 > t0
        # This next line, I think, ensures that the tests are using the fixtures in the
        #   fiddly right order
        assert timp30 < t1

        si = SourceImporter( bpv['realtime'].id,
                             bpv['realtime_diaobject_position_60000'].id,
                             bpv['realtime_diasource'].id,
                             bpv['realtime_diaforcedsource'].id,
                             None )
        with db.MG() as mongoclient:
            collection = db.get_mongo_collection( mongoclient, collection_name )
            nobj, nroot, npos, nsrc, nprvsrc, nprvfrc, ninfo = si.import_from_mongo( collection )

        with db.DBCon() as pqconn:
            timp90 = pqconn.execute( "SELECT t FROM diasource_import_time WHERE collection=%(col)s",
                                     { "col": collection_name } )[0][0][0]
            assert timp90 > timp30
            assert timp90 > t1

        yield nobj, nroot, npos, nsrc, nprvsrc, nprvfrc, ninfo

    finally:
        with db.DBCon() as conn:
            conn.execute( "DELETE FROM diaforcedsource_extra" )
            conn.execute( "DELETE FROM diaforcedsource" )
            conn.execute( "DELETE FROM diasource_brokerinfo" )
            conn.execute( "DELETE FROM diasource_extra" )
            conn.execute( "DELETE FROM diasource" )
            conn.execute( "DELETE FROM diaobject_position" )
            conn.execute( "DELETE FROM diaobject" )
            conn.execute( "DELETE FROM root_diaobject" )
            conn.execute( "DELETE FROM diasource_import_time" )
            conn.commit()
        with db.MG() as mg:
            col = db.get_mongo_collection( mg, 'source_thumbnails' )
            col.delete_many({})


# Import days 30-90 without importing days 0-30.
# This uses the timestamps returned by some of the other fixtures
@pytest.fixture
def import_only_next60days( barf, procver_collection,
                            alerts_30days_sent_and_brokermessage_consumed,
                            alerts_60moredays_sent_and_brokermessage_consumed
                           ):
    bpv, _pv = procver_collection
    collection_name = f'fastdb_{barf}'
    t0 = alerts_30days_sent_and_brokermessage_consumed
    t1 = alerts_60moredays_sent_and_brokermessage_consumed

    try:
        si = SourceImporter( bpv['realtime'].id,
                             bpv['realtime_diaobject_position_60000'].id,
                             bpv['realtime_diasource'].id,
                             bpv['realtime_diaforcedsource'].id,
                             None )
        with db.MG() as mongoclient:
            collection = db.get_mongo_collection( mongoclient, collection_name )
            nobj, nroot, npos = si.import_objects_from_collection( collection, t0=t0, t1=t1 )
            nsrc, nprvsrc = si.import_sources_from_collection( collection, t0=t0, t1=t1 )
            nfrc = si.import_forcedsources_from_collection( collection, t0=t0, t1=t1 )
            ninfo = si.import_brokerinfo_from_collection( collection, t0=t0, t1=t1 )
            si.import_cutouts_from_collection( collection, t0=t0, t1=t1 )

        yield nobj, nroot, npos, nsrc, nprvsrc, nfrc, ninfo

    finally:
        with db.DBCon() as conn:
            conn.execute( "DELETE FROM diaforcedsource_extra" )
            conn.execute( "DELETE FROM diaforcedsource" )
            conn.execute( "DELETE FROM diasource_brokerinfo" )
            conn.execute( "DELETE FROM diasource_extra" )
            conn.execute( "DELETE FROM diasource" )
            conn.execute( "DELETE FROM diaobject_position" )
            conn.execute( "DELETE FROM diaobject" )
            conn.execute( "DELETE FROM root_diaobject" )
            conn.commit()
        with db.MG() as mongoclient:
            collection = db.get_mongo_collection( mongoclient, "source_thumbnails" )
            collection.delete_many( {} )


# To test the t1 limit, try to import only the first 30 days of alerts (based on the
#   timestamp we get back from the fixture that brokermessageconsumes them)
#   even when all 90 days of alerts have been brokermessageconsumed.
@pytest.fixture
def import_only_30days_after_90days_consumed( barf, procver_collection,
                                              alerts_30days_sent_and_brokermessage_consumed,
                                              alerts_60moredays_sent_and_brokermessage_consumed ):
    bpv, _pv = procver_collection
    collection_name = f'fastdb_{barf}'
    t1 = alerts_30days_sent_and_brokermessage_consumed

    try:
        si = SourceImporter( bpv['realtime'].id,
                             bpv['realtime_diaobject_position_60000'].id,
                             bpv['realtime_diasource'].id,
                             bpv['realtime_diaforcedsource'].id,
                             None )
        with db.MG() as mongoclient:
            collection = db.get_mongo_collection( mongoclient, collection_name )
            nobj, nroot, npos, nsrc, nprvsrc, nfrc, ninfo = si.import_from_mongo( collection, t1=t1 )

        yield nobj, nroot, npos, nsrc, nprvsrc, nfrc, ninfo

    finally:
        with db.DBCon() as conn:
            conn.execute( "DELETE FROM diaforcedsource_extra" )
            conn.execute( "DELETE FROM diaforcedsource" )
            conn.execute( "DELETE FROM diasource_brokerinfo" )
            conn.execute( "DELETE FROM diasource_extra" )
            conn.execute( "DELETE FROM diasource" )
            conn.execute( "DELETE FROM diaobject_position" )
            conn.execute( "DELETE FROM diaobject" )
            conn.execute( "DELETE FROM root_diaobject" )
            conn.execute( "DELETE FROM diasource_import_time" )
            conn.commit()
        with db.MG() as mg:
            col = db.get_mongo_collection( mg, 'source_thumbnails' )
            col.delete_many({})



# **********************************************************************
# Tests on importation of the first 30 days

def test_read_mongo_objects( barf, alerts_30days_sent_and_brokermessage_consumed, procver_collection ):
    bpv, _pv = procver_collection
    collection_name = f'fastdb_{barf}'

    si = SourceImporter( bpv['realtime'].id,
                         bpv['realtime_diaobject_position_60000'].id,
                         bpv['realtime_diasource'].id,
                         bpv['realtime_diaforcedsource'].id,
                         None )
    with db.MG() as mongoclient:
        collection = db.get_mongo_collection( mongoclient, collection_name )

        # First: make sure it finds everyting with no time cut
        with db.DBCon() as conn:
            si.read_mongo_objects( conn, collection )
            rows, _cols = conn.execute( "SELECT * FROM temp_diaobject_import" )
            assert len(rows) == 12

        # Second: make sure it finds everything with a top time cut of now
        #   (which is assuredly after when things were inserted)
        with db.DBCon() as conn:
            si.read_mongo_objects( conn, collection, t1=datetime.datetime.now( tz=datetime.UTC ) )
            rows, _cols = conn.execute( "SELECT * FROM temp_diaobject_import" )
            assert len(rows) == 12

        # Third: make sure it finds nothing with a bottom time cut of now
        with db.DBCon() as conn:
            si.read_mongo_objects( conn, collection, t0=datetime.datetime.now( tz=datetime.UTC ) )
            rows, _cols = conn.execute( "SELECT * FROM temp_diaobject_import" )
            assert len(rows) == 0

        # Testing between times is hard, because I belive all of the things
        # saved will have the same time cut!  So, resort to just giving
        # a ridiculously early t0 and make sure we get everything
        # Third: make sure it finds nothing with a bottom time cut of now
        with db.DBCon() as conn:
            si.read_mongo_objects( conn, collection,
                                   t0=datetime.datetime( 2000, 1, 1, 0, 0, 0, tzinfo=datetime.UTC ),
                                   t1=datetime.datetime.now( tz=datetime.UTC ) )
            rows, _cols = conn.execute( "SELECT * FROM temp_diaobject_import" )
            assert len(rows) == 12

    # TODO : look at other fields?


def test_read_mongo_sources( barf, alerts_30days_sent_and_brokermessage_consumed, procver_collection ):
    bpv, _pv = procver_collection
    collection_name = f'fastdb_{barf}'

    # Not going to test time cuts here because it's the same code path that
    #   was already tested intest_read_mongo_objects

    si = SourceImporter( bpv['realtime'].id,
                         bpv['realtime_diaobject_position_60000'].id,
                         bpv['realtime_diasource'].id,
                         bpv['realtime_diaforcedsource'].id,
                         None )
    with db.MG() as mongoclient:
        collection = db.get_mongo_collection( mongoclient, collection_name )
        with db.DBCon() as conn:
            si.read_mongo_sources( conn, collection )
            rows, _cols = conn.execute( "SELECT * FROM temp_diasource_import" )
            assert len(rows) == 77
            rows, _cols = conn.execute( "SELECT * FROM temp_diasource_extra_import" )
            assert len(rows) == 77

    # TODO : more stringent tests


def test_read_mongo_previous_sources( barf, alerts_30days_sent_and_brokermessage_consumed, procver_collection ):
    bpv, _pv = procver_collection
    collection_name = f'fastdb_{barf}'

    si = SourceImporter( bpv['realtime'].id,
                         bpv['realtime_diaobject_position_60000'].id,
                         bpv['realtime_diasource'].id,
                         bpv['realtime_diaforcedsource'].id,
                         None )
    with db.MG() as mongoclient:
        collection = db.get_mongo_collection( mongoclient, collection_name )
        with db.DBCon() as conn:
            si.read_mongo_prvsources( conn, collection )
            rows, cols = conn.execute( "SELECT * FROM temp_prvdiasource_import" )
            assert len(rows) == 65
            exrows, _excols = conn.execute( "SELECT * FROM temp_prvdiasource_extra_import" )
            assert len(exrows) == 65


        # Check that the mongo aggregation stuff in read_mongo_provsources is
        #   right by doing it long-form in python

        pulledsourceids = set( row[0] for row in rows )
        assert len( pulledsourceids ) == len(rows)
        prvsources = {}

        for src in collection.find( {} ):
            if src['prvdiasources'] is not None:
                for prvsrc in src['prvdiasources']:
                    if prvsrc['diasourceid'] not in prvsources:
                        prvsources[ prvsrc['diasourceid'] ] = prvsrc

        assert set( prvsources.keys() ) == pulledsourceids

    # TODO: check more fields


def test_read_mongo_previous_forced_sources( barf, alerts_30days_sent_and_brokermessage_consumed, procver_collection ):
    bpv, _pv = procver_collection
    collection_name = f'fastdb_{barf}'

    si = SourceImporter( bpv['realtime'].id,
                         bpv['realtime_diaobject_position_60000'].id,
                         bpv['realtime_diasource'].id,
                         bpv['realtime_diaforcedsource'].id,
                         None )
    with db.MG() as mongoclient:
        collection = db.get_mongo_collection( mongoclient, collection_name )
        with db.DBCon() as conn:
            si.read_mongo_prvforcedsources( conn, collection )
            rows, cols = conn.execute( "SELECT * FROM temp_prvdiaforcedsource_import" )
            coldex = { c: i for i, c in enumerate(cols) }
            assert len(rows) == 148
            exrows, _excols = conn.execute( "SELECT * FROM temp_prvdiaforcedsource_extra_import" )
            assert len(exrows) == 148

        # Check that the mongo aggregation stuff in read_mongo_provsources is
        #   right by doing it long-form in python

        pulledsourceids = set( f"{row[coldex['diaobjectid']]}_{row[coldex['visit']]}" for row in rows )
        assert len( pulledsourceids ) == len(rows)
        prvsources = {}

        for src in collection.find( {} ):
            if src['prvdiaforcedsources'] is not None:
                for prvsrc in src['prvdiaforcedsources']:
                    prvsrcid = f"{prvsrc['diaobjectid']}_{prvsrc['visit']}"
                    if prvsrcid not in prvsources:
                        prvsources[ prvsrcid ] = prvsrc

        assert set( prvsources.keys() ) == pulledsourceids

    # TODO: check more fields


def test_read_mongo_brokerinfo( barf, alerts_30days_sent_and_brokermessage_consumed, procver_collection ):
    bpv, _pv = procver_collection
    collection_name = f"fastdb_{barf}"

    si = SourceImporter( bpv['realtime'].id,
                         bpv['realtime_diaobject_position_60000'].id,
                         bpv['realtime_diasource'].id,
                         bpv['realtime_diaforcedsource'].id,
                         None )
    with db.MG() as mongoclient:
        collection = db.get_mongo_collection( mongoclient, collection_name )
        with db.DBCon() as conn:
            si.read_mongo_brokerinfo( conn, collection )
            rows, cols = conn.execute( "SELECT * FROM temp_diasource_brokerinfo_import" )
            coldex = { c: i for i, c in enumerate(cols) }

        assert len(rows) == 154

        pulledids = set( f"{row[coldex['brokername']]}_{row[coldex['topic']]}_{row[coldex['diasourceid']]}"
                         for row in rows )
        assert len( pulledids ) == len( rows )
        msgids = [ f"{c['brokername']}_{c['topic']}_{c['diasource']['diasourceid']}"
                   for c in collection.find( {},
                                             projection={ 'topic': 1,
                                                          'diasource.diasourceid': 1,
                                                          'brokername': 1 } ) ]
        assert len( msgids ) == len( pulledids )
        assert set( msgids ) == pulledids

        # TODO : check actual content....


def test_import_objects( import_first30days_objects ):
    nobj, nroot, npos = import_first30days_objects
    assert nobj == 12
    assert nroot == 12
    assert npos == 12
    with db.DB() as conn:
        cursor = conn.cursor()
        cursor.execute( "SELECT * FROM diaobject" )
        objrows = cursor.fetchall()
        objcols = { cursor.description[i].name: i for i in range( len(cursor.description) ) }
        assert len(objrows) == 12

        cursor.execute( "SELECT * FROM diaobject_position" )
        posrows = cursor.fetchall()
        poscols = { cursor.description[i].name: i for i in range( len(cursor.description) ) }
        assert set( p[poscols['diaobjectid']] for p in posrows ) == set( o[objcols['diaobjectid']] for o in objrows )
        assert all( p[poscols['ra']] is not None for p in posrows )
        assert all( p[poscols['dec']] is not None for p in posrows )

        cursor.execute( "SELECT id FROM root_diaobject" )
        rootids = [ r[0] for r in cursor.fetchall() ]
        assert len(rootids) == 12

        # Make sure that all the object rootids are distinct
        assert set( r[objcols['rootid']] for r in objrows ) == set( rootids )

    # TODO : look at more?  Compare ppdb_diaobject to diaobject?


def test_import_sources( import_first30days_sources ):
    nsrc, nprvsrc, ninfo = import_first30days_sources
    assert nsrc == 77
    assert nprvsrc == 0   # All sources will have already been imported directly
    assert ninfo == 154
    with db.DB() as conn:
        cursor = conn.cursor()
        cursor.execute( "SELECT * FROM diasource" )
        coldex = { desc[0]: i for i, desc in enumerate(cursor.description) }
        rows = cursor.fetchall()
    assert len(rows) == 77
    assert min( r[coldex['midpointmjdtai']] for r in rows ) == pytest.approx( 60278.029, abs=0.01 )
    assert max( r[coldex['midpointmjdtai']] for r in rows ) == pytest.approx( 60303.211, abs=0.01 )

    with db.MG() as mongoclient:
        collection = db.get_mongo_collection( mongoclient, "source_thumbnails" )
        assert collection.count_documents( {} ) == nsrc

    # TODO :more?


def test_import_prvforcedsources( import_30days_prvforcedsources ):
    assert import_30days_prvforcedsources == 148
    with db.DB() as conn:
        cursor = conn.cursor()
        cursor.execute( "SELECT * FROM diaforcedsource" )
        rows = cursor.fetchall()
    assert len(rows) == 148

    # TODO : More


def test_import_30days( barf, messy_import_30days, alerts_30days_sent_and_brokermessage_consumed ):
    t0 = alerts_30days_sent_and_brokermessage_consumed
    now = datetime.datetime.now( tz=datetime.UTC )
    nobj, nroot, npos, nsrc, nprvsrc, nfrc, ninfo = messy_import_30days
    assert nobj == 12
    assert nroot == 12
    assert npos == 12
    assert nsrc == 77
    assert nprvsrc == 0
    assert ninfo == 154
    assert nfrc == 148

    with db.DBCon() as pqconn:
        tablecounts = { 'diaobject': nobj,
                        'root_diaobject': nroot,
                        'diaobject_position': nobj,
                        'diasource': nsrc + nprvsrc,
                        'diasource_extra': nsrc + nprvsrc,
                        'diasource_brokerinfo': ninfo,
                        'diaforcedsource': nfrc,
                        'diaforcedsource_extra': nfrc
                       }
        for table, num in tablecounts.items():
            q = sql.SQL( "SELECT COUNT(*) FROM {table}" ).format( table=sql.Identifier( table ) )
            assert num == pqconn.execute( q )[0][0][0]

        t1 = pqconn.execute( "SELECT t FROM diasource_import_time WHERE collection=%(col)s",
                             { 'col': f'fastdb_{barf}' } )[0][0][0]
        assert t1 < now
        assert t1 > t0


# **********************************************************************
# Now make sure that if we import 30 days, then import 60 days, we get what's expected

def test_import_30days_60days( barf, messy_import_30days, import_30days_60days, test_user ):
    nobj30, nroot30, npos30, nsrc30, nprvsrc30, nprvfrc30, ninfo30 = messy_import_30days
    nobj60, nroot60, npos60, nsrc60, nprvsrc60, nprvfrc60, ninfo60 = import_30days_60days
    assert nobj60 == 25
    assert nroot60 == 25
    assert npos60 == 25
    assert nsrc60 == 104
    assert nprvsrc60 == 0   # at this point, anything that could be imported has been
    assert nprvfrc60 == 707
    assert ninfo60 == 208

    with db.DBCon( dictcursor=True ) as pqconn:
        tablecounts = { 'diaobject': nobj30 + nobj60,
                        'root_diaobject': nroot30 + nroot60,
                        'diaobject_position': npos30 + npos60,
                        'diasource': nsrc30 + nsrc60 + nprvsrc30 + nprvsrc60,
                        'diasource_extra': nsrc30 + nsrc60 + nprvsrc30 + nprvsrc60,
                        'diasource_brokerinfo': ninfo30 + ninfo60,
                        'diaforcedsource': nprvfrc30 + nprvfrc60,
                        'diaforcedsource_extra': nprvfrc30 + nprvfrc60 }
        for table, num in tablecounts.items():
            q = sql.SQL( "SELECT COUNT(*) FROM {table}" ).format( table=sql.Identifier( table ) )
            assert num == pqconn.execute( q )[0]['count']

        objects = pqconn.execute( "SELECT * FROM diaobject" )
        roots = pqconn.execute( "SELECT * FROM root_diaobject" )
        sources = pqconn.execute( "SELECT * FROM diasource" )

    assert min( r['midpointmjdtai'] for r in sources ) == pytest.approx( 60278.029, abs=0.01 )
    assert max( r['midpointmjdtai'] for r in sources ) == pytest.approx( 60362.3266, abs=0.01 )
    assert set( r['id'] for r in roots ) == set( o['rootid'] for o in objects )

    with db.MG() as mongoclient:
        collection = db.get_mongo_collection( mongoclient, "source_thumbnails" )
        assert collection.count_documents( {} ) == nsrc30 + nsrc60


# **********************************************************************
# Test importating the following 60 days when the first 30 days
#   have NOT been imported.  This will test the time cutoffs, and
#   also test that previous sources pulls in things that didn't
#   get pulled in with the direct source import.

def test_import_only_next60days( import_only_next60days ):
    nobj, nroot, npos, nsrc, nprvsrc, nfrc, ninfo = import_only_next60days
    assert nobj == 29
    assert nroot == 29
    assert npos == 29
    assert nsrc == 104
    assert nprvsrc == 48
    assert nfrc == 770
    assert ninfo == 208

    with db.DBCon( dictcursor=True ) as pqconn:
        tablecounts = { 'diaobject': nobj,
                        'root_diaobject': nobj,
                        'diaobject_position': nobj,
                        'diasource': nsrc + nprvsrc,
                        'diasource_extra': nsrc + nprvsrc,
                        'diasource_brokerinfo': ninfo,
                        'diaforcedsource':  nfrc,
                        'diaforcedsource_extra': nfrc
                       }
        for table, num in tablecounts.items():
            q = sql.SQL( "SELECT COUNT(*) FROM {table}" ).format( table=sql.Identifier( table ) )
            assert num == pqconn.execute( q )[0]['count']

        sources = pqconn.execute( "SELECT * FROM diasource" )

    # The min mjd should be greater than the max mjd from test_import_30days
    assert min( r['midpointmjdtai'] for r in sources ) == pytest.approx( 60278.2469, abs=0.01 )
    assert max( r['midpointmjdtai'] for r in sources ) == pytest.approx( 60362.3266, abs=0.01 )

    with db.MG() as mongoclient:
        collection = db.get_mongo_collection( mongoclient, "source_thumbnails" )
        # Only the sources imported directly will have thumbnails; previous sources will not
        assert collection.count_documents( {} ) == nsrc


# **********************************************************************
# Test that even if all 90 days have been consumed from the brokers,
#   if we give the right time cutoff we only import the first 30 days.

def test_import_only_30days_after_90days_consumed( barf, import_only_30days_after_90days_consumed,
                                                   alerts_30days_sent_and_brokermessage_consumed,
                                                   alerts_60moredays_sent_and_brokermessage_consumed ):
    nobj, nroot, npos, nsrc, nprvsrc, nfrc, ninfo = import_only_30days_after_90days_consumed
    t30consume = alerts_30days_sent_and_brokermessage_consumed
    t60consume = alerts_60moredays_sent_and_brokermessage_consumed
    now = datetime.datetime.now( tz=datetime.UTC )

    assert nroot == 12
    assert nobj == 12
    assert npos == 12
    assert nsrc == 77
    assert nprvsrc == 0
    assert ninfo == 154
    assert nfrc == 148

    # Let's really make sure all 90 days were consumed
    with db.MG() as mg:
        coll = db.get_mongo_collection( mg, f'fastdb_{barf}' )
        assert coll.count_documents( {} ) == 362

    with db.DBCon() as pqconn:
        tablecounts = { 'diaobject': nobj,
                        'root_diaobject': nroot,
                        'diaobject_position': nobj,
                        'diasource': nsrc + nprvsrc,
                        'diasource_extra': nsrc + nprvsrc,
                        'diasource_brokerinfo': ninfo,
                        'diaforcedsource': nfrc,
                        'diaforcedsource_extra': nfrc
                       }
        for table, num in tablecounts.items():
            q = sql.SQL( "SELECT COUNT(*) FROM {table}" ).format( table=sql.Identifier( table ) )
            assert num == pqconn.execute( q )[0][0][0]

        t1 = pqconn.execute( "SELECT t FROM diasource_import_time WHERE collection=%(col)s",
                             { 'col': f'fastdb_{barf}' } )[0][0][0]
        assert t1 < now
        assert t1 < t60consume
        assert t1 >= t30consume



# **********************************************************************
# The test_user fixture is in the next two fixtures not because it's
#   needed for the test, but because this is a convenient test for
#   loading up a database for use developing the web ap.  See the developers documentation for FASTDB.

@pytest.mark.skipif( env_as_bool('RUN_FULL90DAYS'), reason='RUN_FULL90DAYS is set' )
def test_full90days_fast( alerts_90days_sent_received_and_imported ):
    nobj, nroot, npos, nsrc, nprvsrc, nfrc, ninfo = alerts_90days_sent_received_and_imported
    assert nobj == 37
    assert nroot == nobj
    assert npos == nobj
    assert nsrc == 181
    assert nprvsrc == 0
    assert nfrc == 855
    assert ninfo == 2 * nsrc

    with db.MG() as mongoclient:
        collection = db.get_mongo_collection( mongoclient, "source_thumbnails" )
        assert collection.count_documents( {} ) == nsrc


@pytest.mark.skipif( not env_as_bool('RUN_FULL90DAYS'), reason='RUN_FULL90DAYS is not set' )
def test_full90days( fully_do_alerts_90days_sent_received_and_imported ):
    nobj, nroot, npos, nsrc, nprvsrc, nfrc, ninfo = fully_do_alerts_90days_sent_received_and_imported
    assert nobj == 37
    assert nroot == nobj
    assert npos == nobj
    assert nsrc == 181
    assert nprvsrc == 0
    assert nfrc == 855
    assert ninfo == 2 * nsrc

    with db.MG() as mongoclient:
        collection = db.get_mongo_collection( mongoclient, "source_thumbnails" )
        assert collection.count_documents( {} ) == nsrc
