# At the moment, this also tests dr_importer.py
# Except that has been temporarily deprecated

import pytest
import os
import datetime
import time
import random
import psycopg

import db
from util import env_as_bool, FDBLogger
from services.source_importer import SourceImporter
from services.brokerconsumer import FinkConsumer
# from services.dr_importer import DRImporter

# Ordering of these tests matters, because they use module scope fixtures.
# See the comment before class TestImport

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
        import pdb; pdb.set_trace()
        pass
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
# Fixtures that are used in multiple tests

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
            # We can be cavalier here becasue diaobject was supposed to be empty when we started
            conn.execute( "DELETE FROM diaobject_position" )
            conn.execute( "DELETE FROM diaobject" )
            conn.execute( "DELETE FROM root_diaobject" )
            conn.commit()


# def import_first30days_hosts( import_first30days_objects, procver_collection ):
#     bpv, _pv = procver_collection
#     try:
#         dri = DRImporter( bpv['realtime'].id )
#         yield dri.import_host_info()
#     finally:
#         with db.DBCon() as conn:
#             conn.execute( "DELETE FROM host_galaxy" )
#             conn.commit()


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


# Import days 30-90 without importing days 0-30
# This uses the timestamps returned by some of the other fixtures
@pytest.fixture
def import_next60days( barf, procver_collection,
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


# @pytest.fixture
# def import_next60days_hosts( import_next60days_noprv, procver_collection ):
#     bpv, _pv = procver_collection
#     try:
#         dri = DRImporter( bpv['realtime'].id )
#         yield dri.import_host_info()
#     finally:
#         with db.DBCon() as conn:
#             conn.execute( "DELETE FROM host_galaxy" )
#             conn.commit()


# Import days 30-90 after importing days 0-30
@pytest.fixture
def import_30days_60days( barf, procver_collection, import_first30days_sources, import_30days_prvforcedsources,
                           alerts_30days_sent_and_brokermessage_consumed,
                           alerts_60moredays_sent_and_brokermessage_consumed ):
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
            nprvfrc = si.import_forcedsources_from_collection( collection, t0=t0, t1=t1 )
            ninfo = si.import_brokerinfo_from_collection( collection, t0=t0, t1=t1 )
            si.import_cutouts_from_collection( collection, t0=t0, t1=t1 )
        # dri = DRImporter( bpv['realtime'].id )
        # nhosts = dri.import_host_info()

        yield nobj, nroot, npos, nsrc, nprvsrc, nprvfrc, ninfo
    finally:
        # # Parent fixtures do most cleanup, but not of hosts
        # with db.DConB() as conn:
        #     conn.execute( "DELETE FROM host_galaxy" )
        #     conn.commit()
        pass


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
            coldex = { c: i for i, c in enumerate(cols) }
            assert len(rows) == 65
            exrows, _excols = conn.execute( "SELECT * FROM temp_prvdiasource_extra_import" )
            assert len(exrows) == 65


        # Check that the mongo aggregation stuff in read_mongo_provsources is
        #   right by doing it long-form in python

        pulledsourceids = set( f"{row[coldex['diaobjectid']]}_{row[coldex['visit']]}" for row in rows )
        assert len( pulledsourceids ) == len(rows)
        prvsources = {}

        for src in collection.find( {} ):
            if src['prvdiasources'] is not None:
                for prvsrc in src['prvdiasources']:
                    prvsrcid = f"{prvsrc['diaobjectid']}_{prvsrc['visit']}"
                    if prvsrcid not in prvsources:
                        prvsources[ prvsrcid ] = prvsrc

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

        pulledids = set( f"{row[coldex['brokername']]}_{row[coldex['topic']]}_"
                         f"{row[coldex['diaobjectid']]}_{row[coldex['visit']]}"
                         for row in rows )
        assert len( pulledids ) == len( rows )
        msgids = [ f"{c['brokername']}_{c['topic']}_{c['diaobjectid']}_{c['diasource']['visit']}"
                   for c in collection.find( {},
                                             projection={ 'diaobjectid': 1,
                                                          'topic': 1,
                                                          'diasource.visit': 1,
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


@pytest.mark.skip( reason="Hosts aren't currently in the LSST schema" )
def test_import_hosts( import_first30days_hosts ):
    assert import_first30days_hosts == 18
    with db.DB() as conn:
        cursor = conn.cursor()
        cursor.execute( "SELECT COUNT(*) FROM host_galaxy" )
        assert cursor.fetchone()[0] == import_first30days_hosts


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


# **********************************************************************
# Yikes, OK.  pytest raises all kinds of issues.
#
# Background: the tests in alertcycle.py are module-scope tests because
# they're slow.  They're used in test modules other than this one, so I
# can't just put them in this file.
#
# Tests in this file are ordered so that all the ones that need the *60days*
# fixtures not to have run yet happen above this point.  This class depends
# on the *60days* fixture not yet having run (it will run it part way
# through), so all other tests that include the *60days* fixtures must be
# below this class.
#
# But... now we have the problem that we want to do two tests, one before
# *60days* one after, but with a fixtures that's run before the first test,
# persists through the second tests, but then cleans up before any further
# tests after the next two tests.  The only way to do that is to introduce
# another scope and put those two tests in a class.  Which is a weird reason
# to use a class, but whatevs.  (The other way would have been to put these
# two tests in their own module, but that would mean overall evaluting the
# alertcycle.py tests yet another time.)  (And, of course, we could just use
# no module-scope fixtures, but that would be *really* slow, adding >30s for
# every test in this file.)

class TestImport:

    # Run SourceImporter.import_from_mongo after the first 30 days of alerts are out
    @pytest.fixture( scope='class' )
    def run_import_30days( self, barf, procver_collection, alerts_30days_sent_and_brokermessage_consumed ):
        bpv, _pv = procver_collection
        collection_name = f'fastdb_{barf}'
        tsent = alerts_30days_sent_and_brokermessage_consumed

        try:
            with db.MG() as mongoclient:
                collection = db.get_mongo_collection( mongoclient, collection_name )
                si = SourceImporter( bpv['realtime'].id,
                                     bpv['realtime_diaobject_position_60000'].id,
                                     bpv['realtime_diasource'].id,
                                     bpv['realtime_diaforcedsource'].id,
                                     None )
                nobj, nroot, npos, nsrc, nprvsrc, nfrc, ninfo = si.import_from_mongo( collection )

            yield nobj, nroot, npos, nsrc, nprvsrc, nfrc, ninfo, tsent, datetime.datetime.now( tz=datetime.UTC )
        finally:
            with db.DBCon() as conn:
                conn.execute( "DELETE FROM diaforcedsource_extra" )
                conn.execute( "DELETE FROM diaforcedsource" )
                conn.execute( "DELETE FROM diasource_brokerinfo" )
                conn.execute( "DELETE FROM diasource_extra" )
                conn.execute( "DELETE FROM diasource" )
                conn.execute( "DELETE FROM diasource_import_time WHERE collection=%(col)s",
                             { 'col': collection_name} )
                conn.execute( "DELETE FROM diaobject_position" )
                conn.execute( "DELETE FROM diaobject" )
                conn.execute( "DELETE FROM root_diaobject" )
                conn.commit()
            with db.MG() as mongoclient:
                collection = db.get_mongo_collection( mongoclient, "source_thumbnails" )
                collection.delete_many( {} )


    def test_run_import_30days( self, barf, run_import_30days ):
        collection_name = f'fastdb_{barf}'

        nobj, nroot, npos, nsrc, nprvsrc, nfrc, ninfo, tsent, t30 = run_import_30days
        assert nobj == 12
        assert nroot == 12
        assert npos == 12
        assert nsrc == 77
        assert nfrc == 148
        assert ninfo == 154
        with db.DB() as conn:
            cursor = conn.cursor()
            cursor.execute( "SELECT COUNT(*) FROM diaobject" )
            assert cursor.fetchone()[0] == nobj
            cursor.execute( "SELECT COUNT(*) FROM diaobject_position" )
            assert cursor.fetchone()[0] == nobj
            cursor.execute( "SELECT COUNT(*) FROM diasource" )
            assert cursor.fetchone()[0] == nsrc
            cursor.execute( "SELECT COUNT(*) FROM diasource_extra" )
            assert cursor.fetchone()[0] == nsrc
            cursor.execute( "SELECT COUNT(*) FROM diaforcedsource" )
            assert cursor.fetchone()[0] == nfrc
            cursor.execute( "SELECT COUNT(*) FROM diaforcedsource_extra" )
            assert cursor.fetchone()[0] == nfrc
            cursor.execute( "SELECT COUNT(*) FROM diasource_brokerinfo" )
            assert cursor.fetchone()[0] == ninfo
            cursor.execute( "SELECT t FROM diasource_import_time WHERE collection=%(col)s", { 'col': collection_name } )
            t = cursor.fetchone()[0]
            assert t > tsent
            assert t < t30
            assert t30 < datetime.datetime.now( tz=datetime.UTC )

        with db.MG() as mongoclient:
            collection = db.get_mongo_collection( mongoclient, "source_thumbnails" )
            assert collection.count_documents( {} ) == nsrc



    # Test that we can import the next 60 days.  Also make sure the
    #   timestamps come out right; the first 30 days sould be imported before
    #   this test begins and also before the next 60 days of alerts were sent
    #   out.  The next 60 days should be imported after both of those.
    def test_run_import_30days_60days( self, barf, procver_collection, run_import_30days,
                                       alerts_60moredays_sent_and_brokermessage_consumed
                                      ):
        bpv, _pv = procver_collection
        nobj30, nroot30, npos30, nsrc30, nprvsrc30, nfrc30, ninfo30, t30send, t30 = run_import_30days
        t60send = alerts_60moredays_sent_and_brokermessage_consumed
        collection_name = f'fastdb_{barf}'

        try:
            t0 = datetime.datetime.now( tz=datetime.UTC )

            with db.DB() as conn:
                cursor = conn.cursor()
                cursor.execute( "SELECT t FROM diasource_import_time WHERE collection=%(col)s",
                                { 'col': collection_name } )
                t30stamp = cursor.fetchone()[0]

                cursor.execute( "SELECT rootid FROM diaobject" )
                objrootids30 = set( r[0] for r in cursor.fetchall() )
                cursor.execute( "SELECT id FROM root_diaobject" )
                rootids30 = set( r[0] for r in cursor.fetchall() )

            with db.MG() as mongoclient:
                collection = db.get_mongo_collection( mongoclient, collection_name )
                si = SourceImporter( bpv['realtime'].id,
                                     bpv['realtime_diaobject_position_60000'].id,
                                     bpv['realtime_diasource'].id,
                                     bpv['realtime_diaforcedsource'].id,
                                     None )
                nobj, nroot, npos, nsrc, nprvsrc, nfrc, ninfo = si.import_from_mongo( collection )
            t1 = datetime.datetime.now( tz=datetime.UTC )

            with db.DB() as conn:
                cursor = conn.cursor()
                cursor.execute( "SELECT rootid FROM diaobject" )
                objrootids = set( r[0] for r in cursor.fetchall() )
                cursor.execute( "SELECT id FROM root_diaobject" )
                rootids = set( r[0] for r in cursor.fetchall() )

            assert nobj30 == 12
            assert nroot30 == 12
            assert npos30 == 12
            assert nsrc30 == 77
            assert nprvsrc30 == 0
            assert nfrc30 == 148
            assert ninfo30 == 154
            assert objrootids30 == rootids30
            assert nobj == 25
            assert nroot == 25
            assert nsrc == 104
            assert nprvsrc == 0
            assert nfrc == 707
            assert objrootids == rootids

            with db.DB() as conn:
                cursor = conn.cursor()
                cursor.execute( "SELECT COUNT(*) FROM diaobject" )
                totobj = cursor.fetchone()[0]
                cursor.execute( "SELECT COUNT(*) FROM root_diaobject" )
                totroot = cursor.fetchone()[0]
                cursor.execute( "SELECT COUNT(*) FROM diaobject_position" )
                totpos = cursor.fetchone()[0]
                cursor.execute( "SELECT COUNT(*) FROM diasource" )
                totsrc = cursor.fetchone()[0]
                cursor.execute( "SELECT COUNT(*) FROM diasource_extra" )
                totsrcextra = cursor.fetchone()[0]
                cursor.execute( "SELECT COUNT(*) FROM diasource_brokerinfo" )
                totinfo = cursor.fetchone()[0]
                cursor.execute( "SELECT COUNT(*) FROM diaforcedsource" )
                totfrc = cursor.fetchone()[0]
                cursor.execute( "SELECT COUNT(*) FROM diaforcedsource_extra" )
                totfrcextra = cursor.fetchone()[0]
                cursor.execute( "SELECT t FROM diasource_import_time WHERE collection=%(col)s",
                                { 'col': collection_name } )
                t60 = cursor.fetchone()[0]

                cursor.execute( "SELECT rootid FROM diaobject" )
                totobjrootids = set( r[0] for r in cursor.fetchall() )
                cursor.execute( "SELECT id FROM root_diaobject" )
                totrootids = set( r[0] for r in cursor.fetchall() )

            assert totobj == nobj30 + nobj
            assert totroot == totobj
            assert totpos == totobj
            assert totsrc == nsrc30 + nsrc
            assert totsrcextra == totsrc
            assert totinfo == 2 * totsrc
            assert totfrc == nfrc30 + nfrc
            assert totfrcextra == totfrc
            assert totobjrootids == totrootids

            assert t30 > t30send
            assert t60send > t30
            assert t0 > t60send
            assert t0 > t30
            assert t0 > t30stamp
            assert t60 > t0
            assert t1 > t60
            assert t60 < datetime.datetime.now( tz=datetime.UTC )

            with db.MG() as mongoclient:
                collection = db.get_mongo_collection( mongoclient, "source_thumbnails" )
                assert collection.count_documents( {} ) == totsrc

        finally:
            # Necessary cleanup will be done by the run_import_30days
            #   test-scope fixture.
            pass


# **********************************************************************
# Test importating the following 60 days when the first 30 days
#   have NOT been imported.  This will test the time cutoffs, and
#   also test that previous sources pulls in things that didn't
#   get pulled in with the direct source import.

def test_import_next60days( import_next60days ):
    nobj, nroot, npos, nsrc, nprvsrc, nfrc, ninfo = import_next60days
    assert nobj == 29
    assert nroot == 29
    assert npos == 29
    assert nsrc == 104
    assert nprvsrc == 48
    assert nfrc == 770
    assert ninfo == 208

    with db.DB() as conn:
        cursor = conn.cursor()
        cursor.execute( "SELECT * FROM diaobject" )
        objects = cursor.fetchall()
        cursor.execute( "SELECT * FROM root_diaobject" )
        roots = cursor.fetchall()
        cursor.execute( "SELECT * FROM diaobject_position" )
        positions = cursor.fetchall()
        cursor.execute( "SELECT * FROM diasource" )
        sourcecoldex = { desc[0]: i for i, desc in enumerate(cursor.description) }
        sources = cursor.fetchall()
        cursor.execute( "SELECT * FROM diasource_extra" )
        sources_extra = cursor.fetchall()
        cursor.execute( "SELECT * FROM diasource_brokerinfo" )
        infos = cursor.fetchall()
        cursor.execute( "SELECT * FROM diaforcedsource" )
        forced = cursor.fetchall()
        cursor.execute( "SELECT * FROM diaforcedsource_extra" )
        forced_extra = cursor.fetchall()

    assert len(objects) == nobj
    assert len(roots) == nroot
    assert len(positions) == npos
    assert len(sources) == nsrc + nprvsrc
    assert len(sources_extra) == nsrc + nprvsrc
    assert len(infos) == ninfo
    assert len(forced) == nfrc
    assert len(forced_extra) == nfrc
    # The min mjd should be greater than the max mjd from test_import_sources
    assert min( r[sourcecoldex['midpointmjdtai']] for r in sources ) == pytest.approx( 60278.2469, abs=0.01 )
    assert max( r[sourcecoldex['midpointmjdtai']] for r in sources ) == pytest.approx( 60362.3266, abs=0.01 )

    with db.MG() as mongoclient:
        collection = db.get_mongo_collection( mongoclient, "source_thumbnails" )
        # Only the sources imported directly will have thumbnails; previous sources will not
        assert collection.count_documents( {} ) == nsrc


@pytest.mark.skip( reason="Hosts aren't currently in the LSST schema" )
def test_import_next60days_hosts( import_next60days_hosts ):
    assert import_next60days_hosts == 30
    with db.DB() as conn:
        cursor = conn.cursor()
        cursor.execute( "SELECT COUNT(*) FROM host_galaxy" )
        assert cursor.fetchone()[0] == import_next60days_hosts


# **********************************************************************
# Now make sure that if we import 30 days, then import 60 days, we get what's expected

def test_import_30days_60days( import_30days_60days, test_user ):
    nobj, nroot, npos, nsrc, nprvsrc, nprvfrc, ninfo = import_30days_60days
    assert nobj == 25
    assert nroot == 25
    assert npos == 25
    assert nsrc == 104
    assert nprvsrc == 0   # at this point, anything that could be imported has been
    assert nprvfrc == 707
    assert ninfo == 208
    with db.DB() as conn:
        cursor = conn.cursor( row_factory=psycopg.rows.dict_row )
        cursor.execute( "SELECT * FROM diaobject" )
        objects = cursor.fetchall()
        cursor.execute( "SELECT * FROM root_diaobject" )
        roots = cursor.fetchall()
        cursor.execute( "SELECT * FROM diaobject_position" )
        positions = cursor.fetchall()
        cursor.execute( "SELECT * FROM diasource" )
        sources = cursor.fetchall()
        cursor.execute( "SELECT * FROM diasource_extra" )
        sources_extra = cursor.fetchall()
        cursor.execute( "SELECT * FROM diasource_brokerinfo" )
        brokerinfos = cursor.fetchall()
        cursor.execute( "SELECT * FROM diaforcedsource" )
        forced = cursor.fetchall()
        cursor.execute( "SELECT * FROM diaforcedsource_extra" )
        forced_extra = cursor.fetchall()

    # nobj, nrsc, nprvsrc, nprvfrc above are affected row counts returned
    #   from the import of days 60-90, so are lower than the total numbers
    #   in the tables below.
    assert len(objects) == 37
    assert len(roots) ==len(objects)
    assert len(positions) == len(objects)
    assert len(sources) == 181
    assert len(sources_extra) == len(sources)
    assert len(brokerinfos) == 2 * len(sources)
    assert len(forced) == 855
    assert len(forced_extra) == len(forced)
    assert min( r['midpointmjdtai'] for r in sources ) == pytest.approx( 60278.029, abs=0.01 )
    assert max( r['midpointmjdtai'] for r in sources ) == pytest.approx( 60362.3266, abs=0.01 )
    assert set( r['id'] for r in roots ) == set( o['rootid'] for o in objects )

    with db.MG() as mongoclient:
        collection = db.get_mongo_collection( mongoclient, "source_thumbnails" )
        assert collection.count_documents( {} ) == len( sources )


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
