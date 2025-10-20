# At the moment, this also tests dr_importer.py

import pytest
import datetime

import psycopg

import db
from util import env_as_bool
from services.source_importer import SourceImporter
from services.dr_importer import DRImporter

# Ordering of these tests matters, because they use module scope fixtures.
# See the comment before class TestImport

# NOTE : there is currently no test that checks when object_processing_version
#   and processing_version are different


# **********************************************************************
# Fixtures that are used in multiple tests

@pytest.fixture
def import_first30days_objects( barf, alerts_30days_sent_and_brokermessage_consumed, procver_collection ):
    bpv, _pv = procver_collection
    collection_name = f'fastdb_{barf}'
    t1 = alerts_30days_sent_and_brokermessage_consumed

    try:
        si = SourceImporter( bpv['realtime'].id, bpv['realtime'].id )
        with db.MG() as mongoclient:
            collection = db.get_mongo_collection( mongoclient, collection_name )
            nobj, nroot = si.import_objects_from_collection( collection, t1=t1 )

        yield nobj, nroot
    finally:
        with db.DB() as conn:
            cursor = conn.cursor()
            # We can be cavalier here becasue diaobject was supposed to be empty when we started
            cursor.execute( "DELETE FROM diaobject" )
            cursor.execute( "DELETE FROM root_diaobject" )
            conn.commit()


def import_first30days_hosts( import_first30days_objects, procver_collection ):
    bpv, _pv = procver_collection
    try:
        dri = DRImporter( bpv['realtime'].id )
        yield dri.import_host_info()
    finally:
        with db.DB() as conn:
            cursor = conn.cursor()
            cursor.execute( "DELETE FROM host_galaxy" )
            conn.commit()


@pytest.fixture
def import_first30days_sources( barf, import_first30days_objects, procver_collection,
                                alerts_30days_sent_and_brokermessage_consumed ):
    bpv, _pv = procver_collection
    collection_name = f'fastdb_{barf}'
    t1 = alerts_30days_sent_and_brokermessage_consumed

    try:
        si = SourceImporter( bpv['realtime'].id, bpv['realtime'].id )
        with db.MG() as mongoclient:
            collection = db.get_mongo_collection( mongoclient, collection_name )
            n = si.import_sources_from_collection( collection, t1=t1 )

        yield n
    finally:
        with db.DB() as conn:
            cursor = conn.cursor()
            cursor.execute( "DELETE FROM diasource" )
            conn.commit()


@pytest.fixture
def import_30days_prvsources( barf, import_first30days_sources, procver_collection,
                              alerts_30days_sent_and_brokermessage_consumed ):
    bpv, _pv = procver_collection
    collection_name = f'fastdb_{barf}'
    t1 = alerts_30days_sent_and_brokermessage_consumed

    try:
        si = SourceImporter( bpv['realtime'].id, bpv['realtime'].id )
        with db.MG() as mongoclient:
            collection = db.get_mongo_collection( mongoclient, collection_name )
            n = si.import_prvsources_from_collection( collection, t1=t1 )

        yield n
    finally:
        # Don't have to clean up; import_first30days_sources will clean up the diasources table
        pass


@pytest.fixture
def import_30days_prvforcedsources( barf, import_first30days_sources, procver_collection,
                              alerts_30days_sent_and_brokermessage_consumed ):
    bpv, _pv = procver_collection
    collection_name = f'fastdb_{barf}'
    t1 = alerts_30days_sent_and_brokermessage_consumed

    try:
        si = SourceImporter( bpv['realtime'].id, bpv['realtime'].id )
        with db.MG() as mongoclient:
            collection = db.get_mongo_collection( mongoclient, collection_name )
            n = si.import_prvforcedsources_from_collection( collection, t1=t1 )

        yield n
    finally:
        with db.DB() as conn:
            cursor = conn.cursor()
            cursor.execute( "DELETE FROM diaforcedsource" )
            conn.commit()


# Import days 30-90 without importing days 0-30
# This uses the timestamps returned by some of the other fixtures
@pytest.fixture
def import_next60days_noprv( barf, procver_collection,
                             alerts_30days_sent_and_brokermessage_consumed,
                             alerts_60moredays_sent_and_brokermessage_consumed
                            ):
    bpv, _pv = procver_collection
    collection_name = f'fastdb_{barf}'
    t0 = alerts_30days_sent_and_brokermessage_consumed
    t1 = alerts_60moredays_sent_and_brokermessage_consumed

    try:
        si = SourceImporter( bpv['realtime'].id, bpv['realtime'].id )
        with db.MG() as mongoclient:
            collection = db.get_mongo_collection( mongoclient, collection_name )
            nobj, nroot = si.import_objects_from_collection( collection, t0=t0, t1=t1 )
            nsrc = si.import_sources_from_collection( collection, t0=t0, t1=t1 )

        yield nobj, nroot, nsrc
    finally:
        with db.DB() as conn:
            cursor = conn.cursor()
            cursor.execute( "DELETE FROM diaforcedsource" )
            cursor.execute( "DELETE FROM diasource" )
            cursor.execute( "DELETE FROM diaobject" )
            cursor.execute( "DELETE FROM root_diaobject" )
            conn.commit()


@pytest.fixture
def import_next60days_hosts( import_next60days_noprv, procver_collection ):
    bpv, _pv = procver_collection
    try:
        dri = DRImporter( bpv['realtime'].id )
        yield dri.import_host_info()
    finally:
        with db.DB() as conn:
            cursor = conn.cursor()
            cursor.execute( "DELETE FROM host_galaxy" )
            conn.commit()


@pytest.fixture
def import_next60days_prv( barf, procver_collection, import_next60days_noprv,
                           alerts_30days_sent_and_brokermessage_consumed,
                           alerts_60moredays_sent_and_brokermessage_consumed ):
    bpv, _pv = procver_collection
    collection_name = f'fastdb_{barf}'
    t0 = alerts_30days_sent_and_brokermessage_consumed
    t1 = alerts_60moredays_sent_and_brokermessage_consumed

    try:
        si = SourceImporter( bpv['realtime'].id, bpv['realtime'].id )
        with db.MG() as mongoclient:
            collection = db.get_mongo_collection( mongoclient, collection_name )
            nsrc = si.import_prvsources_from_collection( collection, t0=t0, t1=t1 )
            nfrc = si.import_prvforcedsources_from_collection( collection, t0=t0, t1=t1 )

        yield nsrc, nfrc
    finally:
        # Parent fixture does cleanup
        pass


# Import days 30-90 after importing days 0-30
@pytest.fixture
def import_30days_60days( barf, procver_collection, import_30days_prvsources, import_30days_prvforcedsources,
                           alerts_30days_sent_and_brokermessage_consumed,
                           alerts_60moredays_sent_and_brokermessage_consumed ):
    bpv, _pv = procver_collection
    collection_name = f'fastdb_{barf}'
    t0 = alerts_30days_sent_and_brokermessage_consumed
    t1 = alerts_60moredays_sent_and_brokermessage_consumed

    try:
        si = SourceImporter( bpv['realtime'].id, bpv['realtime'].id )
        with db.MG() as mongoclient:
            collection = db.get_mongo_collection( mongoclient, collection_name )
            nobj, nroot = si.import_objects_from_collection( collection, t0=t0, t1=t1 )
            nsrc = si.import_sources_from_collection( collection, t0=t0, t1=t1 )
            nprvsrc = si.import_prvsources_from_collection( collection, t0=t0, t1=t1 )
            nprvfrc = si.import_prvforcedsources_from_collection( collection, t0=t0, t1=t1 )
        dri = DRImporter( bpv['realtime'].id )
        nhosts = dri.import_host_info()

        yield nobj, nroot, nsrc, nprvsrc, nprvfrc, nhosts
    finally:
        # Parent fixtures do most cleanup, but not of hosts
        with db.DB() as conn:
            cursor = conn.cursor()
            cursor.execute( "DELETE FROM host_galaxy" )
            conn.commit()


# **********************************************************************
# Tests on importation of the first 30 days

def test_read_mongo_objects( barf, alerts_30days_sent_and_brokermessage_consumed, procver_collection ):
    bpv, _pv = procver_collection
    collection_name = f'fastdb_{barf}'

    si = SourceImporter( bpv['realtime'].id, bpv['realtime'].id )
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


def test_read_mongo_sources( barf, alerts_30days_sent_and_brokermessage_consumed, procver_collection ):
    bpv, _pv = procver_collection
    collection_name = f'fastdb_{barf}'

    # Not going to test time cuts here because it's the same code path that
    #   was already tested intest_read_mongo_objects

    si = SourceImporter( bpv['realtime'].id, bpv['realtime'].id )
    with db.MG() as mongoclient:
        collection = db.get_mongo_collection( mongoclient, collection_name )
        with db.DB() as pqconn:
            si.read_mongo_sources( pqconn, collection )
            cursor = pqconn.cursor()
            cursor.execute( "SELECT * FROM temp_diasource_import" )
            rows = cursor.fetchall()

    assert len(rows) == 77

    # TODO : more stringent tests


def test_read_mongo_previous_sources( barf, alerts_30days_sent_and_brokermessage_consumed, procver_collection ):
    bpv, _pv = procver_collection
    collection_name = f'fastdb_{barf}'

    si = SourceImporter( bpv['realtime'].id, bpv['realtime'].id )
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

        pulledsourceids = set( f"{row[coldex['diaobjectid']]}_{row[coldex['visit']]}" for row in rows )
        assert len( pulledsourceids ) == len(rows)
        prvsources = {}

        for src in collection.find( {} ):
            if src['msg']['prvDiaSources'] is not None:
                for prvsrc in src['msg']['prvDiaSources']:
                    prvsrcid = f"{prvsrc['diaObjectId']}_{prvsrc['visit']}"
                    if prvsrcid not in prvsources:
                        prvsources[ prvsrcid ] = prvsrc

        assert set( prvsources.keys() ) == pulledsourceids

    # TODO: check more fields


def test_read_mongo_previous_forced_sources( barf, alerts_30days_sent_and_brokermessage_consumed, procver_collection ):
    bpv, _pv = procver_collection
    collection_name = f'fastdb_{barf}'

    si = SourceImporter( bpv['realtime'].id, bpv['realtime'].id )
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

        pulledsourceids = set( f"{row[coldex['diaobjectid']]}_{row[coldex['visit']]}" for row in rows )
        assert len( pulledsourceids ) == len(rows)
        prvsources = {}

        for src in collection.find( {} ):
            if src['msg']['prvDiaForcedSources'] is not None:
                for prvsrc in src['msg']['prvDiaForcedSources']:
                    prvsrcid = f"{prvsrc['diaObjectId']}_{prvsrc['visit']}"
                    if prvsrcid not in prvsources:
                        prvsources[ prvsrcid ] = prvsrc

        assert set( prvsources.keys() ) == pulledsourceids

    # TODO: check more fields


def test_import_objects( import_first30days_objects ):
    nobj, nroot = import_first30days_objects
    assert nobj == 12
    assert nroot == 12
    with db.DB() as conn:
        cursor = conn.cursor()
        cursor.execute( "SELECT * FROM diaobject" )
        objrows = cursor.fetchall()
        objcols = { cursor.description[i].name: i for i in range( len(cursor.description) ) }
        assert len(objrows) == 12

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
    assert import_first30days_sources == 77
    with db.DB() as conn:
        cursor = conn.cursor()
        cursor.execute( "SELECT * FROM diasource" )
        coldex = { desc[0]: i for i, desc in enumerate(cursor.description) }
        rows = cursor.fetchall()
    assert len(rows) == 77
    assert min( r[coldex['midpointmjdtai']] for r in rows ) == pytest.approx( 60278.029, abs=0.01 )
    assert max( r[coldex['midpointmjdtai']] for r in rows ) == pytest.approx( 60303.211, abs=0.01 )

    # TODO :more?


def test_import_prvsources( import_30days_prvsources ):
    assert import_30days_prvsources == 0
    with db.DB() as conn:
        cursor = conn.cursor()
        cursor.execute( "SELECT * FROM diasource" )
        rows = cursor.fetchall()
    # There won't be any new sources, because all sources that might
    #   have been a previous already got imported by
    #   import_sources.
    assert len(rows) == 77

    # TODO : More


def test_import_provforcedsources( import_30days_prvforcedsources ):
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
                si = SourceImporter( bpv['realtime'].id, bpv['realtime'].id )
                nobj, nroot, nsrc, nfrc = si.import_from_mongo( collection )

            yield nobj, nroot, nsrc, nfrc, tsent, datetime.datetime.now( tz=datetime.UTC )
        finally:
            with db.DB() as conn:
                cursor = conn.cursor()
                cursor.execute( "DELETE FROM diaforcedsource" )
                cursor.execute( "DELETE FROM diasource" )
                cursor.execute( "DELETE FROM diasource_import_time WHERE collection=%(col)s",
                                { 'col': collection_name} )
                cursor.execute( "DELETE FROM diaobject" )
                cursor.execute( "DELETE FROM root_diaobject" )
                conn.commit()


    def test_run_import_30days( self, barf, run_import_30days ):
        collection_name = f'fastdb_{barf}'

        nobj, nroot, nsrc, nfrc, tsent, t30 = run_import_30days
        assert nobj == 12
        assert nroot == 12
        assert nsrc == 77
        assert nfrc == 148
        with db.DB() as conn:
            cursor = conn.cursor()
            cursor.execute( "SELECT COUNT(*) FROM diaobject" )
            assert cursor.fetchone()[0] == nobj
            cursor.execute( "SELECT COUNT(*) FROM diasource" )
            assert cursor.fetchone()[0] == nsrc
            cursor.execute( "SELECT COUNT(*) FROM diaforcedsource" )
            assert cursor.fetchone()[0] == nfrc
            cursor.execute( "SELECT t FROM diasource_import_time WHERE collection=%(col)s", { 'col': collection_name } )
            t = cursor.fetchone()[0]
            assert t > tsent
            assert t < t30
            assert t30 < datetime.datetime.now( tz=datetime.UTC )


    # Test that we can import the next 60 days.  Also make sure the
    #   timestamps come out right; the first 30 days sould be imported before
    #   this test begins and also before the next 60 days of alerts were sent
    #   out.  The next 60 days should be imported after both of those.
    def test_run_import_30days_60days( self, barf, procver_collection, run_import_30days,
                                       alerts_60moredays_sent_and_brokermessage_consumed
                                      ):
        bpv, _pv = procver_collection
        nobj30, nroot30, nsrc30, nfrc30, t30send, t30 = run_import_30days
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
                si = SourceImporter( bpv['realtime'].id, bpv['realtime'].id )
                nobj, nroot, nsrc, nfrc = si.import_from_mongo( collection )
            t1 = datetime.datetime.now( tz=datetime.UTC )

            with db.DB() as conn:
                cursor = conn.cursor()
                cursor.execute( "SELECT rootid FROM diaobject" )
                objrootids = set( r[0] for r in cursor.fetchall() )
                cursor.execute( "SELECT id FROM root_diaobject" )
                rootids = set( r[0] for r in cursor.fetchall() )

            assert nobj30 == 12
            assert nroot30 == 12
            assert nsrc30 == 77
            assert nfrc30 == 148
            assert objrootids30 == rootids30
            assert nobj == 25
            assert nroot == 25
            assert nsrc == 104
            assert nfrc == 707
            assert objrootids == rootids

            with db.DB() as conn:
                cursor = conn.cursor()
                cursor.execute( "SELECT COUNT(*) FROM diaobject" )
                totobj = cursor.fetchone()[0]
                cursor.execute( "SELECT COUNT(*) FROM root_diaobject" )
                totroot = cursor.fetchone()[0]
                cursor.execute( "SELECT COUNT(*) FROM diasource" )
                totsrc = cursor.fetchone()[0]
                cursor.execute( "SELECT COUNT(*) FROM diaforcedsource" )
                totfrc = cursor.fetchone()[0]
                cursor.execute( "SELECT t FROM diasource_import_time WHERE collection=%(col)s",
                                { 'col': collection_name } )
                t60 = cursor.fetchone()[0]

                cursor.execute( "SELECT rootid FROM diaobject" )
                totobjrootids = set( r[0] for r in cursor.fetchall() )
                cursor.execute( "SELECT id FROM root_diaobject" )
                totrootids = set( r[0] for r in cursor.fetchall() )

            assert totobj == 37
            assert totroot == 37
            assert totsrc == 181
            assert totfrc == 855
            assert totobjrootids == totrootids
            assert totobj == nobj + nobj30
            assert totsrc == nsrc + nsrc30
            assert totfrc == nfrc + nfrc30

            assert t30 > t30send
            assert t60send > t30
            assert t0 > t60send
            assert t0 > t30
            assert t0 > t30stamp
            assert t60 > t0
            assert t1 > t60
            assert t60 < datetime.datetime.now( tz=datetime.UTC )

        finally:
            # Necessary cleanup will be done by the run_import_30days
            #   test-scope fixture.
            pass


# **********************************************************************
# Test importating the following 60 days when the first 30 days
#   have NOT been imported.  This will test the time cutoffs, and
#   also test that previous sources pulls in things that didn't
#   get pulled in with the direct source import.

def test_import_next60days( import_next60days_noprv ):
    nobj, nroot, nsrc = import_next60days_noprv
    assert nobj == 29
    assert nroot == 29
    assert nsrc == 104

    with db.DB() as conn:
        cursor = conn.cursor()
        cursor.execute( "SELECT * FROM diasource" )
        sourcecoldex = { desc[0]: i for i, desc in enumerate(cursor.description) }
        sources = cursor.fetchall()
        cursor.execute( "SELECT * FROM diaobject" )
        objects = cursor.fetchall()
        cursor.execute( "SELECT * FROM root_diaobject" )
        roots = cursor.fetchall()
        cursor.execute( "SELECT * FROM diaforcedsource" )
        forced = cursor.fetchall()

    assert len(objects) == 29
    assert len(roots) == 29
    assert len(sources) == 104
    assert len(forced) == 0
    # The min mjd should be greater than the max mjd from test_import_sources
    assert min( r[sourcecoldex['midpointmjdtai']] for r in sources ) == pytest.approx( 60310.1535, abs=0.01 )
    assert max( r[sourcecoldex['midpointmjdtai']] for r in sources ) == pytest.approx( 60362.3266, abs=0.01 )


@pytest.mark.skip( reason="Hosts aren't currently in the LSST schema" )
def test_import_next60days_hosts( import_next60days_hosts ):
    assert import_next60days_hosts == 30
    with db.DB() as conn:
        cursor = conn.cursor()
        cursor.execute( "SELECT COUNT(*) FROM host_galaxy" )
        assert cursor.fetchone()[0] == import_next60days_hosts


def test_import_next60days_with_prev( import_next60days_prv ):
    nprvsources, nprvforced = import_next60days_prv
    assert nprvsources == 48
    assert nprvforced == 770
    with db.DB() as conn:
        cursor = conn.cursor()
        cursor.execute( "SELECT * FROM diasource" )
        sourcecoldex = { desc[0]: i for i, desc in enumerate(cursor.description) }
        sources = cursor.fetchall()
        cursor.execute( "SELECT * FROM diaobject" )
        objects = cursor.fetchall()
        cursor.execute( "SELECT * FROM diaforcedsource" )
        forced = cursor.fetchall()

    assert len(objects) == 29
    # len(sources) is not the same as nprvsources because nprvsources are only the sources added from
    #   previousDiaSource in all the alerts.
    assert len(sources) == 152
    assert len(forced) == 770
    # It seems that the first source from test_import_sources is not one of the previouses of this
    #   new batch (meaning that object was not detected in days 60-90), because the lowest mjd here
    #   is not the same as the lowest mjd in test_import_sources.  (assuming that that object was
    #   detected again in days 30-90).
    assert min( r[sourcecoldex['midpointmjdtai']] for r in sources ) == pytest.approx( 60278.2469, abs=0.01 )
    assert max( r[sourcecoldex['midpointmjdtai']] for r in sources ) == pytest.approx( 60362.3266, abs=0.01 )


# **********************************************************************
# Now make sure that if we import 30 days, then import 60 days, we get what's expected

def test_import_30days_60days( import_30days_60days, test_user ):
    nobj, nroot, nsrc, nprvsrc, nprvfrc, nhosts = import_30days_60days
    assert nobj == 25
    assert nroot == 25
    assert nsrc == 104
    assert nprvsrc == 0   # at this point, anything that could be imported has been
    assert nprvfrc == 707
    # Hosts aren't currently in the LSST schema
    # assert nhosts == 42
    assert nhosts == 0
    with db.DB() as conn:
        cursor = conn.cursor( row_factory=psycopg.rows.dict_row )
        cursor.execute( "SELECT * FROM diasource" )
        sources = cursor.fetchall()
        cursor.execute( "SELECT * FROM diaobject" )
        objects = cursor.fetchall()
        cursor.execute( "SELECT * FROM diaforcedsource" )
        forced = cursor.fetchall()
        cursor.execute( "SELECT * FROM host_galaxy" )
        hosts = cursor.fetchall()
        cursor.execute( "SELECT * FROM root_diaobject" )
        roots = cursor.fetchall()

    # nobj, nrsc, nprvsrc, nprvfrc above are affected row counts returned
    #   from the import of days 60-90, so are lower than the total numbers
    #   in the tables below.
    assert len(objects) == 37
    assert len(sources) == 181
    assert len(forced) == 855
    # Hosts aren't currently in the LSST schema
    # assert len(hosts) == 42
    assert len(hosts) == 0
    assert min( r['midpointmjdtai'] for r in sources ) == pytest.approx( 60278.029, abs=0.01 )
    assert max( r['midpointmjdtai'] for r in sources ) == pytest.approx( 60362.3266, abs=0.01 )
    assert set( r['id'] for r in roots ) == set( o['rootid'] for o in objects )

    # Make sure hosts loaded match the hosts we thought should be loaded
    hostids = set( [ h['id'] for h in hosts ] )
    objhostids = set( [ o['nearbyextobj1id'] for o in objects if o['nearbyextobj1id'] is not None ] )
    objhostids.update( [ o['nearbyextobj2id'] for o in objects if o['nearbyextobj2id'] is not None ] )
    objhostids.update( [ o['nearbyextobj3id'] for o in objects if o['nearbyextobj3id'] is not None ] )
    assert hostids == objhostids


# **********************************************************************
# The test_user fixture is in the next two fixtures not because it's
#   needed for the test, but because this is a convenient test for
#   loading up a database for use developing the web ap.  See the developers documentation for FASTDB.

@pytest.mark.skipif( env_as_bool('RUN_FULL90DAYS'), reason='RUN_FULL90DAYS is set' )
def test_full90days_fast( alerts_90days_sent_received_and_imported ):
    # TODO -- actually check stuff?
    pass


@pytest.mark.skipif( not env_as_bool('RUN_FULL90DAYS'), reason='RUN_FULL90DAYS is not set' )
def test_full90days( fully_do_alerts_90days_sent_received_and_imported ):
    # TODO -- actually check stuff?
    pass
