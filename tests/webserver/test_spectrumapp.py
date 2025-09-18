import pytest
import datetime
import pytz
import uuid
import numpy
import psycopg.rows

import astropy.time

import ltcv
import db


@pytest.fixture
def setup_wanted_spectra_etc( procver_collection, alerts_90days_sent_received_and_imported, test_user ):
    bpvs, _pvs = procver_collection
    rtbpv = bpvs['realtime']
    # Prime the database with some wanted spectra
    # Some objects of interest:
    #    1696949 — 5 detections, 5 forced
    #                  last forced r = 60359.35 (21.48), last forced = 60359.36 (i, 21.49)
    #                  last source r = 60359.35 (21.48), last source = 60362.33 (z, 21.36)
    #    1981540 — 30 detections, 38 forced
    #                  last forced r = 60352.13 (23.38), last forced = 60355.11 (g, 24.63)
    #                  last source r = 60352.13 (23.38), last source = 60360.09 (z, 21.59)
    #     191776 — 12 detections, 37 forced
    #                  last forced r = 60345.20 (22.31), last forced = 60345.25 (g, 23.36)
    #                  last source r = 60353.24 (22.75), last source = 60353.26 (i, 22.25)
    #    1747042 —  8 detections, 12 forced
    #                  last forced r = 60322.34 (22.35), last forced = 60341.35 (Y, 22.21)
    #                  last source r = 60322.34 (22.35), last source = 60343.31 (i, 23.04)
    #    1173200 — 13 detections, 29 forced
    #                  last forced r = 60322.20 (23.83), last forced = 60326.10 (Y, 22.78)
    #                  last source r = 60316.10 (23.57), last source = 60327.20 (i, 23.44)

    # The latest detection at all to make it into daisource is from
    #  MJD 60362.33 = 2024-02-22T07:55:12Z

    mjdnow = 60362.5
    now = datetime.datetime.utcfromtimestamp( astropy.time.Time( mjdnow, format='mjd', scale='tai' ).unix_tai )
    now = pytz.utc.localize( now )
    try:
        with db.DB() as con:
            cursor = con.cursor()
            cursor.execute( "SELECT rootid,diaobjectid FROM diaobject "
                            "WHERE diaobjectid=ANY(%(obj)s) AND base_procver_id=%(procver)s",
                            { 'obj': [ 1696949, 1981540, 191776, 1747042, 1173200 ],
                              'procver': rtbpv.id } )
            idmap = { r[1]: r[0] for r in cursor.fetchall() }
            assert len(idmap) == 5

            # requester1 has asked for all five
            cursor.execute( "INSERT INTO wantedspectra(wantspec_id,root_diaobject_id,wanttime,user_id,"
                            "                          requester,priority) "
                            "VALUES (%(wid)s,%(rid)s,%(t)s,%(uid)s,%(req)s,%(prio)s)",
                            { 'wid': uuid.uuid4(),
                              'rid': idmap[1696949],
                              't': now - datetime.timedelta( minutes=1 ),
                              'uid': test_user.id,
                              'req': 'requester1',
                              'prio': 3 } )
            cursor.execute( "INSERT INTO wantedspectra(wantspec_id,root_diaobject_id,wanttime,user_id,"
                            "                          requester,priority) "
                            "VALUES (%(wid)s,%(rid)s,%(t)s,%(uid)s,%(req)s,%(prio)s)",
                            { 'wid': uuid.uuid4(),
                              'rid': idmap[1981540],
                              't': now - datetime.timedelta( days=1 ),
                              'uid': test_user.id,
                              'req': 'requester1',
                              'prio': 4 } )
            cursor.execute( "INSERT INTO wantedspectra(wantspec_id,root_diaobject_id,wanttime,user_id,"
                            "                          requester,priority) "
                            "VALUES (%(wid)s,%(rid)s,%(t)s,%(uid)s,%(req)s,%(prio)s)",
                            { 'wid': uuid.uuid4(),
                              'rid': idmap[191776],
                              't': now - datetime.timedelta( days=5 ),
                              'uid': test_user.id,
                              'req': 'requester1',
                              'prio': 2 } )
            cursor.execute( "INSERT INTO wantedspectra(wantspec_id,root_diaobject_id,wanttime,user_id,"
                            "                          requester,priority) "
                            "VALUES (%(wid)s,%(rid)s,%(t)s,%(uid)s,%(req)s,%(prio)s)",
                            { 'wid': uuid.uuid4(),
                              'rid': idmap[1747042],
                              't': now - datetime.timedelta( days=10 ),
                              'uid': test_user.id,
                              'req': 'requester1',
                              'prio': 1 } )
            cursor.execute( "INSERT INTO wantedspectra(wantspec_id,root_diaobject_id,wanttime,user_id,"
                            "                          requester,priority) "
                            "VALUES (%(wid)s,%(rid)s,%(t)s,%(uid)s,%(req)s,%(prio)s)",
                            { 'wid': uuid.uuid4(),
                              'rid': idmap[1173200],
                              't': now - datetime.timedelta( days=40 ),
                              'uid': test_user.id,
                              'req': 'requester1',
                              'prio': 5 } )
            # requester2 very recently asked for a spectrum of a source that requester1 asked for a long time ago
            cursor.execute( "INSERT INTO wantedspectra(wantspec_id,root_diaobject_id,wanttime,user_id,"
                            "                          requester,priority) "
                            "VALUES (%(wid)s,%(rid)s,%(t)s,%(uid)s,%(req)s,%(prio)s)",
                            { 'wid': uuid.uuid4(),
                              'rid': idmap[1173200],
                              't': now - datetime.timedelta( days=1 ),
                              'uid': test_user.id,
                              'req': 'requester2',
                              'prio': 5 } )

            # Put in a couple of spectrum claims
            cursor.execute( "INSERT INTO plannedspectra(plannedspec_id,root_diaobject_id,facility,created_at,plantime) "
                            "VALUES (%(pid)s,%(rid)s,%(fac)s,%(ct)s,%(pt)s)",
                            { 'pid': uuid.uuid4(),
                              'rid': idmap[1747042],
                              'fac': 'test facility',
                              'ct': now - datetime.timedelta( days=9 ),
                              'pt': now - datetime.timedelta( days=8 )
                             } )
            cursor.execute( "INSERT INTO plannedspectra(plannedspec_id,root_diaobject_id,facility,created_at,plantime) "
                            "VALUES (%(pid)s,%(rid)s,%(fac)s,%(ct)s,%(pt)s)",
                            { 'pid': uuid.uuid4(),
                              'rid': idmap[1696949],
                              'fac': 'test facility',
                              'ct': now,
                              'pt': now + datetime.timedelta( days=1 )
                             } )
            cursor.execute( "INSERT INTO plannedspectra(plannedspec_id,root_diaobject_id,facility,created_at,plantime) "
                            "VALUES (%(pid)s,%(rid)s,%(fac)s,%(ct)s,%(pt)s)",
                            { 'pid': uuid.uuid4(),
                              'rid': idmap[191776],
                              'fac': 'test facility',
                              'ct': now - datetime.timedelta( days=4 ),
                              'pt': now - datetime.timedelta( days=3 )
                             } )

            # One of the planned spectra was observed
            cursor.execute( "INSERT INTO spectruminfo(specinfo_id,root_diaobject_id,facility,inserted_at,"
                            "                         mjd,z,classid) "
                            "VALUES (%(sid)s,%(rid)s,%(fac)s,%(t)s,%(mjd)s,%(z)s,%(class)s)",
                            { 'sid': uuid.uuid4(),
                              'rid': idmap[191776],
                              'fac': 'test facility',
                              't': now - datetime.timedelta( days=1 ),
                              'mjd': mjdnow - 2,
                              'z': 0.25,
                              'class': 2222 } )

            con.commit()

        yield mjdnow, now, idmap

    finally:
        with db.DB() as con:
            cursor = con.cursor()
            cursor.execute( "DELETE FROM spectruminfo" )
            cursor.execute( "DELETE FROM plannedspectra" )
            cursor.execute( "DELETE FROM wantedspectra" )
            con.commit()


@pytest.fixture
def setup_spectrum_info( setup_wanted_spectra_etc ):
    mjdnow, now, idmap = setup_wanted_spectra_etc

    # The previous fixture adds one.  Let's add more.

    with db.DB() as con:
        cursor = con.cursor()

        cursor.execute( "INSERT INTO spectruminfo(specinfo_id,root_diaobject_id,facility,inserted_at,"
                        "                         mjd,z,classid) "
                        "VALUES (%(sid)s,%(rid)s,%(fac)s,%(t)s,%(mjd)s,%(z)s,%(class)s)",
                        { 'sid': uuid.uuid4(),
                          'rid': idmap[1173200],
                          'fac': 'test facility',
                          't': now - datetime.timedelta( days=25 ),
                          'mjd': mjdnow - 24,
                          'z': 0.12,
                          'class': 2235 } )

        cursor.execute( "INSERT INTO spectruminfo(specinfo_id,root_diaobject_id,facility,inserted_at,"
                        "                         mjd,z,classid) "
                        "VALUES (%(sid)s,%(rid)s,%(fac)s,%(t)s,%(mjd)s,%(z)s,%(class)s)",
                        { 'sid': uuid.uuid4(),
                          'rid': idmap[1173200],
                          'fac': "Galileo's Telescope",
                          't': now - datetime.timedelta( days=2 ),
                          'mjd': mjdnow - 3,
                          'z': 0.005,
                          'class': 2322 } )

        cursor.execute( "INSERT INTO spectruminfo(specinfo_id,root_diaobject_id,facility,inserted_at,"
                        "                         mjd,z,classid) "
                        "VALUES (%(sid)s,%(rid)s,%(fac)s,%(t)s,%(mjd)s,%(z)s,%(class)s)",
                        { 'sid': uuid.uuid4(),
                          'rid': idmap[191776],
                          'fac': "Rob's C8 in his back yard",
                          't': now - datetime.timedelta( days=10 ),
                          'mjd': mjdnow - 14,
                          'z': 1.25,
                          'class': 2342 } )

        con.commit()

    return mjdnow, now, idmap
    # Don't have to clean up, parent fixture will do that



def test_ask_for_spectra( procver_collection, alerts_90days_sent_received_and_imported, fastdb_client ):
    _bpvs, pvs = procver_collection
    rtpv = pvs['realtime']
    try:
        # Get some hot lightcurves
        df, objdf, _hostdf = ltcv.get_hot_ltcvs( rtpv.description, mjd_now=60328., source_patch=True )
        assert df.index.get_level_values('mjd').max() < 60328.
        assert len(objdf.rootid.unique()) == 14
        assert len(df) == 310

        # Pick out five objects to ask for spectra

        chosenobjs = [ str(i) for i in objdf.rootid.unique()[ numpy.array([1, 5, 7]) ] ]

        # Ask

        res = fastdb_client.post( '/spectrum/askforspectrum',
                                  json={ 'requester': 'testing',
                                         'objectids': chosenobjs,
                                         'priorities': [3, 5, 2] } )
        assert isinstance( res, dict )
        assert res['status'] == 'ok'

        with db.DB() as con:
            cursor = con.cursor( row_factory=psycopg.rows.dict_row )
            cursor.execute( "SELECT * FROM wantedspectra" )
            rows = cursor.fetchall()

        assert len(rows) == 3
        assert set( str(r['root_diaobject_id']) for r in rows ) == set( chosenobjs )
        prios = { str(r['root_diaobject_id']) : r['priority'] for r in rows }
        assert prios[ chosenobjs[0] ] == 3
        assert prios[ chosenobjs[1] ] == 5
        assert prios[ chosenobjs[2] ] == 2

        assert all( r['requester'] == 'testing' for r in rows )
        now = datetime.datetime.now( tz=datetime.UTC )
        before = now - datetime.timedelta( minutes=10 )
        assert all( r['wanttime'] < now for r in rows )
        assert all( r['wanttime'] > before for r in rows )

        # Make sure that if the same requester asks again, priorities are updated, not added to the list

        later = datetime.datetime.now( tz=datetime.UTC )

        res = fastdb_client.post( '/spectrum/askforspectrum',
                                  json={ 'requester': 'testing',
                                         'objectids': [ chosenobjs[0] ],
                                         'priorities' : [ 1 ] } )
        assert res['status'] == 'ok'

        with db.DB() as con:
            cursor = con.cursor( row_factory=psycopg.rows.dict_row )
            cursor.execute( "SELECT * FROM wantedspectra" )
            rows = cursor.fetchall()

        assert len(rows) == 3
        assert set( str(r['root_diaobject_id']) for r in rows ) == set( chosenobjs )
        prios = { str(r['root_diaobject_id']) : r['priority'] for r in rows }
        wanttimes = { str(r['root_diaobject_id']) : r['wanttime'] for r in rows }
        assert prios[ chosenobjs[0] ] == 1
        assert prios[ chosenobjs[1] ] == 5
        assert prios[ chosenobjs[2] ] == 2
        assert all( r['requester'] == 'testing' for r in rows )

        evenlater = datetime.datetime.now( tz=datetime.UTC )

        assert wanttimes[ chosenobjs[0] ] > later
        assert wanttimes[ chosenobjs[0] ] < evenlater
        assert all( wanttimes[ chosenobjs[i] ] < now for i in ( 1, 2 ) )
        assert all( wanttimes[ chosenobjs[i] ] > before for i in ( 1, 2 ) )

    finally:
        with db.DB() as con:
            cursor = con.cursor()
            cursor.execute( "DELETE FROM wantedspectra" )
            con.commit()


def test_get_wanted_spectra( setup_wanted_spectra_etc, fastdb_client ):
    mjdnow, _now, idmap = setup_wanted_spectra_etc

    # Test 1 : If we pass nothing (except for mjd_now, which we need
    #   for the test), we should get all spectra ever requested that
    #   have not been claimed in the last 7 days, that have no
    #   observed spectra in the last 7 days, and that have been detected
    #   in the last 14 days.  That should throw out 1696949 and 191776
    #   (both requested in the last 7 days), as well as 1747042 and
    #   1173200 (neither detected in the last 14 days), leaving only 1981540.
    # 1981540 only has one requester, so there should only be one entry
    #   in the resutant list.

    res = fastdb_client.post( '/spectrum/spectrawanted', json={ 'mjd_now': mjdnow } )
    assert isinstance( res, dict )
    assert res['status'] == 'ok'
    assert len( res['wantedspectra'] ) == 1
    assert str( res['wantedspectra'][0]['root_diaobject_id'] ) == str( idmap[1981540] )

    # Test 2 : set a bunch of filters to None to see if we get everything
    # We should get back *6* responses.  Five objects, but one is requested
    #   by two different requesters.
    res = fastdb_client.post( '/spectrum/spectrawanted', json={ 'mjd_now': mjdnow,
                                                                'not_claimed_in_last_days': None,
                                                                'detected_since_mjd': None,
                                                                'no_spectra_in_last_days': None } )
    assert len( res['wantedspectra'] ) == 6
    assert set( r['requester'] for r in res['wantedspectra'] ) == { 'requester1', 'requester2' }
    assert len( set( r['root_diaobject_id'] for r in res['wantedspectra'] ) ) == 5

    # Test 3: Like last time, but set no_spectra_in_last_days to 1; shouldn't change the result
    res = fastdb_client.post( '/spectrum/spectrawanted', json={ 'mjd_now': mjdnow,
                                                                'not_claimed_in_last_days': None,
                                                                'detected_since_mjd': None,
                                                                'no_spectra_in_last_days': 1 } )
    assert len( res['wantedspectra'] ) == 6
    assert set( r['requester'] for r in res['wantedspectra'] ) == { 'requester1', 'requester2' }
    assert len( set( r['root_diaobject_id'] for r in res['wantedspectra'] ) ) == 5

    # Test 4: Now no_spectra_in_last_days is 3, should filter out 191776
    res = fastdb_client.post( '/spectrum/spectrawanted', json={ 'mjd_now': mjdnow,
                                                                'not_claimed_in_last_days': None,
                                                                'detected_since_mjd': None,
                                                                'no_spectra_in_last_days': 3 } )
    assert len( res['wantedspectra'] ) == 5
    assert set( r['requester'] for r in res['wantedspectra'] ) == { 'requester1', 'requester2' }
    assert len( set( r['root_diaobject_id'] for r in res['wantedspectra'] ) ) == 4
    assert str( idmap[191776] ) not in [ r['root_diaobject_id'] for r in res['wantedspectra'] ]

    # Test 5: no_spectra_in_last_days defaults to 7, filters out 191776 again
    res = fastdb_client.post( '/spectrum/spectrawanted', json={ 'mjd_now': mjdnow,
                                                                'not_claimed_in_last_days': None,
                                                                'detected_since_mjd': None } )
    assert len( res['wantedspectra'] ) == 5
    assert set( r['requester'] for r in res['wantedspectra'] ) == { 'requester1', 'requester2' }
    assert len( set( r['root_diaobject_id'] for r in res['wantedspectra'] ) ) == 4
    assert str( idmap[191776] ) not in [ r['root_diaobject_id'] for r in res['wantedspectra'] ]

    # Test 6: using only the detected_since_mjd test, put in 60330, should filter out
    #   1173200 -- which is the one requested by requester2
    res = fastdb_client.post( '/spectrum/spectrawanted', json={ 'mjd_now': mjdnow,
                                                                'not_claimed_in_last_days': None,
                                                                'detected_since_mjd': 60330.,
                                                                'no_spectra_in_last_days': None } )
    assert len( res['wantedspectra'] ) == 4
    assert all( r['requester'] == 'requester1' for r in res['wantedspectra'] )
    assert set( r['root_diaobject_id'] for r in res['wantedspectra'] ) == { str(idmap[i]) for i in
                                                                            [ 1696949, 1981540, 191776, 1747042 ] }


    # Test 7: detected_in_last_days = 15 should throw out 1747042 and 1173200
    res = fastdb_client.post( '/spectrum/spectrawanted', json={ 'mjd_now': mjdnow,
                                                                'not_claimed_in_last_days': None,
                                                                'detected_in_last_days': 15,
                                                                'no_spectra_in_last_days': None } )
    assert len( res['wantedspectra'] ) == 3
    assert all( r['requester'] == 'requester1' for r in res['wantedspectra'] )
    assert set( r['root_diaobject_id'] for r in res['wantedspectra'] ) == { str(idmap[i]) for i in
                                                                            [ 1696949, 1981540, 191776 ] }

    # Test 8: passing both detected_in_last_days and detected_since_mjd should ignore ..._last_days
    res = fastdb_client.post( '/spectrum/spectrawanted', json={ 'mjd_now': mjdnow,
                                                                'not_claimed_in_last_days': None,
                                                                'detected_since_mjd': 60330.,
                                                                'detected_in_last_days': 15,
                                                                'no_spectra_in_last_days': None } )
    assert len( res['wantedspectra'] ) == 4
    assert all( r['requester'] == 'requester1' for r in res['wantedspectra'] )
    assert set( r['root_diaobject_id'] for r in res['wantedspectra'] ) == { str(idmap[i]) for i in
                                                                            [ 1696949, 1981540, 191776, 1747042 ] }

    # Test 10 and 11: check requester
    res = fastdb_client.post( '/spectrum/spectrawanted', json={ 'mjd_now': mjdnow,
                                                                'requester': 'requester1',
                                                                'not_claimed_in_last_days': None,
                                                                'detected_since_mjd': None,
                                                                'no_spectra_in_last_days': None } )
    assert len( res['wantedspectra'] ) == 5
    assert all( r['requester'] == 'requester1' for r in res['wantedspectra'] )
    assert len( set( r['root_diaobject_id'] for r in res['wantedspectra'] ) ) == 5

    res = fastdb_client.post( '/spectrum/spectrawanted', json={ 'mjd_now': mjdnow,
                                                                'requester': 'requester2',
                                                                'not_claimed_in_last_days': None,
                                                                'detected_since_mjd': None,
                                                                'no_spectra_in_last_days': None } )
    assert len( res['wantedspectra'] ) == 1
    assert res['wantedspectra'][0]['requester'] == 'requester2'
    assert res['wantedspectra'][0]['root_diaobject_id'] == str( idmap[1173200] )

    # Test 12: lim_mag = 23.0 should throw out 1173200 and 1747042
    res = fastdb_client.post( '/spectrum/spectrawanted', json={ 'mjd_now': mjdnow,
                                                                'not_claimed_in_last_days': None,
                                                                'detected_since_mjd': None,
                                                                'no_spectra_in_last_days': None,
                                                                'lim_mag': 23. } )
    assert len( res['wantedspectra'] ) == 3
    assert len( set( r['root_diaobject_id'] for r in res['wantedspectra'] ) ) == 3
    assert str(idmap[1696949]) in [ r['root_diaobject_id'] for r in res['wantedspectra'] ]
    assert str(idmap[1173200]) not in [ r['root_diaobject_id'] for r in res['wantedspectra'] ]
    assert str(idmap[1747042]) not in [ r['root_diaobject_id'] for r in res['wantedspectra'] ]

    # Test 13: lim_mag = 23.0 and lim_mag_band='r' should throw out 1981540 and 1173200
    res = fastdb_client.post( '/spectrum/spectrawanted', json={ 'mjd_now': mjdnow,
                                                                'not_claimed_in_last_days': None,
                                                                'detected_since_mjd': None,
                                                                'no_spectra_in_last_days': None,
                                                                'lim_mag': 23.3,
                                                                'lim_mag_band': 'r'} )
    assert len( res['wantedspectra'] ) == 3
    assert len( set( r['root_diaobject_id'] for r in res['wantedspectra'] ) ) == 3
    assert str(idmap[1696949]) in [ r['root_diaobject_id'] for r in res['wantedspectra'] ]
    assert str(idmap[1173200]) not in [ r['root_diaobject_id'] for r in res['wantedspectra'] ]
    assert str(idmap[1981540]) not in [ r['root_diaobject_id'] for r in res['wantedspectra'] ]


def test_plan_spectrum( setup_wanted_spectra_etc, fastdb_client ):
    _mjdnow, _now, idmap = setup_wanted_spectra_etc

    # There are three planned spectra in the database from the fixture.
    # Add another, see if it goes.

    res = fastdb_client.post( '/spectrum/planspectrum',
                              json={ 'root_diaobject_id': str(idmap[1747042]),
                                     'facility': 'Second test facility',
                                     'plantime': '2031-12-13 02:00:00'
                                    } )
    assert isinstance( res, dict )
    assert res['status'] == 'ok'

    with db.DB() as con:
        cursor = con.cursor( row_factory=psycopg.rows.dict_row )
        cursor.execute( "SELECT * FROM plannedspectra" )
        rows = cursor.fetchall()

    assert len(rows) == 4
    assert set( str(r['root_diaobject_id']) for r in rows ) == { str(idmap[i]) for i in ( 1747042, 1696949, 191776 ) }
    assert len( [ r for r in rows if r['root_diaobject_id'] == idmap[1747042] ] ) == 2
    assert set( r['facility'] for r in rows ) == { 'test facility', 'Second test facility' }


def test_remove_spectrum_plan( setup_wanted_spectra_etc, fastdb_client ):
    _mjdnow, _now, idmap = setup_wanted_spectra_etc

    res = fastdb_client.post( '/spectrum/planspectrum',
                              json={ 'root_diaobject_id': str(idmap[1747042]),
                                     'facility': 'Second test facility',
                                     'plantime': '2031-12-13 02:00:00'
                                    } )

    res = fastdb_client.post( 'spectrum/removespectrumplan', json={ 'root_diaobject_id': str(idmap[1747042]),
                                                                    'facility': 'test facility' } )
    assert res['status'] == 'ok'
    assert res['ndel'] == 1

    with db.DB() as con:
        cursor = con.cursor( row_factory=psycopg.rows.dict_row )
        cursor.execute( "SELECT * FROM plannedspectra" )
        rows = cursor.fetchall()

    assert len(rows) == 3
    assert set( str(r['root_diaobject_id']) for r in rows ) == { str(idmap[i]) for i in ( 1747042, 1696949, 191776 ) }
    assert [ r['facility'] for r in rows if r['root_diaobject_id'] == idmap[1747042] ] == [ 'Second test facility' ]
    assert set( r['facility'] for r in rows ) == { 'test facility', 'Second test facility' }


def test_report_spectrum_info( setup_wanted_spectra_etc, fastdb_client ):
    _mjdnow, _now, idmap = setup_wanted_spectra_etc

    res = fastdb_client.post( '/spectrum/reportspectruminfo',
                              json={ 'root_diaobject_id': str( idmap[1747042] ),
                                     'facility': "Rob's C8 in his back yard",
                                     'mjd': 60364.128,
                                     'z': 1.36,
                                     'classid': 2232 } )
    assert res['status'] == 'ok'

    with db.DB() as con:
        cursor = con.cursor( row_factory=psycopg.rows.dict_row )
        cursor.execute( "SELECT * FROM spectruminfo" )
        rows = cursor.fetchall()

    # There was one pre-existing one from the fixture
    assert len(rows) == 2
    r = [ row for row in rows if row['root_diaobject_id']==idmap[1747042] ][0]
    assert r['facility'] == "Rob's C8 in his back yard"
    # Note that the mjd column in the spectruminfo table is only a real, so only has 24 bits of precision
    assert r['mjd'] == pytest.approx( 60364.13, abs=0.01 )
    assert r['z'] == pytest.approx( 1.36, abs=0.01 )
    assert r['classid'] == 2232


def test_get_known_spectrum_info( setup_spectrum_info, fastdb_client):
    mjdnow, now, idmap = setup_spectrum_info

    # Get them all
    res = fastdb_client.post( "/spectrum/getknownspectruminfo", json={} )
    assert isinstance( res, list )
    assert len(res) == 4
    assert set( r['root_diaobject_id'] for r in res ) == set( str(idmap[i]) for i in ( 191776, 1173200 ) )
    for r in res:
        if r['root_diaobject_id'] == str( idmap[191776] ):
            assert r['classid'] == 2342 if r['facility'] == "Rob's C8 in his back yard" else 2222
        else:
            assert r['classid'] == 2322 if r['facility'] == "Galileo's Telescope" else 2235

    # Get only the ones from test facility
    res = fastdb_client.post( "/spectrum/getknownspectruminfo", json={ 'facility': 'test facility' } )
    assert len(res) == 2
    assert set( r['root_diaobject_id'] for r in res ) == set( str(idmap[i]) for i in ( 191776, 1173200 ) )
    assert set( r['classid'] for r in res ) == { 2222, 2235 }

    # Test filtering by root_diaobject_id
    res = fastdb_client.post( "/spectrum/getknownspectruminfo", json={ 'root_diaobject_ids': str(idmap[191776]) } )
    assert all( r['root_diaobject_id'] == str(idmap[191776]) for r in res )
    assert set( r['facility'] for r in res ) == { "test facility", "Rob's C8 in his back yard" }

    res = fastdb_client.post( "/spectrum/getknownspectruminfo",
                              json={ 'root_diaobject_ids': [ str(idmap[191776]),
                                                             'e7cb3c55-6679-4e4f-8e36-d2c6eab8faa1' ] } )
    assert all( r['root_diaobject_id'] == str(idmap[191776]) for r in res )
    assert set( r['facility'] for r in res ) == { "test facility", "Rob's C8 in his back yard" }

    res = fastdb_client.post( "/spectrum/getknownspectruminfo", json={ 'root_diaobject_ids': [ str(idmap[191776]),
                                                                                               str(idmap[1173200]) ] } )
    assert len(res) == 4
    assert set( r['root_diaobject_id'] for r in res ) == set( str(idmap[i]) for i in ( 191776, 1173200 ) )
    for r in res:
        if r['root_diaobject_id'] == str( idmap[191776] ):
            assert r['classid'] == 2342 if r['facility'] == "Rob's C8 in his back yard" else 2222
        else:
            assert r['classid'] == 2322 if r['facility'] == "Galileo's Telescope" else 2235

    # Test filtering by mjd
    res = fastdb_client.post( "/spectrum/getknownspectruminfo", json={ 'mjd_min': mjdnow-5 } )
    assert len(res) ==2
    assert set( r['root_diaobject_id'] for r in res ) == set( str(idmap[i]) for i in ( 191776, 1173200 ) )
    assert set( r['facility'] for r in res ) == {  "test facility", "Galileo's Telescope" }
    assert set( r['z'] for r in res ) == { 0.005, 0.25 }

    res = fastdb_client.post( "/spectrum/getknownspectruminfo", json={ 'mjd_max': mjdnow-5 } )
    assert len(res) ==2
    assert set( r['root_diaobject_id'] for r in res ) == set( str(idmap[i]) for i in ( 191776, 1173200 ) )
    assert set( r['facility'] for r in res ) == {  "test facility", "Rob's C8 in his back yard" }
    assert set( r['z'] for r in res ) == { 0.12, 1.25 }


    res = fastdb_client.post( "/spectrum/getknownspectruminfo", json={ 'mjd_min': mjdnow-15,
                                                                       'mjd_max': mjdnow-5 } )
    assert len(res) == 1
    assert res[0]['root_diaobject_id'] == str( idmap[191776] )
    assert res[0]['facility'] == "Rob's C8 in his back yard"
    assert res[0]['classid'] == 2342
    assert res[0]['z'] == 1.25

    # Test filtering by classid

    res = fastdb_client.post( "/spectrum/getknownspectruminfo", json={ 'classid': 2342 } )
    assert len(res) == 1
    assert res[0]['root_diaobject_id'] == str( idmap[191776] )
    assert res[0]['facility'] == "Rob's C8 in his back yard"
    assert res[0]['classid'] == 2342
    assert res[0]['z'] == 1.25

    res = fastdb_client.post( "/spectrum/getknownspectruminfo", json={ 'classid': 42 } )
    res == []

    # Test filtering by z
    res = fastdb_client.post( "/spectrum/getknownspectruminfo", json={ 'z_min': 0.2 } )
    assert len(res) == 2
    assert all( r['root_diaobject_id'] == str(idmap[191776]) for r in res )
    assert set( r['facility'] for r in res ) == { 'test facility', "Rob's C8 in his back yard" }

    res = fastdb_client.post( "/spectrum/getknownspectruminfo", json={ 'z_max': 0.01 } )
    assert len(res) == 1
    assert res[0]['root_diaobject_id'] == str( idmap[1173200] )
    assert res[0]['facility'] == "Galileo's Telescope"
    assert res[0]['z'] == 0.005
    assert res[0]['classid'] == 2322

    res = fastdb_client.post( "/spectrum/getknownspectruminfo", json={ 'z_min': 0.1, 'z_max': 0.2 } )
    assert len(res) == 1
    assert res[0]['root_diaobject_id'] == str( idmap[1173200] )
    assert res[0]['facility'] == "test facility"
    assert res[0]['z'] == 0.12
    assert res[0]['classid'] == 2235

    # Test filtering by since
    res = fastdb_client.post( "/spectrum/getknownspectruminfo",
                              json={ 'since': ( now - datetime.timedelta(days=5) ).isoformat() } )
    assert len(res) == 2
    assert set( r['root_diaobject_id'] for r in res ) == set( str(idmap[i]) for i in ( 191776, 1173200 ) )
    assert set( r['facility'] for r in res ) == { "test facility", "Galileo's Telescope" }
    assert set( r['classid'] for r in res ) == { 2222, 2322 }
