import pytest
import datetime
import pytz
import uuid
import numpy

import astropy.time

import ltcv
# import spectrum
import db


def _get_test_object_maps( con=None ):
    with db.DBCon( con ) as con:
        # I am shortcutting this next query and not dealing with procver, etc., because
        # I know the snana import fixture only has one processing version for everything.
        rows, _cols = con.execute( "SELECT o.rootid,o.diaobjectid,p.ra,p.dec "
                                   "FROM diaobject o "
                                   "INNER JOIN diaobject_position p ON p.diaobjectid=o.diaobjectid "
                                   "WHERE o.diaobjectid=ANY(%(obj)s)",
                                   { 'obj': [ 1696949, 1186717, 191776, 1747042, 1173200 ]} )
        idmap = { r[1]: r[0] for r in rows }
        ramap = { r[1]: r[2] for r in rows }
        decmap = { r[1]: r[3] for r in rows }
        assert len(idmap) == 5

    return idmap, ramap, decmap


@pytest.fixture
def setup_wanted_spectra_etc( alerts_90days_sent_received_and_imported, test_user ):
    # Prime the database with some wanted spectra
    #
    # To find objects to use in this test, I ran this query:
    #
    # SELECT o.diaobjectid, ns.num AS nsrc, nf.num AS nfrc,
    #        ROUND(CAST(s.maxmjd AS numeric),2) AS srcmjd,
    #        s.maxband AS srcband,
    #        ROUND(CAST( CASE WHEN s.maxflux<=0 THEN 99.99 ELSE -2.5*LOG(s.maxflux)+31.4 END AS numeric),2) AS srcmag,
    #        ROUND(CAST(f.maxmjd AS numeric),2) AS frcmjd,
    #        f.maxband AS frcband,
    #        ROUND(CAST( CASE WHEN f.maxflux<=0 THEN 99.99 ELSE -2.5*LOG(f.maxflux)+31.4 END AS numeric),2) AS frcmag
    # FROM diaobject o
    # INNER JOIN
    #   ( SELECT DISTINCT ON( diaobjectid ) diaobjectid, midpointmjdtai AS maxmjd,
    #        band AS maxband, psfflux AS maxflux
    #     FROM diasource
    #     WHERE midpointmjdtai <= 60362.5 AND band='r'
    #     ORDER BY diaobjectid, midpointmjdtai DESC
    #   ) s ON s.diaobjectid=o.diaobjectid
    # INNER JOIN
    #   ( SELECT DISTINCT ON( diaobjectid ) diaobjectid, midpointmjdtai AS maxmjd,
    #       band AS maxband, psfflux AS maxflux
    #       FROM diaforcedsource
    #       WHERE midpointmjdtai < 60362.5 AND band='r'
    #       ORDER BY diaobjectid, midpointmjdtai DESC
    #    ) f ON f.diaobjectid=o.diaobjectid
    # INNER JOIN
    #   ( SELECT DISTINCT ON( diaobjectid ) diaobjectid, COUNT(diasourceid) AS num
    #     FROM diasource
    #     GROUP BY diaobjectid
    #   ) ns ON ns.diaobjectid=o.diaobjectid
    # INNER JOIN
    #   ( SELECT DISTINCT ON( diaobjectid ) diaobjectid, COUNT(diaforcedsourceid) AS num
    #     FROM diaforcedsource
    #     GROUP BY diaobjectid
    #   ) nf ON nf.diaobjectid=o.diaobjectid
    # ORDER BY s.maxmjd DESC;
    #
    # Being cavalier about processing versions becasue we know there is only one from the snana fixture.
    # Remove the two "AND band='r'" to get the latest forced and source for any band.
    #
    # Some objects of interest:
    #    1696949 — 5 detections, 5 forced
    #                  last forced r = 60359.35 (21.48), last forced = 60359.36 (i, 21.49)
    #                  last source r = 60359.35 (21.48), last source = 60362.33 (z, 21.36)
    #    1186717 — 6 detections, 11 forced
    #                  last forced r = 60353.37 (23.30), last forced = 60353.37 (r, 23.30)
    #                  last source r = 60348.35 (23.27), last source = 60358.32 (i, 23.37)
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
        with db.DBCon() as con:
            idmap, ramap, decmap = _get_test_object_maps( con )

            # requester1 has asked for all five
            q = ( "INSERT INTO wantedspectra(wantspec_id,root_diaobject_id,wanttime,user_id,"
                  "                          requester,is_host,ra,dec,priority) "
                  "VALUES (%(wid)s,%(rid)s,%(t)s,%(uid)s,%(req)s,%(is_host)s,%(ra)s,%(dec)s,%(prio)s)" )
            con.execute( q,
                         { 'wid': uuid.uuid4(),
                           'rid': idmap[1696949],
                           't': now - datetime.timedelta( minutes=1 ),
                           'uid': test_user.id,
                           'req': 'requester1',
                           'is_host': False,
                           'ra': ramap[1696949],
                           'dec': decmap[1696949],
                           'prio': 3 } )
            con.execute( q,
                         { 'wid': uuid.uuid4(),
                           'rid': idmap[1186717],
                           't': now - datetime.timedelta( days=1 ),
                           'uid': test_user.id,
                           'req': 'requester1',
                           'is_host': True,
                           'ra': ramap[1186717],
                           'dec': decmap[1186717],
                           'prio': 4 } )
            con.execute( q,
                         { 'wid': uuid.uuid4(),
                           'rid': idmap[191776],
                           't': now - datetime.timedelta( days=5 ),
                           'uid': test_user.id,
                           'req': 'requester1',
                           'is_host': True,
                           'ra': ramap[191776],
                           'dec': decmap[191776],
                           'prio': 2 } )
            con.execute( q,
                         { 'wid': uuid.uuid4(),
                           'rid': idmap[1747042],
                           't': now - datetime.timedelta( days=10 ),
                           'uid': test_user.id,
                           'req': 'requester1',
                           'is_host': False,
                           'ra': ramap[1747042],
                           'dec': decmap[1747042],
                           'prio': 1 } )
            con.execute( q,
                         { 'wid': uuid.uuid4(),
                           'rid': idmap[1173200],
                           't': now - datetime.timedelta( days=40 ),
                           'uid': test_user.id,
                           'req': 'requester1',
                           'is_host': False,
                           'ra': ramap[1173200],
                           'dec': decmap[1173200],
                           'prio': 5 } )
            # requester2 very recently asked for a spectrum of a source that requester1 asked for a long time ago
            con.execute( q,
                         { 'wid': uuid.uuid4(),
                           'rid': idmap[1173200],
                           't': now - datetime.timedelta( days=1 ),
                           'uid': test_user.id,
                           'req': 'requester2',
                           'is_host': False,
                           'ra': ramap[1173200],
                           'dec': decmap[1173200],
                           'prio': 5 } )

            # Put in a couple of spectrum claims
            q = ( "INSERT INTO plannedspectra(plannedspec_id,root_diaobject_id,is_host,facility,created_at,plantime) "
                  "VALUES (%(pid)s,%(rid)s,%(ih)s,%(fac)s,%(ct)s,%(pt)s)" )
            con.execute( q,
                         { 'pid': uuid.uuid4(),
                           'rid': idmap[1747042],
                           'ih': False,
                           'fac': 'test facility',
                           'ct': now - datetime.timedelta( days=9 ),
                           'pt': now - datetime.timedelta( days=8 )
                          } )
            con.execute( q,
                         { 'pid': uuid.uuid4(),
                           'rid': idmap[1696949],
                           'ih': False,
                           'fac': 'test facility',
                           'ct': now,
                           'pt': now + datetime.timedelta( days=1 )
                          } )
            con.execute( q,
                         { 'pid': uuid.uuid4(),
                           'rid': idmap[191776],
                           'ih': False,
                           'fac': 'test facility',
                           'ct': now - datetime.timedelta( days=4 ),
                           'pt': now - datetime.timedelta( days=3 )
                          } )
            con.execute( q,
                         { 'pid': uuid.uuid4(),
                           'rid': idmap[1747042],
                           'ih': True,
                           'fac': 'test facility 2',
                           'ct': now - datetime.timedelta( days=9 ),
                           'pt': now - datetime.timedelta( days=8 )
                          } )

            # One of the planned spectra was observed
            con.execute( "INSERT INTO spectruminfo(specinfo_id,root_diaobject_id,facility,inserted_at,"
                         "                         mjd,z,classid,ra,dec,is_host) "
                         "VALUES (%(sid)s,%(rid)s,%(fac)s,%(t)s,%(mjd)s,%(z)s,%(class)s,%(ra)s,%(dec)s,%(ishost)s)",
                         { 'sid': uuid.uuid4(),
                           'rid': idmap[191776],
                           'fac': 'test facility',
                           't': now - datetime.timedelta( days=1 ),
                           'mjd': mjdnow - 2,
                           'z': 0.25,
                           'class': 2222,
                           'ra': ramap[191776],
                           'dec': decmap[191776],
                           'ishost': True
                          } )

            con.commit()

        yield mjdnow, now, idmap, ramap, decmap

    finally:
        with db.DBCon() as con:
            con.execute( "DELETE FROM spectruminfo" )
            con.execute( "DELETE FROM plannedspectra" )
            con.execute( "DELETE FROM wantedspectra" )
            con.commit()


@pytest.fixture
def setup_spectrum_info( setup_wanted_spectra_etc ):
    mjdnow, now, idmap, ramap, decmap = setup_wanted_spectra_etc

    # The previous fixture adds one.  Let's add more.

    with db.DBCon() as con:
        q = ( "INSERT INTO spectruminfo(specinfo_id,root_diaobject_id,facility,inserted_at,"
              "                         mjd,z,classid,class_description,ra,dec,is_host) "
              "VALUES (%(sid)s,%(rid)s,%(fac)s,%(t)s,%(mjd)s,%(z)s,%(class)s,%(desc)s,%(ra)s,%(dec)s,%(ishost)s)" )
        con.execute(q,
                        { 'sid': uuid.uuid4(),
                          'rid': idmap[1173200],
                          'fac': 'test facility',
                          't': now - datetime.timedelta( days=25 ),
                          'mjd': mjdnow - 24,
                          'z': 0.12,
                          'class': 2235,
                          'desc': "Microlens",
                          'ra': ramap[1173200],
                          'dec': decmap[1173200],
                          'ishost': False } )

        con.execute( q,
                     { 'sid': uuid.uuid4(),
                       'rid': idmap[1173200],
                       'fac': "Galileo's Telescope",
                       't': now - datetime.timedelta( days=2 ),
                       'mjd': mjdnow - 3,
                       'z': 0.005,
                       'class': 2322,
                       'desc': "Cepheid",
                       'ra': ramap[1173200],
                       'dec': decmap[1173200],
                       'ishost': False } )

        con.execute( q,
                     { 'sid': uuid.uuid4(),
                       'rid': idmap[191776],
                       'fac': "Rob's C8 in his back yard",
                       't': now - datetime.timedelta( days=10 ),
                       'mjd': mjdnow - 14,
                       'z': 1.25,
                       'class': 2342,
                       'desc': "δ Scuti",
                       'ra': ramap[191776],
                       'dec': decmap[191776],
                       'ishost': False } )

        con.commit()

    return mjdnow, now, idmap, ramap, decmap
    # Don't have to clean up, parent fixture will do that



def test_ask_for_spectra( procver_collection, alerts_90days_sent_received_and_imported, fastdb_client, test_user ):
    _bpvs, pvs, _pvinfo = procver_collection
    rtpv = pvs['realtime']
    try:
        # Get some hot lightcurves
        df, objdf = ltcv.get_hot_ltcvs( rtpv.description, mjd_now=60328., source_patch=True, return_format='pandas' )
        assert df.index.get_level_values('mjd').max() < 60328.
        assert len(objdf.rootid.unique()) == 13
        assert len(df) == 294

        # Pick out three objects to ask for spectra.
        # NOTE.  I'm being cavalier here.  In reality, objdf could have multiple rows for
        #   the same rootid.  But, for the loaded SNANA set, I know that won't happen.

        objdex = numpy.array([1, 5, 7])
        chosenids = [ str(objdf.iloc[i].rootid) for i in objdex ]
        chosenras = [ objdf.iloc[i].ra for i in objdex ]
        chosendecs = [ objdf.iloc[i].dec for i in objdex ]
        chosenishosts = [ False, True, False ]
        chosenprios = [ 3, 5, 2 ]

        queryjson = { 'requester': 'testing',
                      'rootids': chosenids,
                      'ras': chosenras,
                      'decs': chosendecs,
                      'is_hosts': chosenishosts,
                      'priorities': chosenprios }

        # Test failure modes
        for oops in [ 'requester', 'rootids', 'priorities', 'ras', 'decs' ]:
            json = queryjson.copy()
            del json[ oops ]
            with pytest.raises( RuntimeError, match=( f"Error response from server, status 422: "
                                                      f"Missing required fields: {{'{oops}'}}" )
                               ):
                fastdb_client.post( '/spectrum/askforspectrum', json=json )

        # Ask

        res = fastdb_client.post( '/spectrum/askforspectrum', json=queryjson )
        assert isinstance( res, dict )
        assert res['status'] == 'ok'

        with db.DBCon( dictcursor=True ) as con:
            rows = con.execute( "SELECT * FROM wantedspectra" )

        assert len(rows) == 3
        assert set( str(r['root_diaobject_id']) for r in rows ) == set( chosenids )
        for field, comp in zip( [ 'priority', 'is_host', 'ra', 'dec' ],
                                [ chosenprios, chosenishosts, chosenras, chosendecs ] ):
            vals = { str(r['root_diaobject_id']) : r[field] for r in rows }
            assert all( vals[ chosenids[i] ] == comp[i] for i in range( len(chosenids) ) )

        assert all( r['requester'] == 'testing' for r in rows )
        assert all( r['user_id'] == test_user.id for r in rows )
        now = datetime.datetime.now( tz=datetime.UTC )
        before = now - datetime.timedelta( minutes=10 )
        assert all( r['wanttime'] < now for r in rows )
        assert all( r['wanttime'] > before for r in rows )

        # Make sure that if the same requester asks again, priorities are updated, not added to the list
        # (Also, incidentally test passing a scalar instead of a list.)

        later = datetime.datetime.now( tz=datetime.UTC )

        res = fastdb_client.post( '/spectrum/askforspectrum',
                                  json={ 'requester': 'testing',
                                         'rootids': chosenids[0],
                                         'ras': chosenras[0],
                                         'decs': chosendecs[0],
                                         'is_hosts': chosenishosts[0],
                                         'priorities' : 1 } )

        assert res['status'] == 'ok'

        with db.DBCon( dictcursor=True ) as con:
            rows = con.execute( "SELECT * FROM wantedspectra" )

        assert len(rows) == 3
        assert set( str(r['root_diaobject_id']) for r in rows ) == set( chosenids )
        for field, comp in zip( [ 'priority', 'is_host', 'ra', 'dec' ],
                                [ chosenprios, chosenishosts, chosenras, chosendecs ] ):
            vals = { str(r['root_diaobject_id']) : r[field] for r in rows }
            if field == 'priority':
                assert vals[ chosenids[0] ] == 1
                assert all( vals[ chosenids[i] ] == comp[i] for i in range(1, len(chosenids) ) )
            else:
                assert all( vals[ chosenids[i] ] == comp[i] for i in range( len(chosenids) ) )

        assert all( r['requester'] == 'testing' for r in rows )
        assert all( r['user_id'] == test_user.id for r in rows )

        evenlater = datetime.datetime.now( tz=datetime.UTC )
        wanttimes = { str(r['root_diaobject_id']) : r['wanttime'] for r in rows }
        assert wanttimes[ chosenids[0] ] > later
        assert wanttimes[ chosenids[0] ] < evenlater
        assert all( wanttimes[ chosenids[i] ] < now for i in ( 1, 2 ) )
        assert all( wanttimes[ chosenids[i] ] > before for i in ( 1, 2 ) )

        # TODO test differing is_host

    finally:
        with db.DBCon() as con:
            con.execute( "DELETE FROM wantedspectra" )
            con.commit()


def test_get_wanted_spectra( setup_wanted_spectra_etc, fastdb_client ):
    # TODO : is_host was added after most of these tests were written.
    # They were adapted, but think about whether we need more tests for
    # it.

    mjdnow, _now, idmap, ramap, decmap = setup_wanted_spectra_etc

    # Test 1 : If we pass nothing (except for mjd_now, which we need for
    #   the test), we should get all spectra ever requested that have
    #   not been claimed in the last 7 days, that have no observed
    #   spectra in the last 7 days, and that have been detected in the
    #   last 14 days.  That should throw out 1696949 (claimed in the
    #   last 7 days), as well as 1747042 and 1173200 (neither detected
    #   in the last 14 days), as wella s 191776 (requested and observed
    #   in the last 7 days with is_host=True), leaving only 1186717,
    #   which has only one requester.

    res = fastdb_client.post( '/spectrum/spectrawanted', json={ 'mjd_now': mjdnow } )
    assert isinstance( res, dict )
    assert res['status'] == 'ok'
    assert len( res['wantedspectra'] ) == 1
    assert res['wantedspectra'][0]['root_diaobject_id'] == str( idmap[1186717] )

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
                                                                            [ 1696949, 1186717, 191776, 1747042 ] }


    # Test 7: detected_in_last_days = 15 should throw out 1747042 and 1173200
    res = fastdb_client.post( '/spectrum/spectrawanted', json={ 'mjd_now': mjdnow,
                                                                'not_claimed_in_last_days': None,
                                                                'detected_in_last_days': 15,
                                                                'no_spectra_in_last_days': None } )
    assert len( res['wantedspectra'] ) == 3
    assert all( r['requester'] == 'requester1' for r in res['wantedspectra'] )
    assert set( r['root_diaobject_id'] for r in res['wantedspectra'] ) == { str(idmap[i]) for i in
                                                                            [ 1696949, 1186717, 191776 ] }

    # Test 8: passing both detected_in_last_days and detected_since_mjd should ignore ..._last_days
    res = fastdb_client.post( '/spectrum/spectrawanted', json={ 'mjd_now': mjdnow,
                                                                'not_claimed_in_last_days': None,
                                                                'detected_since_mjd': 60330.,
                                                                'detected_in_last_days': 15,
                                                                'no_spectra_in_last_days': None } )
    assert len( res['wantedspectra'] ) == 4
    assert all( r['requester'] == 'requester1' for r in res['wantedspectra'] )
    assert set( r['root_diaobject_id'] for r in res['wantedspectra'] ) == { str(idmap[i]) for i in
                                                                            [ 1696949, 1186717, 191776, 1747042 ] }

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

    # Test 12: lim_mag = 23.0 should throw out 1186717, 1747042, 1173200
    #
    # Do it straight (i.e. not through the webap) so I can debug
    # import pdb; pdb.set_trace()
    # df = spectrum.what_spectra_are_wanted( 'realtime', mjdnow=60362.5, notclaimsince=None, detsince=None,
    #                                        nospecsince=None, lim_mag=23. )

    res = fastdb_client.post( '/spectrum/spectrawanted', json={ 'mjd_now': mjdnow,
                                                                'not_claimed_in_last_days': None,
                                                                'detected_since_mjd': None,
                                                                'no_spectra_in_last_days': None,
                                                                'lim_mag': 23. } )
    assert len( res['wantedspectra'] ) == 2
    assert len( set( r['root_diaobject_id'] for r in res['wantedspectra'] ) ) == 2
    assert str(idmap[1696949]) in [ r['root_diaobject_id'] for r in res['wantedspectra'] ]
    assert str(idmap[191776]) in [ r['root_diaobject_id'] for r in res['wantedspectra'] ]
    assert str(idmap[1173200]) not in [ r['root_diaobject_id'] for r in res['wantedspectra'] ]
    assert str(idmap[1747042]) not in [ r['root_diaobject_id'] for r in res['wantedspectra'] ]
    assert str(idmap[1186717]) not in [ r['root_diaobject_id'] for r in res['wantedspectra'] ]

    # Test 13: lim_mag = 23.0 and lim_mag_band='r' should throw out 1186717 and 1173200
    res = fastdb_client.post( '/spectrum/spectrawanted', json={ 'mjd_now': mjdnow,
                                                                'not_claimed_in_last_days': None,
                                                                'detected_since_mjd': None,
                                                                'no_spectra_in_last_days': None,
                                                                'lim_mag': 23.3,
                                                                'lim_mag_band': 'r'} )
    assert len( res['wantedspectra'] ) == 3
    assert len( set( r['root_diaobject_id'] for r in res['wantedspectra'] ) ) == 3
    assert str(idmap[1696949]) in [ r['root_diaobject_id'] for r in res['wantedspectra'] ]
    assert str(idmap[191776]) in [ r['root_diaobject_id'] for r in res['wantedspectra'] ]
    assert str(idmap[1747042]) in [ r['root_diaobject_id'] for r in res['wantedspectra'] ]
    assert str(idmap[1173200]) not in [ r['root_diaobject_id'] for r in res['wantedspectra'] ]
    assert str(idmap[1186717]) not in [ r['root_diaobject_id'] for r in res['wantedspectra'] ]


def test_plan_spectrum( setup_wanted_spectra_etc, fastdb_client ):
    _mjdnow, _now, idmap, _ramap, _decmap = setup_wanted_spectra_etc

    # There are three planned spectra in the database from the fixture.
    # Add another, see if it goes.

    res = fastdb_client.post( '/spectrum/planspectrum',
                              json={ 'root_diaobject_id': str(idmap[1747042]),
                                     'facility': 'Second test facility',
                                     'plantime': '2031-12-13 02:00:00'
                                    } )
    assert isinstance( res, dict )
    assert res['status'] == 'ok'

    with db.DBCon( dictcursor=True ) as con:
        rows = con.execute( "SELECT * FROM plannedspectra" )

    assert len(rows) == 5
    assert set( str(r['root_diaobject_id']) for r in rows ) == { str(idmap[i]) for i in ( 1747042, 1696949, 191776 ) }
    assert len( [ r for r in rows if r['root_diaobject_id'] == idmap[1747042] ] ) == 3
    assert set( r['facility'] for r in rows ) == { 'test facility', 'test facility 2', 'Second test facility' }


def test_remove_spectrum_plan( setup_wanted_spectra_etc, fastdb_client ):
    _mjdnow, _now, idmap, _ramap, _decmap = setup_wanted_spectra_etc

    res = fastdb_client.post( '/spectrum/planspectrum',
                              json={ 'root_diaobject_id': str(idmap[1747042]),
                                     'facility': 'Second test facility',
                                     'plantime': '2031-12-13 02:00:00'
                                    } )

    res = fastdb_client.post( 'spectrum/removespectrumplan', json={ 'root_diaobject_id': str(idmap[1747042]),
                                                                    'facility': 'test facility' } )
    assert res['status'] == 'ok'
    assert res['ndel'] == 1

    with db.DBCon( dictcursor=True ) as con:
        rows = con.execute( "SELECT * FROM plannedspectra" )

    assert len(rows) == 4
    assert set( str(r['root_diaobject_id']) for r in rows ) == { str(idmap[i]) for i in ( 1747042, 1696949, 191776 ) }
    assert ( set( r['facility'] for r in rows if r['root_diaobject_id'] == idmap[1747042] )
             == { 'test facility 2', 'Second test facility' } )
    assert set( r['facility'] for r in rows ) == { 'test facility', 'test facility 2', 'Second test facility' }


def test_report_spectrum_info( setup_wanted_spectra_etc, fastdb_client ):
    _mjdnow, _now, idmap, ramap, decmap = setup_wanted_spectra_etc

    res = fastdb_client.post( '/spectrum/reportspectruminfo',
                              json={ 'root_diaobject_id': str( idmap[1747042] ),
                                     'ra': ramap[1747042],
                                     'dec': decmap[1747042],
                                     'facility': "Rob's C8 in his back yard",
                                     'mjd': 60364.128,
                                     'z': 1.36,
                                     'classid': 2232 } )
    assert res['status'] == 'ok'

    with db.DBCon( dictcursor=True ) as con:
        rows = con.execute( "SELECT * FROM spectruminfo" )

    # There was one pre-existing one from the fixture
    assert len(rows) == 2
    r = [ row for row in rows if row['root_diaobject_id']==idmap[1747042] ][0]
    assert r['facility'] == "Rob's C8 in his back yard"
    # Note that the mjd column in the spectruminfo table is only a real, so only has 24 bits of precision
    assert r['mjd'] == pytest.approx( 60364.13, abs=0.01 )
    assert r['z'] == pytest.approx( 1.36, abs=0.01 )
    assert r['classid'] == 2232
    assert r['is_host'] is None
    assert r['class_description'] is None

    # TODO MORE; test rejecting of missing requried, test unknown keys, test various things null


def test_get_known_spectrum_info( setup_spectrum_info, fastdb_client):
    # TODO : the spectruminfo table schema has evolved since these tests were
    #   written.  Update tests to check all of that!

    mjdnow, now, idmap, _ramap, _decmap = setup_spectrum_info

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
