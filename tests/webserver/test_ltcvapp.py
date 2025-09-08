import pytest
import uuid

import numpy as np

import ltcv
import util


def test_getmanyltcvs( test_user, fastdb_client, set_of_lightcurves ):
    roots = set_of_lightcurves

    def _check_res( infos, ltcvs, res, mjdnow=None, which='patch' ):
        # JSON only allows string keys, which makes me rage.
        # We should be using a binary format, yes?
        assert list( int(i) for i in res.keys() ) == infos['diaobjectid']

        for rooti, strid in enumerate( res.keys() ):
            dex = infos['diaobjectid'].index( int(strid) )
            for k in infos.keys():
                if isinstance( infos[k][dex], uuid.UUID ):
                    assert str( infos[k][dex] ) == res[strid][k]
                else:
                    assert infos[k][dex] == res[strid][k]
            for k in ltcvs[int(strid)].keys():
                if k == 'rootid':
                    assert str( ltcvs[int(strid)][k] ) == res[strid]['ltcv'][k]
                else:
                    # Really hope I don't gotta pytest.approx here, but I might need to
                    assert all( [ i == j for i, j in zip( ltcvs[int(strid)][k], res[strid]['ltcv'][k] ) ] )

    infos = ltcv.get_object_infos( [ 200, 201, 202 ], return_format='json' )
    ltcvs = ltcv.many_object_ltcvs( 'pvc_pv2', [ 200, 201, 202 ], return_format='json', which='patch' )
    res = fastdb_client.post( '/ltcv/getmanyltcvs', json={ 'objids': [ 200, 201, 202 ] } )
    assert 'ispatch' in res['200']['ltcv'].keys()
    _check_res( infos, ltcvs, res )

    res = fastdb_client.post( '/ltcv/getmanyltcvs/pvc_pv2',
                              json={ 'objids': [ str(roots[i]['root'].id) for i in [0, 1, 2] ] } )
    _check_res( infos, ltcvs, res )

    # Default is pv3, which should be identical to pv2
    resdef = fastdb_client.post( '/ltcv/getmanyltcvs',
                                 json={ 'objids': [ str(roots[i]['root'].id) for i in [0, 1, 2] ] } )
    _check_res( infos, ltcvs, resdef )
    assert res == resdef

    # Test mjd_now
    resdefnow = fastdb_client.post( '/ltcv/getmanyltcvs',
                                    json={ 'objids': [ str(roots[i]['root'].id) for i in [0, 1, 2] ],
                                           'mjd_now': 60041. } )
    for d in resdefnow.values():
        assert all( [ m <= 60041 for m in d['ltcv']['mjd'] ] )
    for mess in ltcvs.values():
        for k in ( 'mjd', 'band', 'flux', 'fluxerr', 'isdet', 'ispatch' ):
            mess[k] = [ i for i, m in zip( mess[k], mess['mjd'] ) if m <= 60041 ]
    _check_res( infos, ltcvs, resdef )

    # Only the first object exists in pv1
    infos = ltcv.get_object_infos( [ 100 ], return_format='json' )
    ltcvs = ltcv.many_object_ltcvs( 'pvc_pv1', 100, return_format='json', which='patch' )
    res = fastdb_client.post( '/ltcv/getmanyltcvs/pvc_pv1',
                              json={ 'objids': [ str(roots[i]['root'].id) for i in [0, 1, 2] ] } )
    assert list( res.keys() ) == [ '100' ]
    _check_res( infos, ltcvs, res )

    # Test which='detections' and 'forced'
    infos = ltcv.get_object_infos( [ 200, 201, 202 ], return_format='json' )
    ltcvs = ltcv.many_object_ltcvs( 'pvc_pv2', [ 200, 201, 202 ], return_format='json', which='detections' )
    res = fastdb_client.post( '/ltcv/getmanyltcvs', json={ 'objids': [ 200, 201, 202 ], 'which': 'detections' } )
    assert 'ispatch' not in res['200']['ltcv'].keys()
    _check_res( infos, ltcvs, res )

    infos = ltcv.get_object_infos( [ 200, 201, 202 ], return_format='json' )
    ltcvs = ltcv.many_object_ltcvs( 'pvc_pv2', [ 200, 201, 202 ], return_format='json', which='forced' )
    res = fastdb_client.post( '/ltcv/getmanyltcvs', json={ 'objids': [ 200, 201, 202 ], 'which': 'forced' } )
    assert 'ispatch' not in res['200']['ltcv'].keys()
    _check_res( infos, ltcvs, res )

    # Test 'ispatch' where patch it matters (i.e. where there are sources without corresponding forced sources)
    infos = ltcv.get_object_infos( [ 0, 1, 2 ], return_format='json' )
    ltcvs = ltcv.many_object_ltcvs( 'realtime', [ 0, 1, 2 ], return_format='json', which='patch' )
    res = fastdb_client.post( '/ltcv/getmanyltcvs/realtime', json={ 'objids': [ 0, 1, 2 ], 'which': 'patch' } )
    assert 'ispatch' in res['0']['ltcv'].keys()
    assert any( res['2']['ltcv']['ispatch'] )
    assert not all( res['2']['ltcv']['ispatch'] )
    _check_res( infos, ltcvs, res )


def test_getltcv( test_user, fastdb_client, set_of_lightcurves, procver_collection ):
    roots = set_of_lightcurves
    bpvs, _pvs = procver_collection

    def _check_ltcv( res, rootdex, objdex, bpv ):
        assert res['diaobjectid'] == roots[rootdex]['objs'][objdex]['obj'].diaobjectid
        assert res['rootid'] == str( roots[rootdex]['root'].id )
        assert res['ra'] == roots[rootdex]['objs'][objdex]['obj'].ra
        assert res['dec'] == roots[rootdex]['objs'][objdex]['obj'].dec
        assert res['base_procver_id'] == str( bpvs[bpv].id )
        forced = roots[rootdex]['objs'][objdex]['frc'][bpv]
        sources = roots[rootdex]['objs'][objdex]['src'][bpv]
        srci = 0
        for i in range( len(res['ltcv']['mjd'] ) ):
            # Should have forced photometry where ispatch is not 1.  (ispatch will only be 1 for last n points)
            if ( 'ispatch' in res['ltcv'] ) and not ( res['ltcv']['ispatch'][i] ):
                assert res['ltcv']['mjd'][i] == pytest.approx( forced[i].midpointmjdtai, abs=1./3600./24. )
                assert res['ltcv']['band'][i] == forced[i].band
                assert res['ltcv']['flux'][i] == pytest.approx( forced[i].psfflux, rel=1e-6 )
                assert res['ltcv']['fluxerr'][i] == pytest.approx( forced[i].psffluxerr, rel=1e-6 )
            # If 'isdet' is true, should correspond to a source (which are sorted... I think)
            if res['ltcv']['isdet'][i]:
                assert res['ltcv']['mjd'][i] == pytest.approx( sources[srci].midpointmjdtai, abs=1./3600./24. )
                assert res['ltcv']['band'][i] == sources[srci].band
                assert res['ltcv']['flux'][i] == pytest.approx( sources[srci].psfflux, rel=1e-6 )
                assert res['ltcv']['fluxerr'][i] == pytest.approx( sources[srci].psffluxerr, rel=1e-6 )
                srci += 1

    # The base processing version of the *object* is going to be bpv2,
    #   even though we're supposed to be pulling photometry from default
    #   (which is an alias for pv3).  (TODO: fix the fixture so that the
    #   processing versions are different for different sources so we
    #   can test that!  That probably means editing other tests too when
    #   the fixture changes....)
    res = fastdb_client.post( '/ltcv/getltcv/203' )
    _check_ltcv( res, 3, 1, 'bpv2' )

    res = fastdb_client.post( '/ltcv/getltcv/pvc_pv3/203' )
    _check_ltcv( res, 3, 1, 'bpv2' )

    res = fastdb_client.post( f'/ltcv/getltcv/{roots[3]["root"].id}' )
    _check_ltcv( res, 3, 1, 'bpv2' )

    res = fastdb_client.post( f'/ltcv/getltcv/pvc_pv3/{roots[3]["root"].id}' )
    _check_ltcv( res, 3, 1, 'bpv2' )

    res = fastdb_client.post( '/ltcv/getltcv/realtime/0' )
    _check_ltcv( res, 0, 0, 'realtime' )

    res = fastdb_client.post( f'/ltcv/getltcv/realtime/{roots[0]["root"].id}' )
    _check_ltcv( res, 0, 0, 'realtime' )

    res = fastdb_client.post( '/ltcv/getltcv/pvc_pv1/100' )
    _check_ltcv( res, 0, 2, 'bpv1' )


# TODO : test getrandomltcv ; that might require the ability to pass a random seed for a reproducible test.


def test_gethottransients( test_user, fastdb_client, procver_collection, set_of_lightcurves ):
    # This tests gets the same information as ../test_ltcv.py, only via
    # the webap.  ../test_ltcv.py::test_get_hot_ltcvs makes sure that
    # the direct call to ltcv.get_hot_ltcvs returns the right stuff.
    # (Or, at least, it should.)  This test makes sure that what you get
    # from the webap matches what you get from a direct call.

    def _compare_direct_to_webap( df, res ):
        assert set( str(i) for i in df['rootid'].unique() ) == set( r['rootid'] for r in res )
        for objrow in res:
            subdf = df[ df.rootid == util.asUUID( objrow['rootid'] ) ]
            assert len(subdf) == len( objrow['photometry']['mjd'] )
            assert objrow['zp'] == 31.4
            assert ( subdf.diaobjectid == objrow['diaobjectid'] ).all()
            assert all( subdf.ra == objrow['ra'] )
            assert all( subdf.dec == objrow['dec'] )
            assert ( subdf.band == np.array( objrow['photometry']['band'] ) ).all()
            assert ( subdf.is_source == np.array( objrow['photometry']['is_source'] ) ).all()
            # Thought required: should we be doing an approx thing for the rest of these?
            #  (Either pytest.approx or np.isclose.)
            assert ( subdf.midpointmjdtai == np.array( objrow['photometry']['mjd'] ) ).all()
            assert ( subdf.psfflux == np.array( objrow['photometry']['flux'] ) ).all()
            assert ( subdf.psffluxerr == np.array( objrow['photometry']['fluxerr'] ) ).all()

    # TODO : look at some of the actual returned values to make sure they're right.
    # (Query the database and compare?)

    df, _ = ltcv.get_hot_ltcvs( 'pvc_pv3', detected_since_mjd=60035, mjd_now=60056 )
    res = fastdb_client.post( '/ltcv/gethottransients', json={ 'processing_version': 'pvc_pv3',
                                                               'detected_since_mjd': 60035,
                                                               'mjd_now': 60056 } )
    _compare_direct_to_webap( df, res )

    df, _ = ltcv.get_hot_ltcvs( 'pvc_pv3', detected_since_mjd=60035, mjd_now=60046 )
    res = fastdb_client.post( '/ltcv/gethottransients', json={ 'processing_version': 'pvc_pv3',
                                                               'detected_since_mjd': 60035,
                                                               'mjd_now': 60046 } )
    _compare_direct_to_webap( df, res )

    df, _ = ltcv.get_hot_ltcvs( 'pvc_pv3', detected_in_last_days=2, mjd_now=60021 )
    res = fastdb_client.post( '/ltcv/gethottransients', json={ 'processing_version': 'pvc_pv3',
                                                               'detected_in_last_days': 2,
                                                               'mjd_now': 60021 } )
    _compare_direct_to_webap( df, res )

    df, _ = ltcv.get_hot_ltcvs( 'pvc_pv3', detected_in_last_days=2, mjd_now=60041 )
    res = fastdb_client.post( '/ltcv/gethottransients', json={ 'processing_version': 'pvc_pv3',
                                                               'detected_in_last_days': 2,
                                                               'mjd_now': 60041 } )
    _compare_direct_to_webap( df, res )

    # detected_in_last_days defaults to 30
    df, _ = ltcv.get_hot_ltcvs( 'pvc_pv3', mjd_now=60085 )
    res = fastdb_client.post( '/ltcv/gethottransients', json={ 'processing_version': 'pvc_pv3',
                                                               'mjd_now': 60085 } )
    _compare_direct_to_webap( df, res )
    df, _ = ltcv.get_hot_ltcvs( 'pvc_pv3', mjd_now=60095 )
    res = fastdb_client.post( '/ltcv/gethottransients', json={ 'processing_version': 'pvc_pv3',
                                                               'mjd_now': 60095 } )
    _compare_direct_to_webap( df, res )

    # Test source patch.  Gotta use pvc_pv1 for this.

    df, _ = ltcv.get_hot_ltcvs( 'pvc_pv1', mjd_now=60031 )
    res = fastdb_client.post( '/ltcv/gethottransients', json={ 'processing_version': 'pvc_pv1',
                                                               'mjd_now': 60031 } )
    _compare_direct_to_webap( df, res )

    df, _ = ltcv.get_hot_ltcvs( 'pvc_pv1', mjd_now=60031, source_patch=True )
    res = fastdb_client.post( '/ltcv/gethottransients', json={ 'processing_version': 'pvc_pv1',
                                                               'mjd_now': 60031,
                                                               'source_patch': True } )
    _compare_direct_to_webap( df, res )
