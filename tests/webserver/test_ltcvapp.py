import pytest
import uuid

import ltcv


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


def test_gethottransients( test_user, fastdb_client, procver, alerts_90days_sent_received_and_imported ):
    # This tests gets the same information as ../test_ltcv.py, only via the webap.

    # TODO : look at some of the actual returned values to make sure they're right.
    # (Query the database and compare?)

    # ****************************************
    # return format 0
    res = fastdb_client.post( '/ltcv/gethottransients',
                              json={ 'processing_version': procver.description,
                                     'detected_since_mjd': 60325.,
                                     'mjd_now': 60328. } )
    assert isinstance( res, list )
    assert len( res ) == 4
    assert set( res[0].keys() ) == { 'objectid', 'ra', 'dec', 'zp', 'redshift', 'sncode', 'photometry' }
    for pkey in [ 'mjd', 'band', 'flux', 'fluxerr', 'is_source' ]:
        assert sum( len( res[i]['photometry'][pkey] ) for i in range( len(res) ) ) == 88


    res = fastdb_client.post( '/ltcv/gethottransients',
                              json={ 'processing_version': procver.description,
                                     'detected_since_mjd': 60325.,
                                     'mjd_now': 60328.,
                                     'source_patch': True } )
    assert isinstance( res, list )
    assert len( res ) == 4
    assert set( res[0].keys() ) == { 'objectid', 'ra', 'dec', 'zp', 'redshift', 'sncode', 'photometry' }
    for pkey in [ 'mjd', 'band', 'flux', 'fluxerr', 'is_source' ]:
        assert sum( len( res[i]['photometry'][pkey] ) for i in range( len(res) ) ) == 91
    assert sum( sum( res[i]['photometry']['is_source'] ) for i in range( len(res) ) ) == 3


    res = fastdb_client.post( '/ltcv/gethottransients',
                              json={ 'processing_version': procver.description,
                                     'detected_in_last_days': 3.,
                                     'mjd_now': 60328.,
                                     'source_patch': True } )
    assert isinstance( res, list )
    assert len( res ) == 4
    assert set( res[0].keys() ) == { 'objectid', 'ra', 'dec', 'zp', 'redshift', 'sncode', 'photometry' }
    for pkey in [ 'mjd', 'band', 'flux', 'fluxerr', 'is_source' ]:
        assert sum( len( res[i]['photometry'][pkey] ) for i in range( len(res) ) ) == 91
    assert sum( sum( res[i]['photometry']['is_source'] ) for i in range( len(res) ) ) == 3


    res = fastdb_client.post( '/ltcv/gethottransients',
                              json={ 'processing_version': procver.description,
                                     'mjd_now': 60328.,
                                     'source_patch': True } )
    assert isinstance( res, list )
    assert len( res ) == 14
    assert set( res[0].keys() ) == { 'objectid', 'ra', 'dec', 'zp', 'redshift', 'sncode', 'photometry' }
    for pkey in [ 'mjd', 'band', 'flux', 'fluxerr', 'is_source' ]:
        assert sum( len( res[i]['photometry'][pkey] ) for i in range( len(res) ) ) == 310
    assert sum( sum( res[i]['photometry']['is_source'] ) for i in range( len(res) ) ) == 12


    res = fastdb_client.post( '/ltcv/gethottransients',
                              json={ 'processing_version': procver.description,
                                     'detected_since_mjd': 60325.,
                                     'mjd_now': 60328.,
                                     'source_patch': True,
                                     'include_hostinfo': True } )
    assert isinstance( res, list )
    assert len( res ) == 4
    assert set( res[0].keys() ) == { 'objectid', 'ra', 'dec', 'zp', 'redshift', 'sncode', 'photometry',
                                     'hostgal_petroflux_r', 'hostgal_petroflux_r_err',
                                     'hostgal_stdcolor_u_g', 'hostgal_stdcolor_g_r', 'hostgal_stdcolor_r_i',
                                     'hostgal_stdcolor_i_z', 'hostgal_stdcolor_z_y', 'hostgal_stdcolor_u_g_err',
                                     'hostgal_stdcolor_g_r_err', 'hostgal_stdcolor_r_i_err',
                                     'hostgal_stdcolor_i_z_err', 'hostgal_stdcolor_z_y_err',
                                     'hostgal_snsep', 'hostgal_pzmean', 'hostgal_pzstd' }


    # ****************************************
    # return format 1
    res = fastdb_client.post( '/ltcv/gethottransients',
                              json={ 'processing_version': procver.description,
                                     'detected_in_last_days': 3.,
                                     'mjd_now': 60328.,
                                     'source_patch': True,
                                     'return_format': 1 } )
    assert isinstance( res, list )
    assert len( res ) == 4
    assert set( res[0].keys() ) == { 'objectid', 'ra', 'dec', 'zp', 'redshift', 'sncode',
                                     'mjd', 'band', 'flux', 'fluxerr', 'is_source' }
    for pkey in [ 'mjd', 'band', 'flux', 'fluxerr', 'is_source' ]:
        assert sum( len( res[i][pkey] ) for i in range( len(res) ) ) == 91
    assert sum( sum( res[i]['is_source'] ) for i in range( len(res) ) ) == 3

    res = fastdb_client.post( '/ltcv/gethottransients',
                              json={ 'processing_version': procver.description,
                                     'detected_since_mjd': 60325.,
                                     'mjd_now': 60328.,
                                     'source_patch': True,
                                     'include_hostinfo': True,
                                     'return_format': 1 } )
    assert isinstance( res, list )
    assert len( res ) == 4
    assert set( res[0].keys() ) == { 'objectid', 'ra', 'dec', 'zp', 'redshift', 'sncode',
                                     'mjd', 'band', 'flux', 'fluxerr', 'is_source',
                                     'hostgal_petroflux_r', 'hostgal_petroflux_r_err',
                                     'hostgal_stdcolor_u_g', 'hostgal_stdcolor_g_r', 'hostgal_stdcolor_r_i',
                                     'hostgal_stdcolor_i_z', 'hostgal_stdcolor_z_y', 'hostgal_stdcolor_u_g_err',
                                     'hostgal_stdcolor_g_r_err', 'hostgal_stdcolor_r_i_err',
                                     'hostgal_stdcolor_i_z_err', 'hostgal_stdcolor_z_y_err',
                                     'hostgal_snsep', 'hostgal_pzmean', 'hostgal_pzstd' }


    # ****************************************
    # return format 2
    res = fastdb_client.post( '/ltcv/gethottransients',
                              json={ 'processing_version': procver.description,
                                     'detected_in_last_days': 3.,
                                     'mjd_now': 60328.,
                                     'source_patch': True,
                                     'return_format': 2 } )
    assert isinstance( res, dict )
    assert set( res.keys() ) == { 'objectid', 'ra','dec', 'zp', 'redshift', 'sncode',
                                  'mjd', 'band', 'flux', 'fluxerr', 'is_source' }
    assert all( len(v) == 4 for v in res.values() )
    for pkey in [ 'mjd', 'band', 'flux', 'fluxerr', 'is_source' ]:
        assert sum( len( res[pkey][i] ) for i in range(4) ) == 91
    assert sum( sum( res['is_source'][i] ) for i in range(4) ) == 3

    res = fastdb_client.post( '/ltcv/gethottransients',
                              json={ 'processing_version': procver.description,
                                     'detected_in_last_days': 3.,
                                     'mjd_now': 60328.,
                                     'source_patch': True,
                                     'include_hostinfo': True,
                                     'return_format': 2 } )
    assert set( res.keys() ) == { 'objectid', 'ra', 'dec', 'zp', 'redshift', 'sncode',
                                  'mjd', 'band', 'flux', 'fluxerr', 'is_source',
                                  'hostgal_petroflux_r', 'hostgal_petroflux_r_err',
                                  'hostgal_stdcolor_u_g', 'hostgal_stdcolor_g_r', 'hostgal_stdcolor_r_i',
                                  'hostgal_stdcolor_i_z', 'hostgal_stdcolor_z_y', 'hostgal_stdcolor_u_g_err',
                                  'hostgal_stdcolor_g_r_err', 'hostgal_stdcolor_r_i_err',
                                  'hostgal_stdcolor_i_z_err', 'hostgal_stdcolor_z_y_err',
                                  'hostgal_snsep', 'hostgal_pzmean', 'hostgal_pzstd' }
