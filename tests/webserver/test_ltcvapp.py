import pytest
import numbers

import numpy as np

import ltcv


def test_getmanyltcvs( test_user, fastdb_client, set_of_lightcurves ):
    roots = set_of_lightcurves

    def _check_res( infos, ltcvs, res, mjdnow=None, which='patch',
                    include_base_procver=False, include_source_ids=False, include_source_positions=False ):
        info_str_rootid = [ str(i) for i in infos['rootid'] ]
        assert set(res.keys() ) == set( info_str_rootid )

        for rootid, mess in res.items():
            # Because of the test set we're using, there is only one diaobjectid
            #   for any rootid.  This is too bad, because it means we can't test
            #   the case of multiple diaobjectids.  However, the thought of
            #   redoing the set_of_lightcurves fixture and every test that
            #   depends on it makes me shudder.
            infodex = info_str_rootid.index( rootid )
            thisltcv = ltcvs[ [ str(l['rootid']) for l in ltcvs ].index( rootid ) ]
            if include_base_procver:
                expectedkeys = [ 'diaobjectid', 'obj_base_procverid', 'pos_base_procver_id',
                                 'ra', 'dec', 'raerr', 'decerr', 'ra_dec_cov' ]
            else:
                expectedkeys = [ 'diaobjectid', 'ra', 'dec', 'raerr', 'decerr', 'ra_dec_cov' ]

            assert all( k in mess for k in expectedkeys )
            assert 'ltcv' in mess
            assert all( infos[k][infodex] == ( pytest.approx( mess[k][0], rel=1e-6 )
                                               if isinstance( mess[k][0], numbers.Real )
                                               else mess[k][0] )
                        for k in expectedkeys )

            expectedkeys = [ 'mjd', 'band', 'flux', 'fluxerr', 'isdet' ]
            if which == 'patch':
                expectedkeys.append( 'ispatch' )
            if include_source_ids:
                expectedkeys.extend( 'diaobjectid', 'visit', 'diasourceid', 'diaforcedsourceid' )
            if include_base_procver:
                expectedkeys.append( 'base_procver_s' )
                if which != 'detections':
                    expectedkeys.append( 'base_procver_f' )
            if include_source_positions:
                expectedkeys.extend( [ 'det_ra', 'det_dec', 'det_raerr', 'det_decerr', 'det_ra_dec_cov' ] )

            assert set( mess['ltcv'].keys() ) == set( expectedkeys )
            assert all(
                all( lval == ( pytest.approx( mval, rel=1e-6 ) if isinstance( mval, numbers.Real ) else mval )
                     for lval, mval in zip( thisltcv[k], mess['ltcv'][k] ) )
                for k in expectedkeys
            )

    infos = ltcv.get_object_infos( [ 200, 201, 202 ], processing_version='pvc_pv2', return_format='json' )
    ltcvs = ltcv.many_object_ltcvs( 'pvc_pv3', [ 200, 201, 202 ], return_format='json', which='patch' )
    res = fastdb_client.post( '/ltcv/getmanyltcvs', json={ 'objids': [ 200, 201, 202 ],
                                                           'object_procver': 'pvc_pv2' } )
    import pdb; pdb.set_trace()
    assert 'ispatch' in res[ str(roots[0]['root'].id) ]['ltcv'].keys()
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

    def _compare_direct_to_webap( df, objdf, res ):
        assert ( df.index.get_level_values('diaobjectid').unique()
                 == np.array( [ r['diaobjectid'] for r in res ] ) ).all()
        assert ( objdf.index.get_level_values('diaobjectid').unique()
                 == np.array( [ r['diaobjectid'] for r in res ] ) ).all()
        for objrow in res:
            objid = objrow['diaobjectid']
            subdf = df.loc[ objid ]
            subobjdf = objdf.loc[ objid ]
            assert objrow['rootid'] == str( subobjdf.rootid )
            assert objrow['ra'] == subobjdf.ra
            assert objrow['dec'] == subobjdf.dec
            assert objrow['zp'] == 31.4
            assert len(subdf) == len( objrow['photometry']['mjd'] )
            assert ( subdf.index.values == np.array( objrow['photometry']['mjd'] ) ).all()
            assert ( subdf.band == np.array( objrow['photometry']['band'] ) ).all()
            assert ( subdf.visit == np.array( objrow['photometry']['visit'] ) ).all()
            assert ( subdf.flux == np.array( objrow['photometry']['flux'] ) ) .all()
            assert ( subdf.fluxerr == np.array( objrow['photometry']['fluxerr'] ) ).all()
            assert ( subdf.isdet == np.array( objrow['photometry']['isdet'] ) ).all()
            if 'ispatch' in subdf.columns:
                assert ( subdf.ispatch == np.array( objrow['photometry']['ispatch'] ) ).all()

    df, objdf, _ = ltcv.get_hot_ltcvs( 'pvc_pv3', detected_since_mjd=60035, mjd_now=60056 )
    res = fastdb_client.post( '/ltcv/gethottransients', json={ 'processing_version': 'pvc_pv3',
                                                               'detected_since_mjd': 60035,
                                                               'mjd_now': 60056 } )
    _compare_direct_to_webap( df, objdf, res )

    df, objdf, _ = ltcv.get_hot_ltcvs( 'pvc_pv3', detected_since_mjd=60035, mjd_now=60046 )
    res = fastdb_client.post( '/ltcv/gethottransients', json={ 'processing_version': 'pvc_pv3',
                                                               'detected_since_mjd': 60035,
                                                               'mjd_now': 60046 } )
    _compare_direct_to_webap( df, objdf, res )

    df, objdf, _ = ltcv.get_hot_ltcvs( 'pvc_pv3', detected_in_last_days=2, mjd_now=60021 )
    res = fastdb_client.post( '/ltcv/gethottransients', json={ 'processing_version': 'pvc_pv3',
                                                               'detected_in_last_days': 2,
                                                               'mjd_now': 60021 } )
    _compare_direct_to_webap( df, objdf, res )

    df, objdf, _ = ltcv.get_hot_ltcvs( 'pvc_pv3', detected_in_last_days=2, mjd_now=60041 )
    res = fastdb_client.post( '/ltcv/gethottransients', json={ 'processing_version': 'pvc_pv3',
                                                               'detected_in_last_days': 2,
                                                               'mjd_now': 60041 } )
    _compare_direct_to_webap( df, objdf, res )

    # detected_in_last_days defaults to 30
    df, objdf, _ = ltcv.get_hot_ltcvs( 'pvc_pv3', mjd_now=60085 )
    res = fastdb_client.post( '/ltcv/gethottransients', json={ 'processing_version': 'pvc_pv3',
                                                               'mjd_now': 60085 } )
    _compare_direct_to_webap( df, objdf, res )
    df, objdf, _ = ltcv.get_hot_ltcvs( 'pvc_pv3', mjd_now=60095 )
    res = fastdb_client.post( '/ltcv/gethottransients', json={ 'processing_version': 'pvc_pv3',
                                                               'mjd_now': 60095 } )
    _compare_direct_to_webap( df, objdf, res )

    # Test source patch.  Gotta use pvc_pv1 for this.

    df, objdf, _ = ltcv.get_hot_ltcvs( 'pvc_pv1', mjd_now=60031 )
    res = fastdb_client.post( '/ltcv/gethottransients', json={ 'processing_version': 'pvc_pv1',
                                                               'mjd_now': 60031 } )
    _compare_direct_to_webap( df, objdf, res )

    df, objdf, _ = ltcv.get_hot_ltcvs( 'pvc_pv1', mjd_now=60031, source_patch=True )
    res = fastdb_client.post( '/ltcv/gethottransients', json={ 'processing_version': 'pvc_pv1',
                                                               'mjd_now': 60031,
                                                               'source_patch': True } )
    _compare_direct_to_webap( df, objdf, res )
