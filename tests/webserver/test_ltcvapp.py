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
            thisltcv = ltcvs[ [ str(lc['rootid']) for lc in ltcvs ].index( rootid ) ]
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
    assert 'ispatch' in res[ str(roots[0]['root'].id) ]['ltcv'].keys()
    _check_res( infos, ltcvs, res )

    # Default is pv3, which should be identical to pv2, except there are no objects in
    #   pv2, so we have to give it an object_procver to avoid an error
    with pytest.raises( RuntimeError, match=( "Error response from server, status 422: rootids from "
                                              "many_object_ltcvs and get_object_infos don't match; "
                                              "you probably have the wrong object_procver." ) ):
        _ = fastdb_client.post( '/ltcv/getmanyltcvs',
                                json={ 'objids': [ str(roots[i]['root'].id) for i in [0, 1, 2] ] } )
    resdef = fastdb_client.post( '/ltcv/getmanyltcvs',
                                 json={ 'objids': [ str(roots[i]['root'].id) for i in [0, 1, 2] ],
                                        'object_procver': 'pvc_pv2' } )
    _check_res( infos, ltcvs, resdef )
    assert res == resdef

    # If we just give it procver pv2, then the object procver shouldn't be needed.
    res = fastdb_client.post( '/ltcv/getmanyltcvs/pvc_pv2',
                              json={ 'objids': [ str(roots[i]['root'].id) for i in [0, 1, 2] ] } )
    _check_res( infos, ltcvs, res )

    # Test mjd_now
    resdefnow = fastdb_client.post( '/ltcv/getmanyltcvs/pvc_pv2',
                                    json={ 'objids': [ str(roots[i]['root'].id) for i in [0, 1, 2] ],
                                           'mjd_now': 60041. } )
    for d in resdefnow.values():
        assert all( [ m <= 60041 for m in d['ltcv']['mjd'] ] )
    for mess in ltcvs:
        # ...zip is magic.  It will only provide as many elements as the
        # shorter of the two arrays.  So, we don't have to worry if mjd
        # gets truncated before another.  (It actually makes me feel a
        # little queasy that zip is magic like this...  I might rather
        # get an exception?)
        for k in mess.keys():
            if k == 'rootid':
                continue
            mess[k] = [ i for i, m in zip( mess[k], mess['mjd'] ) if m <= 60041 ]
    _check_res( infos, ltcvs, resdef )

    # Only the first object exists in pv1
    infos = ltcv.get_object_infos( [ 100 ], processing_version='pvc_pv1', return_format='json' )
    ltcvs = ltcv.many_object_ltcvs( 'pvc_pv1', 100, return_format='json', which='patch' )
    res = fastdb_client.post( '/ltcv/getmanyltcvs/pvc_pv1',
                              json={ 'objids': [ str(roots[i]['root'].id) for i in [0, 1, 2] ] } )
    assert list( res.keys() ) == [ str(roots[0]['root'].id) ]
    _check_res( infos, ltcvs, res )

    # Test which='detections' and 'forced'
    infos = ltcv.get_object_infos( [ 200, 201, 202 ], processing_version='pvc_pv2', return_format='json' )
    ltcvs = ltcv.many_object_ltcvs( 'pvc_pv2', [ 200, 201, 202 ], return_format='json', which='detections' )
    res = fastdb_client.post( '/ltcv/getmanyltcvs/pvc_pv2',
                              json={ 'objids': [ 200, 201, 202 ], 'which': 'detections' } )
    _check_res( infos, ltcvs, res, which='detections' )

    infos = ltcv.get_object_infos( [ 200, 201, 202 ], processing_version='pvc_pv2', return_format='json' )
    ltcvs = ltcv.many_object_ltcvs( 'pvc_pv2', [ 200, 201, 202 ], return_format='json', which='forced' )
    res = fastdb_client.post( '/ltcv/getmanyltcvs/pvc_pv2',
                              json={ 'objids': [ 200, 201, 202 ], 'which': 'forced' } )
    _check_res( infos, ltcvs, res, which='forced' )

    # Test 'ispatch' where patch it matters (i.e. where there are sources without corresponding forced sources)
    infos = ltcv.get_object_infos( [ 0, 1, 2 ], processing_version='realtime', return_format='json' )
    ltcvs = ltcv.many_object_ltcvs( 'realtime', [ 0, 1, 2 ], return_format='json', which='patch' )
    res = fastdb_client.post( '/ltcv/getmanyltcvs/realtime', json={ 'objids': [ 0, 1, 2 ], 'which': 'patch' } )
    assert any( res[ str(roots[2]['root'].id) ][ 'ltcv' ][ 'ispatch' ] )
    assert not all( res[ str(roots[2]['root'].id) ][ 'ltcv' ][ 'ispatch' ] )
    _check_res( infos, ltcvs, res )


def test_getltcv( test_user, fastdb_client, set_of_lightcurves, procver_collection ):
    roots = set_of_lightcurves
    bpvs, _pvs = procver_collection

    def _check_ltcv( res, rootdex, objdex, which='patch', bpv_key='unknown', obj_bpv_key=None,
                     include_base_procver=False, include_source_ids=False, include_source_positions=False,
                     obj_base_procver=None, pos_base_procver=None ):
        obj_bpv_key = bpv_key if obj_bpv_key is None else obj_bpv_key
        bpv_key = [ bpv_key ] if not isinstance( bpv_key, list ) else bpv_key
        obj_bpv_key = [ obj_bpv_key ] if not isinstance( obj_bpv_key, list ) else obj_bpv_key
        expectedkeys = [ 'rootid', 'diaobjectid', 'ra', 'dec', 'raerr', 'decerr', 'ra_dec_cov', 'ltcv', 'rootid' ]
        expectedkeys_ltcv = [ 'mjd', 'band', 'flux', 'fluxerr', 'isdet' ]
        if which == 'patch':
            expectedkeys_ltcv.append( 'ispatch' )
        if include_base_procver:
            expectedkeys.extend( [ 'obj_base_procver_id', 'pos_base_procver_id' ] )
            expectedkeys_ltcv.extend( [ 'base_procver_s', 'base_procver_f' ] )
        if include_source_ids:
            expectedkeys_ltcv.extend( [ 'diaobjectid', 'diasourceid' ] )
            if which != 'detections':
                expectedkeys_ltcv.append( 'diaforcedsourceid' )
        if include_source_positions:
            expectedkeys_ltcv.extend( [ 'det_ra', 'det_dec', 'det_raerr', 'det_decerr', 'det_ra_dec_cov' ] )

        assert set( res.keys() ) == set( expectedkeys )
        assert set( res['ltcv'].keys() ) == set( expectedkeys_ltcv )

        assert res['rootid'] == str( roots[rootdex]['root'].id )
        assert all( o == roots[rootdex]['objs'][objdex]['obj'].diaobjectid for o in res['diaobjectid'] )
        for r, d in zip( res['ra'], res['dec'] ):
            assert any( r == pytest.approx( roots[rootdex]['objs'][objdex]['pos'][b][-1].ra, abs=1e-5 )
                        for b in obj_bpv_key )
            assert any( d == pytest.approx( roots[rootdex]['objs'][objdex]['pos'][b][-1].dec, abs=1e-5 )
                        for b in obj_bpv_key )
        if which == 'detections':
            assert all( r == 1 for r in res['ltcv']['isdet'] )
        if include_base_procver:
            for rb in res['obj_base_procver_id']:
                assert any( rb == str(bpvs[o].id) for o in obj_bpv_key )
            for rb in res['pos_base_procver_id']:
                for opv in obj_bpv_key:
                    valid_bpvkeys = [ bpvs[k].id for k in bpvs.keys() if k[0:len(opv)+19]==f'{opv}_diaobject_position' ]
                    assert any( rb == str(v) for v in valid_bpvkeys )

        forced = {}
        sources = {}
        # We're assuming that the bpv keys passed will be in increasing priority order
        for bk in bpv_key:
            forced.update( { f.visit: f for f in roots[rootdex]['objs'][objdex]['frc'][bk] } )
            sources.update( { s.visit: s for s in roots[rootdex]['objs'][objdex]['src'][bk] } )
        forced = list( forced.values() )
        sources = list( sources.values() )
        forced.sort( key=lambda f: f.midpointmjdtai )
        sources.sort( key=lambda s: s.midpointmjdtai )
        srci = 0
        frci = 0
        for i in range( len(res['ltcv']['mjd'] ) ):
            # Should have forced photometry where ispatch is not 1.  (ispatch will only be 1 for last n points)
            if ( ( 'which' == 'forced' ) or
                 ( ( 'ispatch' in res['ltcv'] ) and not ( res['ltcv']['ispatch'][i] ) ) ):
                assert res['ltcv']['mjd'][i] == pytest.approx( forced[frci].midpointmjdtai, abs=1./3600./24. )
                assert res['ltcv']['band'][i] == forced[frci].band
                assert res['ltcv']['flux'][i] == pytest.approx( forced[frci].psfflux, rel=1e-6 )
                assert res['ltcv']['fluxerr'][i] == pytest.approx( forced[frci].psffluxerr, rel=1e-6 )
                frci += 1
            # If 'isdet' is true, should correspond to a source
            if res['ltcv']['isdet'][i]:
                assert res['ltcv']['mjd'][i] == pytest.approx( sources[srci].midpointmjdtai, abs=1./3600./24. )
                assert res['ltcv']['band'][i] == sources[srci].band
                assert res['ltcv']['flux'][i] == pytest.approx( sources[srci].psfflux, rel=1e-6 )
                assert res['ltcv']['fluxerr'][i] == pytest.approx( sources[srci].psffluxerr, rel=1e-6 )
                srci += 1

    res = fastdb_client.post( f'/ltcv/getltcv/pvc_pv2/{roots[3]["root"].id}' )
    _check_ltcv( res, 3, 1, bpv_key='bpv2a' )

    # The base processing version of the *object* is going to be bpv2,
    #   even though we're supposed to be pulling photometry from default
    #   (which is an alias for pv3).  (TODO: fix the fixture so that the
    #   processing versions are different for different sources so we
    #   can test that!  That probably means editing other tests too when
    #   the fixture changes....)
    with pytest.raises( RuntimeError, match=( "Error response from server, status 422: rootids from "
                                              "many_object_ltcvs and get_object_infos don't match; you probably "
                                              "have the wrong object_procver." ) ):
        res = fastdb_client.post( '/ltcv/getltcv/203' )

    with pytest.raises( RuntimeError, match=( "Error response from server, status 422: rootids from "
                                              "many_object_ltcvs and get_object_infos don't match; you probably "
                                              "have the wrong object_procver." ) ):
        res = fastdb_client.post( '/ltcv/getltcv/pvc_pv3/203' )

    res = fastdb_client.post( '/ltcv/getltcv/pvc_pv3/203', json={ 'object_procver': 'pvc_pv2' } )
    _check_ltcv( res, 3, 1, bpv_key='bpv3', obj_bpv_key='bpv2a' )

    # Objects aren't defined in 'default', which is probably bad, damn, I really need to
    #   rethink these fixtures, which will be PAINFUL.
    with pytest.raises( RuntimeError, match=( "Error response from server, status 422: rootids from "
                                              "many_object_ltcvs and get_object_infos don't match; you probably "
                                              "have the wrong object_procver." ) ):
        res = fastdb_client.post( f'/ltcv/getltcv/{roots[3]["root"].id}' )

    with pytest.raises( RuntimeError, match=( "Error response from server, status 422: rootids from "
                                              "many_object_ltcvs and get_object_infos don't match; you probably "
                                              "have the wrong object_procver." ) ):
        res = fastdb_client.post( f'/ltcv/getltcv/pvc_pv3/{roots[3]["root"].id}' )

    res = fastdb_client.post( f'/ltcv/getltcv/pvc_pv3/{roots[3]["root"].id}',
                              json={ 'object_procver': 'pvc_pv2' } )
    _check_ltcv( res, 3, 1, bpv_key='bpv3', obj_bpv_key='bpv2a' )

    res = fastdb_client.post( '/ltcv/getltcv/realtime/0' )
    _check_ltcv( res, 0, 0, bpv_key='realtime' )

    # The object *is* defined in realtime, so the fallback defaults server side should work here
    res = fastdb_client.post( f'/ltcv/getltcv/realtime/{roots[0]["root"].id}' )
    _check_ltcv( res, 0, 0, bpv_key='realtime' )

    # This is an example where there's actually a mix of base processing versions on
    #   the returned forced photometry.
    res = fastdb_client.post( '/ltcv/getltcv/pvc_pv1/100' )
    _check_ltcv( res, 0, 2, bpv_key=['bpv1a', 'bpv1'], obj_bpv_key='bpv1a' )


# TODO : test getrandomltcv ; that might require the ability to pass a random seed for a reproducible test.
@pytest.mark.skip( reason="This test needs to be written" )
def test_getrandomltcv( test_user, fastdb_client, procver_collection, set_of_lightcurves ):
    assert False


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
