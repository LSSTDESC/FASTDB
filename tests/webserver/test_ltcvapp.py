import pytest
import copy
import time

from util import FDBLogger


def test_getmanyltcvs( test_user, fastdb_client, set_of_lightcurves, lightcurve_checker ):
    roots = set_of_lightcurves
    check_ltcv = lightcurve_checker

    ltcvlist = [
        { 'procver': 'pvc_pv1',
          'request_objs': 'do_not_request_any_objects_for_this_test',
          'expected_roots': [ 0 ],
          'expected_diaobjids': [ 100 ],
          'procver_key': 'pv1'
         },
        { 'procver': 'pvc_pv2',
          'request_objs': 'do_not_request_any_objects_for_this_test',
          'expected_roots': [ 0, 1, 2, 3 ],
          'expected_diaobjids': [ 200, 201, 2011, 202, 203 ],
          'procver_key': 'pv2'
         },
        { 'procver': 'pvc_pv3',
          'request_objs': 'do_not_request_any_objects_for_this_test',
          'expected_roots': [ 0, 1, 2, 3 ],
          'expected_diaobjids': [ 200, 201, 2011, 202, 203 ],
          'procver_key': 'pv3'
         },
        { 'procver': 'pvc_pv1',
          'request_objs': None,
          'expected_roots': [ 0 ],
          'expected_diaobjids': [ 100 ],
          'procver_key': 'pv1' },
        { 'procver': 'pvc_pv2',
          'request_objs': None,
          'expected_roots': [ 0, 1, 2, 3 ],
          'expected_diaobjids': [ 200, 201, 2011, 202, 203 ],
          'procver_key': 'pv2'
         },
        { 'procver': 'pvc_pv3',
          'request_objs': None,
          'expected_roots': [ 0, 1, 2, 3 ],
          'expected_diaobjids': [ 200, 201, 2011, 202, 203 ],
          'procver_key': 'pv3'
         },
        # Object 1 is not in pv1, so ony expect object 0 back
        { 'procver': 'pvc_pv1',
          'request_objs': [ str(roots[i]['root'].id) for i in [0, 1] ],
          'expected_roots': [0],
          'expected_diaobjids': [100],
          'procver_key': 'pv1'
         },
        { 'procver': 'pvc_pv1',
          'request_objs': [100, 101],
          'expected_roots': [0],
          'expected_diaobjids': [100],
          'procver_key': 'pv1'
          },
        # pvc_pv2 should be the default
        { 'procver': None,
          'request_objs': [ str(roots[i]['root'].id) for i in [0, 2] ],
          'expected_roots': [0, 2],
          'expected_diaobjids': [200, 202],
          'procver_key': 'pv2'
         },
        # If we ask for diaobjects that are in the wrong processing version, we still get
        #   back the corresponding ones from the sources in this processing version
        { 'procver': 'pvc_pv2',
          'request_objs': [0, 2],
          'expected_roots': [0, 2],
          'expected_diaobjids': [200, 202],
          'procver_key': 'pv2'
         },
        { 'procver': 'pvc_pv2',
          'request_objs': [0, 1, 2],
          'expected_roots': [0, 1, 2],
          'expected_diaobjids': [200, 201, 2011, 202],
          'procver_key': 'pv2'
         },
        { 'procver': 'realtime',
          'request_objs': [0, 1, 2],
          'expected_roots': [0, 1, 2],
          'expected_diaobjids': [0, 1, 2],
          'procver_key':'realtime'
         }
    ]

    extras = [
        {},
        { 'mjd_now': 60041. },
        # This next one is specifically to test the weighted source position case when there
        #   is only one detection with S/N>3
        { 'mjd_now': 60045.5, 'use_weighted_source_positions': 1, 'return_object_info': 1 },
        # ...and this is to explicitly test the case where there are no detections
        { 'mjd_now': 60039., 'use_weighted_source_positions': 1, 'return_object_info': 1 },
        { 'bands': 'r' },
        { 'bands': ['r'] },
        { 'include_source_positions': 1 },
        { 'return_object_info': 1 },
        { 'return_object_info': 1, 'return_diaobject_positions': 1 },
        { 'return_object_info': 1, 'include_base_procver': 1 },
        { 'return_object_info': 1, 'include_base_procver': 1, 'return_diaobject_positions': 1 },
        { 'use_weighted_source_positions': 1, 'include_base_procver': 1 },
        { 'use_weighted_source_positions': 1, 'return_diaobject_positions': 1 },
        { 'use_weighted_source_positions': 1, 'return_diaobject_positions': 1, 'return_object_info': 1 },
        { 'use_weighted_source_positions': 1, 'include_source_positions': 1,
          'return_object_info': 1, 'return_diaobject_positions': 1 },
        { 'use_weighted_source_positions': 1, 'include_source_positions': 1,
          'return_object_info': 1, 'return_diaobject_positions': 1 },
        { 'mjd_now': 60061., 'use_weighted_source_positions': 1, 'include_base_procver': 1,
          'include_source_positions': 1, 'return_diaobject_positions': 1, 'return_object_info': 1 },
    ]

    n = 0
    t0 = time.perf_counter()
    first = True
    for ltcvreq_raw in ltcvlist:
        for which in [ None, 'patch', 'detections', 'forced' ]:
            for extra in extras:
                # Make copies so we can mung them
                ltcvreq = copy.deepcopy( ltcvreq_raw )
                kwargs = copy.deepcopy( extra )

                # ...GAH, OK.  Object 2 (the third object) is only
                #   detected on mjd 60040, and Object 3 (the fourth
                #   object) is only detected on mjd 60050.  We also
                #   only have forced photometry from 10 days before
                #   the first detection.  So, we have to edit the
                #   expected list of objects when mjd_now is given
                #   and early enough.
                if ( 'mjd_now' in extra ):
                    if ( ( extra['mjd_now'] < 60040 ) or
                         ( ( which == 'detections' ) and ( extra['mjd_now'] < 60050 ) )
                        ):
                        if 3 in ltcvreq['expected_roots']:
                            ltcvreq['expected_roots'].remove( 3 )
                        for objid in [ 3, 203, 303 ]:
                            if objid in ltcvreq['expected_diaobjids']:
                                ltcvreq['expected_diaobjids'].remove(objid )
                    if ( ( extra['mjd_now'] < 60030 ) or
                         ( ( which == 'detections' ) and ( extra['mjd_now'] < 60040 ) )
                        ):
                        if 2 in ltcvreq['expected_roots']:
                            ltcvreq['expected_roots'].remove( 2 )
                        for objid in [ 2, 202, 302 ]:
                            if objid in ltcvreq['expected_diaobjids']:
                                ltcvreq['expected_diaobjids'].remove(objid )

                if ltcvreq['procver'] is None:
                    url = '/ltcv/getmanyltcvs'
                else:
                    url = f'/ltcv/getmanyltcvs/{ltcvreq["procver"]}'

                if first:
                    first = False
                    # Just check the bad JSON once
                    for foo in [ [], 'kitten' ]:
                        with pytest.raises( RuntimeError, match=( "^Error response from server, status 422: "
                                                                  "POST data must be a dict passed as json" ) ):
                            n += 1
                            fastdb_client.post( url, json=foo )

                    # Just check the bad paramter call once
                    with pytest.raises( RuntimeError, match=( "^Error response from server, status 422: "
                                                              "Unknown data parameters: {'foo'}" ) ):
                        n += 1
                        fastdb_client.post( url, json={'objids': [0,1,2,3], 'foo': 'bar'} )

                if ltcvreq['request_objs'] != 'do_not_request_any_objects_for_this_test':
                    kwargs['objids'] = ltcvreq['request_objs']
                if which is not None:
                    kwargs['which'] = which
                for i in range(2):
                    if i == 0:
                        res = fastdb_client.post( url, json=kwargs )
                    elif len(kwargs) == 0:
                        import pdb; pdb.set_trace()
                        res = fastdb_client.post( url )
                    else:
                        continue

                    n += 1
                    if which is None:
                        kwargs['which'] = 'patch'
                    if 'objids' in kwargs:
                        del kwargs['objids']
                    kwargs['rootid_is_uuid'] = False
                    check_ltcv( ltcvreq['procver_key'], ltcvreq['expected_roots'], ltcvreq['expected_diaobjids'],
                                res, **kwargs )


    FDBLogger.info( f"{n} requests in {time.perf_counter()-t0:.2f} sec." )



def test_getltcv( test_user, fastdb_client, set_of_lightcurves, lightcurve_checker ):
    roots = set_of_lightcurves
    check_ltcv = lightcurve_checker

    # Each element of the list is:
    #  * processing version
    #  * object to ask for
    #  * expected root ids returned
    #  * expected diaobjects returned

    ltcvlist = [ ( None, 202, [2], [202], 'pv2' ),
                 ( None, roots[2]['root'].id, [2], [202], 'pv2' ),
                 ( 'pvc_pv1', 100, [0], [100] ),
                 ( 'pvc_pv1', roots[0]['root'].id, [0], [100] ),
                 ( 'pvc_pv1', 1, None, None ),
                 ( 'pvc_pv2', roots[1]['root'].id, [1], [201, 2011] ),
                 ( 'pvc_pv2', 1, [1], [201, 2011] ),
                 ( 'pvc_pv2', 201, [1], [201, 2011] ),
                ]

    extras = [
        {},
        { 'mjd_now': 60041. },
        { 'bands': 'r' },
        { 'bands': ['r'] },
        { 'include_source_positions': 1 },
        { 'return_object_info': 1 },
        { 'return_object_info': 1, 'return_diaobject_positions': 1 },
        { 'return_object_info': 1, 'include_base_procver': 1 },
        { 'return_object_info': 1, 'include_base_procver': 1, 'return_diaobject_positions': 1 },
        { 'use_weighted_source_positions': 1, 'include_base_procver': 1 },
        { 'use_weighted_source_positions': 1, 'return_diaobject_positions': 1 },
        { 'use_weighted_source_positions': 1, 'return_diaobject_positions': 1, 'return_object_info': 1 },
        { 'use_weighted_source_positions': 1, 'include_source_positions': 1,
          'return_object_info': 1, 'return_diaobject_positions': 1 },
        { 'use_weighted_source_positions': 1, 'include_source_positions': 1,
          'return_object_info': 1, 'return_diaobject_positions': 1 },
        { 'mjd_now': 60061., 'use_weighted_source_positions': 1, 'include_base_procver': 1,
          'include_source_positions': 1, 'return_diaobject_positions': 1, 'return_object_info': 1 },
    ]


    n = 0
    t0 = time.perf_counter()
    first = True
    firstforreq = set()
    for ltcvreq in ltcvlist:
        for which in [ 'detections', 'forced', 'patch', None ]:
            for extra in extras:
                # So we can mung it
                extra = extra.copy()

                manual = None
                if 'manual_check' in extra:
                    manual = extra['manual_check']
                    del extra['manual_check']
                if which is not None:
                    extra['which'] = which

                if ltcvreq[0] is None:
                    url = f'/ltcv/getltcv/{ltcvreq[1]}'
                    pv = ltcvreq[4]
                else:
                    url = f'/ltcv/getltcv/{ltcvreq[0]}/{ltcvreq[1]}'
                    pv = ltcvreq[0]

                if first:
                    first = False
                    # ...we don't really need to test this 320 times...
                    with pytest.raises( RuntimeError, match=( "^Error response from server, status 422: "
                                                              "Unknown data parameters: {'foo'}" ) ):
                        n += 1
                        fastdb_client.post( url, json={'foo': 'bar'} )

                if ltcvreq[2] is None:
                    # ...and we don't have to test this with every set of options...
                    if ltcvreq not in firstforreq:
                        firstforreq.add( ltcvreq )
                        with pytest.raises( RuntimeError, match=( f"^Error response from server, status 422: "
                                                                  f"Could not find lightcurve for {ltcvreq[1]} "
                                                                  f"in processing version {ltcvreq[0]}" ) ):
                            n += 1
                            res = fastdb_client.post( url, json=extra )
                else:
                    n += 1
                    res = fastdb_client.post( url, json=extra )

                    if ( manual == 1 ) and ( pv == 'pvc_pv2' ):
                        for dex in range( len(res['objinfo']['diaobjectid']) ):
                            if res['objinfo']['diaobjectid'][dex] == 201:
                                # use_weighted_source_position won't be necessary for
                                #   object 201 in pv 2
                                assert res['objinfo']['pos_base_procver'][dex] is not None
                            elif res['objinfo']['diaobjectid'][dex] == 2011:
                                # But, object 2011 has no positions stored
                                assert res['objinfo']['pos_base_procver'][dex] is None
                    elif manual == 2:
                        assert len( res['objinfo']['diaobjectid'] ) == len( ltcvreq[3] )
                        # In this case, every object should have a weighted source position
                        assert all( pbv is None for pbv in res['objinfo']['pos_base_procver'] )

                    extra['rootid_is_uuid'] = False
                    check_ltcv( pv, ltcvreq[2], ltcvreq[3], res, single=True, **extra )

    FDBLogger.info( f"Sent {n} requests in {time.perf_counter()-t0:.2f} sec." )



# TODO : test getrandomltcv ; that might require the ability to pass a random seed for a reproducible test.
@pytest.mark.skip( reason="This test needs to be written" )
def test_getrandomltcv( test_user, fastdb_client, procver_collection, set_of_lightcurves ):
    assert False


def test_gethottransients( test_user, fastdb_client, set_of_lightcurves, lightcurve_checker ):
    check_ltcv = lightcurve_checker

    ltcvinfo = [
        { 'kwargs': { 'mjd_now': 60056., 'detected_since_mjd': 60035. },
          'passprocver': 'pvc_pv2',
          'testprocver': 'pv2',
          'exproot': [1, 2, 3],
          'expobj': [201, 2011, 202, 203]
         },
        { 'kwargs': { 'mjd_now': 60046., 'detected_since_mjd': 60035., },
          'passprocver': 'pvc_pv2',
          'testprocver': 'pv2',
          'exproot': [1, 2],
          'expobj': [201, 2011, 202],
         },
        { 'kwargs': { 'mjd_now': 60021., 'detected_in_last_days': 2 },
          'passprocver': 'pvc_pv2',
          'testprocver': 'pv2',
          'exproot': [0, 1],
          'expobj': [200, 201, 2011]
         },
        { 'kwargs': { 'mjd_now': 60041., 'detected_in_last_days': 2 },
          'passprocver': 'pvc_pv2',
          'testprocver': 'pv2',
          'exproot': [1, 2],
          'expobj': [201, 2011, 202]
         },
        # detected in last days defaults to 30
        { 'kwargs': { 'mjd_now': 60085. },
          'passprocver': 'pvc_pv2',
          'testprocver': 'pv2',
          'exproot': [1, 2, 3],
          'expobj': [201, 2011, 202, 203]
         },
        { 'kwargs': { 'mjd_now': 60095. },
          'passprocver': 'pvc_pv2',
          'testprocver': 'pv2',
          'exproot': [2],
          'expobj': [202]
         },
        { 'kwargs': { 'mjd_now': 60061. },
          'passprocver': 'realtime',
          'testprocver': 'realtime',
          'exproot': [1, 2],
          'expobj': [1, 2]
         },
        { 'kwargs': { 'mjd_now': 60061. },
          'passprocver': None,
          'testprocver': 'realtime',
          'exproot': [1, 2],
          'expobj': [1, 2]
         }
    ]

    extras = [
        {},
        { 'return_diaobject_positions': 1 },
        { 'return_diaobject_positions': 0 },
        { 'include_source_positions': 1 },
        { 'return_diaobject_positions': 1, 'include_source_positions': 1 },
        { 'include_base_procver': 1 },
        { 'include_base_procver': 1, 'return_diaobject_positions': 1 },
        { 'use_weighted_source_positions': 1, 'return_diaobject_positions': 1, 'include_base_procver': 1 },
        { 'use_weighted_source_positions': 1, 'return_diaobject_positions': 1, 'include_base_procver': 1 },
        { 'use_weighted_source_positions': 1, 'return_diaobject_positions': 1 },
    ]


    for lc in ltcvinfo:
        for source_patch in [ True, False, None ]:
            for extra in extras:

                if lc['passprocver'] is None:
                    url = '/ltcv/gethottransients'
                else:
                    url = f'/ltcv/gethottransients/{lc["passprocver"]}'

                kwargs = extra.copy()
                kwargs.update( lc['kwargs'] )
                if source_patch is not None:
                    kwargs['source_patch'] = source_patch

                res = fastdb_client.post( url, json=kwargs )

                for yank in [ 'source_patch', 'detected_since_mjd', 'detected_in_last_days' ]:
                    if yank in kwargs:
                        del kwargs[yank]
                kwargs['which'] = 'patch' if source_patch in ( True, None ) else 'forced'
                if 'return_diaobject_positions' not in kwargs:
                    # get_hot_ltcvs has a different default from many_object_ltcvs
                    kwargs['return_diaobject_positions'] = 1
                kwargs['rootid_is_uuid'] = False
                check_ltcv( lc['testprocver'], lc['exproot'], lc['expobj'], res,
                            return_object_info=True, **kwargs )


    # # This tests gets the same information as ../test_ltcv.py, only via
    # # the webap.  ../test_ltcv.py::test_get_hot_ltcvs makes sure that
    # # the direct call to ltcv.get_hot_ltcvs returns the right stuff.
    # # (Or, at least, it should.)  This test makes sure that what you get
    # # from the webap matches what you get from a direct call.

    # def _compare_direct_to_webap( df, objdf, res ):
    #     assert ( df.index.get_level_values('diaobjectid').unique()
    #              == np.array( [ r['diaobjectid'] for r in res ] ) ).all()
    #     assert ( objdf.index.get_level_values('diaobjectid').unique()
    #              == np.array( [ r['diaobjectid'] for r in res ] ) ).all()
    #     for objrow in res:
    #         objid = objrow['diaobjectid']
    #         subdf = df.loc[ objid ]
    #         subobjdf = objdf.loc[ objid ]
    #         assert objrow['rootid'] == str( subobjdf.rootid )
    #         assert objrow['ra'] == subobjdf.ra
    #         assert objrow['dec'] == subobjdf.dec
    #         assert objrow['zp'] == 31.4
    #         assert len(subdf) == len( objrow['photometry']['mjd'] )
    #         assert ( subdf.index.values == np.array( objrow['photometry']['mjd'] ) ).all()
    #         assert ( subdf.band == np.array( objrow['photometry']['band'] ) ).all()
    #         assert ( subdf.visit == np.array( objrow['photometry']['visit'] ) ).all()
    #         assert ( subdf.flux == np.array( objrow['photometry']['flux'] ) ) .all()
    #         assert ( subdf.fluxerr == np.array( objrow['photometry']['fluxerr'] ) ).all()
    #         assert ( subdf.isdet == np.array( objrow['photometry']['isdet'] ) ).all()
    #         if 'ispatch' in subdf.columns:
    #             assert ( subdf.ispatch == np.array( objrow['photometry']['ispatch'] ) ).all()

    # df, objdf, _ = ltcv.get_hot_ltcvs( 'pvc_pv3', detected_since_mjd=60035, mjd_now=60056 )
    # res = fastdb_client.post( '/ltcv/gethottransients', json={ 'processing_version': 'pvc_pv3',
    #                                                            'detected_since_mjd': 60035,
    #                                                            'mjd_now': 60056 } )
    # _compare_direct_to_webap( df, objdf, res )

    # df, objdf, _ = ltcv.get_hot_ltcvs( 'pvc_pv3', detected_since_mjd=60035, mjd_now=60046 )
    # res = fastdb_client.post( '/ltcv/gethottransients', json={ 'processing_version': 'pvc_pv3',
    #                                                            'detected_since_mjd': 60035,
    #                                                            'mjd_now': 60046 } )
    # _compare_direct_to_webap( df, objdf, res )

    # df, objdf, _ = ltcv.get_hot_ltcvs( 'pvc_pv3', detected_in_last_days=2, mjd_now=60021 )
    # res = fastdb_client.post( '/ltcv/gethottransients', json={ 'processing_version': 'pvc_pv3',
    #                                                            'detected_in_last_days': 2,
    #                                                            'mjd_now': 60021 } )
    # _compare_direct_to_webap( df, objdf, res )

    # df, objdf, _ = ltcv.get_hot_ltcvs( 'pvc_pv3', detected_in_last_days=2, mjd_now=60041 )
    # res = fastdb_client.post( '/ltcv/gethottransients', json={ 'processing_version': 'pvc_pv3',
    #                                                            'detected_in_last_days': 2,
    #                                                            'mjd_now': 60041 } )
    # _compare_direct_to_webap( df, objdf, res )

    # # detected_in_last_days defaults to 30
    # df, objdf, _ = ltcv.get_hot_ltcvs( 'pvc_pv3', mjd_now=60085 )
    # res = fastdb_client.post( '/ltcv/gethottransients', json={ 'processing_version': 'pvc_pv3',
    #                                                            'mjd_now': 60085 } )
    # _compare_direct_to_webap( df, objdf, res )
    # df, objdf, _ = ltcv.get_hot_ltcvs( 'pvc_pv3', mjd_now=60095 )
    # res = fastdb_client.post( '/ltcv/gethottransients', json={ 'processing_version': 'pvc_pv3',
    #                                                            'mjd_now': 60095 } )
    # _compare_direct_to_webap( df, objdf, res )

    # # Test source patch.  Gotta use pvc_pv1 for this.

    # df, objdf, _ = ltcv.get_hot_ltcvs( 'pvc_pv1', mjd_now=60031 )
    # res = fastdb_client.post( '/ltcv/gethottransients', json={ 'processing_version': 'pvc_pv1',
    #                                                            'mjd_now': 60031 } )
    # _compare_direct_to_webap( df, objdf, res )

    # df, objdf, _ = ltcv.get_hot_ltcvs( 'pvc_pv1', mjd_now=60031, source_patch=True )
    # res = fastdb_client.post( '/ltcv/gethottransients', json={ 'processing_version': 'pvc_pv1',
    #                                                            'mjd_now': 60031,
    #                                                            'source_patch': True } )
    # _compare_direct_to_webap( df, objdf, res )
