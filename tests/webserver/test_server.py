import re
import uuid
import pytest

from psycopg import sql
import numpy as np

import db
import ltcv


# If you've manually loaded your test database, but haven't
#   manually inserted a user, from /code/tests run:
#      pytest -v --trace webserver/test_server.py::test_just_make_a_user
#   and, when you get to the Pdb() prompt, go look at the webap.
def test_just_make_a_user( test_user ):
    pass


def test_getprocvers( procver_collection, test_user, fastdb_client ):
    res = fastdb_client.post( '/getprocvers' )
    assert isinstance( res, dict )
    assert res['status'] == 'ok'
    assert res['procvers'] == [ 'default', 'pvc_pv1', 'pvc_pv2', 'pvc_pv3', 'realtime' ]


def test_procver( procver_collection, test_user, fastdb_client, procver_postimes ):
    allbpvs, allpvs, _pvinfo = procver_collection

    def check_res( pv, bpvs, aliases=[] ):
        assert res['id'] == str( allpvs[pv].id )
        assert res['description'] == allpvs[pv].description
        assert res['aliases'] == aliases
        tables = { 'diaobject', 'diasource', 'diaforcedsource', 'diaobject_position', 'host_galaxy' }
        assert set( res['base_procvers'].keys() ) == tables
        # ...reverse engineering the priorities that were set up in procver_collection in conftest.py...
        bpvprios = list( range( len(bpvs) ) )
        bpvprios.reverse()
        bwpostimes = procver_postimes.copy()
        bwpostimes.reverse()
        for tab in tables:
            if tab == 'diaobject_position':
                bpvkeys = [ f'{b}_{tab}_{t}' for b in bpvs for t in bwpostimes ]
                prios = [ 10 * b + t for b in bpvprios for t in range( len(procver_postimes)-1, -1, -1 ) ]
            else:
                bpvkeys = [ f'{b}_{tab}' for b in bpvs ]
                prios = bpvprios
            assert len( res['base_procvers'][tab] ) == len( bpvkeys )
            for i, (p, b) in enumerate( zip( prios, bpvkeys ) ):
                assert res['base_procvers'][tab][i] == [ allbpvs[b].description, p ]

    for suffix in [ 'default', allpvs['pv2'].description, allpvs['pv2'].id ]:
        res = fastdb_client.post( f'/procver/{suffix}' )
        check_res( 'pv2', [ 'bpv2a', 'bpv2' ], [ 'default' ] )

    for suffix in [ allpvs['pv3'].description, allpvs['pv3'].id ]:
        res = fastdb_client.post( f'/procver/{suffix}' )
        check_res( 'pv3', [ 'bpv3' ] )

    for suffix in [ allpvs['pv1'].description, allpvs['pv1'].id ]:
        res = fastdb_client.post( f'/procver/{suffix}' )
        check_res( 'pv1', [ 'bpv1b', 'bpv1a', 'bpv1' ] )

    for suffix in [ allpvs['realtime'].description, allpvs['realtime'].id ]:
        res = fastdb_client.post( f'/procver/{suffix}' )
        check_res( 'realtime', [ 'realtime' ] )


    # Temporarily reduce retries to 0 so these will fail fast
    orig_retries = fastdb_client.retries
    try:
        fastdb_client.retries = 0
        with pytest.raises( RuntimeError, match=( 'Error response from server, status 422: '
                                                  'Unknown processing version 64741' ) ):
            res = fastdb_client.post( '/procver/64741' )
        with pytest.raises( RuntimeError, match=( 'Error response from server, status 422: '
                                                  'Unknown processing version does_not_exist' ) ):
            res = fastdb_client.post( '/procver/does_not_exist' )
    finally:
        fastdb_client.retries = orig_retries


def test_base_procver( procver_collection, test_user, fastdb_client ):
    bpvs, _pvs, _pvinfo = procver_collection

    badbpv = str( uuid.uuid4() )
    with pytest.raises( RuntimeError, match=( f'Error response from server, status 422: '
                                              f'Unknown base processing version {badbpv}' ) ):
        _ = fastdb_client.post( f'/baseprocver/{badbpv}' )

    for k, bpv in bpvs.items():
        with pytest.raises( RuntimeError, match=( 'Error response from server, status 422: table is required '
                                                  'when base_processing_version is not a uuid' ) ):
            _ = fastdb_client.post( f'/baseprocver/{bpv.description}' )

        for suffix in [ bpv.id, f'{bpv.description}/{bpv._table}' ]:
            res = fastdb_client.post( f'/baseprocver/{suffix}' )
            assert res['id'] == str( bpv.id )
            assert res['table'] == bpv._table
            assert res['description'] == bpv.description
            if k[0:8] == 'realtime':
                assert res['procvers'] == [ 'realtime' ]
            else:
                mat = re.search( r'pv(\d)', k )
                pv = f'pvc_pv{mat.group(1)}'
                assert res['procvers'] == [ pv ]


def test_flags_def( fastdb_client ):
    res = fastdb_client.post( "/flagsdefinitions" )
    compare = {
        'flags': [
            { 'mask': 0x00000001, 'definition': 'centroid_flag' },
            { 'mask': 0x00000002, 'definition': 'apFlux_flag' },
            { 'mask': 0x00000004, 'definition': 'apFlux_flag_apertureTruncated' },
            { 'mask': 0x00000008, 'definition': 'isNegative' },
            { 'mask': 0x00000010, 'definition': 'psfFlux_flag' },
            { 'mask': 0x00000020, 'definition': 'psfFlux_flag_edge' },
            { 'mask': 0x00000040, 'definition': 'psfFlux_flag_noGoodPixels' },
            { 'mask': 0x00000080, 'definition': 'trail_flag_edge' },
            { 'mask': 0x00000100, 'definition': 'forced_PsfFlux_flag' },
            { 'mask': 0x00000200, 'definition': 'forced_PsfFlux_flag_edge' },
            { 'mask': 0x00000400, 'definition': 'forced_PsfFlux_flag_noGoodPixels' },
            { 'mask': 0x00000800, 'definition': 'shape_flag' },
            { 'mask': 0x00001000, 'definition': 'shape_flag_no_pixels' },
            { 'mask': 0x00002000, 'definition': 'shape_flag_not_contained' },
            { 'mask': 0x00004000, 'definition': 'shape_flag_parent_source' },
            { 'mask': 0x00008000, 'definition': 'isDipole' },
            { 'mask': 0x00010000, 'definition': 'dipoleFitAttempted' },
            { 'mask': 0x00020000, 'definition': 'glint_trail' },
            { 'mask': 0x00040000, 'definition': 'trail_flag' },
            { 'mask': 0x00080000, 'definition': 'invalidPsfFlag' },
            { 'mask': 0x00100000, 'definition': None },
            { 'mask': 0x00200000, 'definition': 'diff_PixelFlags_nodataCenter' },
            { 'mask': 0x00400000, 'definition': None },
            { 'mask': 0x00800000, 'definition': None },
            { 'mask': 0x01000000, 'definition': None },
            { 'mask': 0x02000000, 'definition': None },
            { 'mask': 0x04000000, 'definition': None },
            { 'mask': 0x08000000, 'definition': None },
            { 'mask': 0x10000000, 'definition': None },
            { 'mask': 0x20000000, 'definition': None },
            { 'mask': 0x40000000, 'definition': None }
        ],
        'flags_inverse': {
            'centroid_flag':                    { 'mask': 0x00000001, 'bit': 0 },
            'apFlux_flag':                      { 'mask': 0x00000002, 'bit': 1 },
            'apFlux_flag_apertureTruncated':    { 'mask': 0x00000004, 'bit': 2 },
            'isNegative':                       { 'mask': 0x00000008, 'bit': 3 },
            'psfFlux_flag':                     { 'mask': 0x00000010, 'bit': 4 },
            'psfFlux_flag_edge':                { 'mask': 0x00000020, 'bit': 5 },
            'psfFlux_flag_noGoodPixels':        { 'mask': 0x00000040, 'bit': 6 },
            'trail_flag_edge':                  { 'mask': 0x00000080, 'bit': 7 },
            'forced_PsfFlux_flag':              { 'mask': 0x00000100, 'bit': 8 },
            'forced_PsfFlux_flag_edge':         { 'mask': 0x00000200, 'bit': 9 },
            'forced_PsfFlux_flag_noGoodPixels': { 'mask': 0x00000400, 'bit': 10 },
            'shape_flag':                       { 'mask': 0x00000800, 'bit': 11 },
            'shape_flag_no_pixels':             { 'mask': 0x00001000, 'bit': 12 },
            'shape_flag_not_contained':         { 'mask': 0x00002000, 'bit': 13 },
            'shape_flag_parent_source':         { 'mask': 0x00004000, 'bit': 14 },
            'isDipole':                         { 'mask': 0x00008000, 'bit': 15 },
            'dipoleFitAttempted':               { 'mask': 0x00010000, 'bit': 16 },
            'glint_trail':                      { 'mask': 0x00020000, 'bit': 17 },
            'trail_flag':                       { 'mask': 0x00040000, 'bit': 18 },
            'invalidPsfFlag':                   { 'mask': 0x00080000, 'bit': 19 },
            'diff_PixelFlags_nodataCenter':     { 'mask': 0x00200000, 'bit': 21 }
        },
        'pixelflags': [
            { 'mask': 0x00000001, 'definition': 'pixelFlags' },
            { 'mask': 0x00000002, 'definition': 'pixelFlags_bad' },
            { 'mask': 0x00000004, 'definition': 'pixelFlags_cr' },
            { 'mask': 0x00000008, 'definition': 'pixelFlags_crCenter' },
            { 'mask': 0x00000010, 'definition': 'pixelFlags_edge' },
            { 'mask': 0x00000020, 'definition': 'pixelFlags_nodata' },
            { 'mask': 0x00000040, 'definition': 'pixelFlags_nodataCenter' },
            { 'mask': 0x00000080, 'definition': 'pixelFlags_interpolated' },
            { 'mask': 0x00000100, 'definition': 'pixelFlags_interpolatedCenter' },
            { 'mask': 0x00000200, 'definition': 'pixelFlags_offimage' },
            { 'mask': 0x00000400, 'definition': 'pixelFlags_saturated' },
            { 'mask': 0x00000800, 'definition': 'pixelFlags_saturatedCenter' },
            { 'mask': 0x00001000, 'definition': 'pixelFlags_suspect' },
            { 'mask': 0x00002000, 'definition': 'pixelFlags_suspectCenter' },
            { 'mask': 0x00004000, 'definition': 'pixelFlags_streak' },
            { 'mask': 0x00008000, 'definition': 'pixelFlags_streakCenter' },
            { 'mask': 0x00010000, 'definition': 'pixelFlags_injected' },
            { 'mask': 0x00020000, 'definition': 'pixelFlags_injectedCenter' },
            { 'mask': 0x00040000, 'definition': 'pixelFlags_injected_template' },
            { 'mask': 0x00080000, 'definition': 'pixelFlags_injected_templateCenter' },
            { 'mask': 0x00100000, 'definition': None },
            { 'mask': 0x00200000, 'definition': None },
            { 'mask': 0x00400000, 'definition': None },
            { 'mask': 0x00800000, 'definition': None },
            { 'mask': 0x01000000, 'definition': None },
            { 'mask': 0x02000000, 'definition': None },
            { 'mask': 0x04000000, 'definition': None },
            { 'mask': 0x08000000, 'definition': None },
            { 'mask': 0x10000000, 'definition': None },
            { 'mask': 0x20000000, 'definition': None },
            { 'mask': 0x40000000, 'definition': None }
        ],
        'pixelflags_inverse': {
            'pixelFlags':                         { 'mask': 0x00000001, 'bit': 0 },
            'pixelFlags_bad':                     { 'mask': 0x00000002, 'bit': 1 },
            'pixelFlags_cr':                      { 'mask': 0x00000004, 'bit': 2 },
            'pixelFlags_crCenter':                { 'mask': 0x00000008, 'bit': 3 },
            'pixelFlags_edge':                    { 'mask': 0x00000010, 'bit': 4 },
            'pixelFlags_nodata':                  { 'mask': 0x00000020, 'bit': 5 },
            'pixelFlags_nodataCenter':            { 'mask': 0x00000040, 'bit': 6 },
            'pixelFlags_interpolated':            { 'mask': 0x00000080, 'bit': 7 },
            'pixelFlags_interpolatedCenter':      { 'mask': 0x00000100, 'bit': 8 },
            'pixelFlags_offimage':                { 'mask': 0x00000200, 'bit': 9 },
            'pixelFlags_saturated':               { 'mask': 0x00000400, 'bit': 10 },
            'pixelFlags_saturatedCenter':         { 'mask': 0x00000800, 'bit': 11 },
            'pixelFlags_suspect':                 { 'mask': 0x00001000, 'bit': 12 },
            'pixelFlags_suspectCenter':           { 'mask': 0x00002000, 'bit': 13 },
            'pixelFlags_streak':                  { 'mask': 0x00004000, 'bit': 14 },
            'pixelFlags_streakCenter':            { 'mask': 0x00008000, 'bit': 15 },
            'pixelFlags_injected':                { 'mask': 0x00010000, 'bit': 16 },
            'pixelFlags_injectedCenter':          { 'mask': 0x00020000, 'bit': 17 },
            'pixelFlags_injected_template':       { 'mask': 0x00040000, 'bit': 18 },
            'pixelFlags_injected_templateCenter': { 'mask': 0x00080000, 'bit': 19 },
        }
    }

    assert res == compare


def test_getdiaobjectinfo( fastdb_client, procver_collection, set_of_lightcurves ):
    roots = set_of_lightcurves

    # Temporarily reduce retries to 0 so the ones that are supposed to fail will fail fast
    orig_retries = fastdb_client.retries
    fastdb_client.retries = 0

    try:
        with pytest.raises( RuntimeError, match=( 'Error response from server, status 422: '
                                                  'must pass either objids or objids_table' ) ):
            res = fastdb_client.post( '/getdiaobjectinfo' )
        with pytest.raises( RuntimeError, match=( 'Error response from server, status 422: '
                                                  'Conflicting processing versions; .* specified in the URL, '
                                                  'but .* passed in the body' ) ):
            res = fastdb_client.post( '/getdiaobjectinfo/realtime/0', json={ 'processing_version': 'pvc_pv2' } )
        with pytest.raises( RuntimeError, match=( 'Error response from server, status 422: '
                                                  'objids must be a list of integers or a list of uuids' ) ):
            res = fastdb_client.post( '/getdiaobjectinfo/foo', json={ 'processing_version': 'pvc_pv2' } )
        with pytest.raises( RuntimeError, match=( 'Error response from server, status 422: '
                                                  'objids must be a list of integers or a list of uuids' ) ):
            res = fastdb_client.post( '/getdiaobjectinfo/realtime' )

        res = fastdb_client.post( f"/getdiaobjectinfo/pvc_pv2/{str(roots[0]['root'].id)}" )
        assert res['diaobjectid'] == [ [ 200 ] ]
        res2 = fastdb_client.post( "/getdiaobjectinfo/pvc_pv2", json={ 'objectids': [ str(roots[0]['root'].id) ] } )
        assert res2 == res
        res2 = fastdb_client.post( "/getdiaobjectinfo/pvc_pv2", json={ 'objectids': str(roots[0]['root'].id) } )
        assert res2 == res
        res2 = fastdb_client.post( "/getdiaobjectinfo", json={ 'processing_version': 'pvc_pv2',
                                                               'objectids': [ str(roots[0]['root'].id) ] } )
        assert res2 == res

        # BROKEN.  Right now the set_of_lightcurves doesn't have objects in the
        #   default processing version.  Issue #70
        # res = fasdtb_client.post( "/getdiaobjectinfo", json={ 'objectids': [ str(roots[0]['root'].id) ] } )
        # res = fastdb_client.post( "/getdiaobjectinfo", json={ 'objectids': [ 200, 201, 202 ] } )
        # assert res['diaobjectid'] == [ 200, 201, 202 ]
        # assert res['rootid'] == [ str(roots[0]['root'].id), str(roots[1]['root'].id), str(roots[2]['root'].id) ]

        res = fastdb_client.post( "/getdiaobjectinfo/pvc_pv2", json={ 'objectids': [ 200, 201 ] } )
        assert sorted( res['diaobjectid'] ) == [ [ 200 ], [ 201, 2011 ] ]
        assert res['rootid'] == sorted( [ str(roots[i]['root'].id) for i in [ 0, 1 ] ] )
        assert set( res.keys() ) == { 'diaobjectid', 'rootid', 'obj_base_procver', 'pos_base_procver',
                                      'ra', 'dec',
                                      'diaobject_ra', 'diaobject_dec', 'diaobject_raerr',
                                      'diaobject_decerr', 'diaobject_ra_dec_cov' }

        res = fastdb_client.post( "/getdiaobjectinfo/pvc_pv2", json={ 'objectids': [ 200, 201, 202 ],
                                                                      'return_diaobject_positions': 0 } )
        assert sorted( res['diaobjectid'] ) == [ [ 200 ], [ 201, 2011 ], [ 202 ] ]
        assert res['rootid'] == sorted( [ str(roots[i]['root'].id) for i in [ 0, 1, 2 ] ] )
        assert set( res.keys() ) == { 'rootid', 'ra', 'dec', 'diaobjectid', 'obj_base_procver' }


    finally:
        fastdb_client.retries = orig_retries


def test_objectsearch( fastdb_client, procver_collection, objstats_realtime_view, check_search_vs_expected ):
    # These tests should be exacly the same as what's in test_ltcv.py::test_object_search

    tests = [ { 'pv': 'pvc_pv2',
                'band': None,
                'roots': [0, 1],
                'conditions': { 'firstdet_mjd_min': 59999, 'firstdet_mjd_max': 60030 }
               },
              { 'pv': 'pvc_pv2',
                'band': None,
                'roots': [1, 2, 3],
                'conditions': { 'lastdet_mjd_min': 60059, 'lastdet_mjd_max': 60081 }
               },
              { 'pv': 'pvc_pv2',
                'band': None,
                'roots': [2, 3],
                'conditions': { 'lastdet_mjd_min': 60059, 'lastdet_mjd_max': 60081,
                                'firstdet_mjd_min': 60039 }
               },
              { 'pv': 'pvc_pv2',
                'band': None,
                'roots': [1, 2],
                'conditions': { 'maxdet_mjd_min': 60034, 'maxdet_mjd_max': 60051 }
               },
              { 'pv': 'pvc_pv2',
                'band': None,
                'roots': [ 1, 2 ],
                'conditions': { 'maxdet_flux_min': np.pow( 10,  (31.4 - 23.1) / 2.5 ) }
               },
              { 'pv': 'pvc_pv2',
                'band': None,
                'roots': [ 1 ],
                'conditions': { 'nsn10_min': 4 }
               },
              { 'pv': 'pvc_pv2',
                'band': None,
                'roots': [ 1, 2 ],
                'conditions': { 'nsn5_min': 2 }
               },
              { 'pv': 'pvc_pv2',
                'band': None,
                'roots': [ 2 ],
                'conditions': { 'nsn5_min': 2, 'nsn5_max': 7 }
               },
              { 'pv': 'pvc_pv2',
                'band': None,
                'roots': [ 1 ],
                'conditions': { 'ndets23_min': 2 }
               },
              { 'pv': 'pvc_pv2',
                'band': None,
                'roots': [ 1, 2 ],
                'conditions': { 'ndets24_min': 5 }
               },
              { 'pv': 'pvc_pv2',
                'band': None,
                'roots': [0, 2, 3 ],
                'conditions': { 'ndets22_max': 0 }
               },
              { 'pv': 'pvc_pv2',
                'band': None,
                'roots': [ 2 ],
                'conditions': { 'ndets24_min': 2, 'ndets22_max': 0  }
               },
              # Probably test other things too.... like bands....
             ]

    made_procvers = { 'realtime' }

    try:
        for test in tests:
            if test['pv'] not in made_procvers:
                with pytest.raises( RuntimeError, match="Can't do object search, materialized view.*doesn't exist" ):
                    _ = fastdb_client.post( f"/objectsearch/{test['pv']}", json=test['conditions'] )

                ltcv.create_object_stats_materialized_view( test['pv'] )
                made_procvers.add( test['pv'] )

            results = fastdb_client.post( f"/objectsearch/{test['pv']}", json=test['conditions'] )
            check_search_vs_expected( test['pv'], test['roots'], test['band'], results )

    finally:
        with db.DBCon() as con:
            for procver in made_procvers:
                if procver == 'realtime':
                    continue

                con.execute_nofetch( sql.SQL( "DROP MATERIALIZED VIEW {view}" )
                                     .format( view=sql.Identifier( f'objstatscomb_{procver}' ) ) )
                con.execute_nofetch( sql.SQL( "DROP MATERIALIZED VIEW {view}" )
                                     .format( view=sql.Identifier( f'objstats_{procver}' ) ) )

            con.commit()


# THIS TEST MUST GO AFTER test_objectsearch BECASUE THAT TEST DEPENDS ON
#   THE objstats_pvc_pv2_view AND objstats_pvc_pv3_view MODULE SCOPE FIXTURES
#   NOT HAVING BEEN RUN YET.
def test_countthings( set_of_lightcurves, objstats_realtime_view, objstats_pvc_pv2_view,
                      objstats_pvc_pv3_view, test_user, fastdb_client ):
    for pv in ( '', 'pvc_pv2', 'pvc_pv3', 'default' ):
        for table in ( 'rootobject', 'rootdiaobject', 'rootid' ):
            suffix = table if pv == '' else f'{table}/{pv}'
            res = fastdb_client.post( f'/count/{suffix}' )
            assert res['status'] == 'ok'
            assert res['table'] == 'rootid'
            # Even though there are no diaobjectids in pv3, the rootids are there
            assert res['count'] == 4

        # Counting diabobject isn't understood
        # for table in ( 'diaobject', 'object' ):
        #     suffix = table if pv == '' else f'{table}/{pv}'
        #     res = fastdb_client.post( f'/count/{suffix}' )
        #     assert res['status'] == 'ok'
        #     assert res['table'] == 'diaobject'
        #     assert res['count'] == ( 0 if pv == 'pvc_pv3' else 5 )

        for table in ( 'diasource', 'source' ):
            suffix = table if pv == '' else f'{table}/{pv}'
            res = fastdb_client.post( f'/count/{suffix}' )
            assert res['status'] == 'ok'
            assert res['table'] == 'diasource'
            assert res['count'] == 52

        for table in ( 'diaforcedsource', 'forced' ):
            suffix = table if pv == '' else f'{table}/{pv}'
            res = fastdb_client.post( f'/count/{suffix}' )
            assert res['status'] == 'ok'
            assert res['table'] == 'diaforcedsource'
            assert res['count'] == 100


    for table in ( 'rootobject', 'rootdiaobject', 'rootid' ):
        res = fastdb_client.post( f'/count/{table}/realtime' )
        assert res['status'] == 'ok'
        assert res['table'] == ( 'diaobject' if table in ( 'diaobject', 'object' ) else 'rootid' )
        assert res['count'] == 3

    for table in ( 'diasource', 'source' ):
        res = fastdb_client.post( f'/count/{table}/realtime' )
        assert res['status'] == 'ok'
        assert res['table'] == 'diasource'
        assert res['count'] == 39

    for table in ( 'diaforcedsource', 'forced' ):
        res = fastdb_client.post( f'/count/{table}/realtime' )
        assert res['status'] == 'ok'
        assert res['table'] == 'diaforcedsource'
        assert res['count'] == 55

    # Temporarily reduce retries to 0 so these will fail fast
    orig_retries = fastdb_client.retries
    try:
        fastdb_client.retries = 0
        with pytest.raises( RuntimeError, match=( 'Error response from server, status 422: Unknown '
                                                  'processing version this_processing_version_does_not_exist' ) ):
            res = fastdb_client.post( '/count/root/this_processing_version_does_not_exist' )
        with pytest.raises( RuntimeError, match=( 'Error response from server, status 422: '
                                                  'Unknown thing to count: this_table_does_not_exist' ) ):
            res = fastdb_client.post( '/count/this_table_does_not_exist' )
    finally:
        fastdb_client.retries = orig_retries


# ======================================================================

# This next one loads up with the database with the ELAsTiCC2 excerpt
#   loaded from snana files, the first 90 days of that having been run
#   through the alert cycle (using the "fast" method of loading the cached
#   data; see services/test_sourceimporter.py), and with edp2 loaded from
#   the test parquet files.
#
# ELAsTiCC2 data are in processing version "realtime", and edp2 data are in
#   processing version "test_load_edp2".
#
# Run this with
#   pytest -v --trace webserver/test_server.py::test_elasticc2_edp2_loaded
# And now you can experiment with the webap at:
#   http://localhost:8080
# (unless you've used the WEBPORT env var to change the port the webap
# listens when starting the docker compose environment).  You can also
# get to it via https, but you will have to confirm a security override
# on your browser to use a self-signed cert.  Log in with user 'test' and
# password 'test_password'.
def test_elasticc2_edp2_loaded( test_user, alerts_materialized_view, edp2_materialized_view ):
    pass
