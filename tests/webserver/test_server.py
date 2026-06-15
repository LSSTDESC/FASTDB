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
        with pytest.raises( RuntimeError, match='Got status 500 trying to connect' ):
            res = fastdb_client.post( '/procver/64741' )
        with pytest.raises( RuntimeError, match='Got status 500 trying to connect' ):
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


def test_countthings( set_of_lightcurves, test_user, fastdb_client ):
    for pv in ( '', 'pvc_pv2', 'pvc_pv3', 'default' ):
        for table in ( 'rootobject', 'rootdiaobject', 'rootid' ):
            suffix = table if pv == '' else f'{table}/{pv}'
            res = fastdb_client.post( f'/count/{suffix}' )
            assert res['status'] == 'ok'
            assert res['table'] == 'rootid'
            assert res['count'] == ( 0 if pv == 'pvc_pv3' else 4 )

        for table in ( 'diaobject', 'object' ):
            suffix = table if pv == '' else f'{table}/{pv}'
            res = fastdb_client.post( f'/count/{suffix}' )
            assert res['status'] == 'ok'
            assert res['table'] == 'diaobject'
            assert res['count'] == ( 0 if pv == 'pvc_pv3' else 5 )

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


    for table in ( 'diaobject', 'object', 'rootobject', 'rootdiaobject', 'rootid' ):
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
        with pytest.raises( RuntimeError, match='Got status 500 trying to connect' ):
            res = fastdb_client.post( '/count/diaobject/this_processing_version_does_not_exist' )
        with pytest.raises( RuntimeError, match=( 'Error response from server, status 422: '
                                                  'Unknown thing to count: this_table_does_not_exist' ) ):
            res = fastdb_client.post( '/count/this_table_does_not_exist' )
    finally:
        fastdb_client.retries = orig_retries


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
            res = fastdb_client.post( '/getdiaobjectinfo/realtime', json={ 'processing_version': 'pvc_pv2' } )
        with pytest.raises( RuntimeError, match=( 'Error response from server, status 422: '
                                                  'must pass either objids or objids_table' ) ):
            res = fastdb_client.post( '/getdiaobjectinfo/realtime' )

        res = fastdb_client.post( f"/getdiaobjectinfo/pvc_pv2/{str(roots[0]['root'].id)}" )
        assert res['diaobjectid'] == [ 200 ]
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
        assert res['diaobjectid'] == [ 200, 201 ]
        assert res['rootid'] == [ str(roots[i]['root'].id) for i in [ 0, 1 ] ]
        assert set( res.keys() ) == { 'diaobjectid', 'rootid', 'obj_base_procver', 'pos_base_procver',
                                      'ra', 'dec', 'raerr', 'decerr', 'ra_dec_cov' }

        res = fastdb_client.post( "/getdiaobjectinfo/pvc_pv2", json={ 'objectids': [ 200, 201, 202 ],
                                                                      'columns': [ 'diaobjectid', 'ra', 'dec' ] } )
        assert res['diaobjectid'] == [ 200, 201, 202 ]
        assert set( res.keys() ) == { 'diaobjectid', 'ra', 'dec' }


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
