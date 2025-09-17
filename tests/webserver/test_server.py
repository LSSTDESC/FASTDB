import re
import pytest


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


def test_procver( procver_collection, test_user, fastdb_client ):
    bpvs, pvs = procver_collection

    for suffix in [ 'default', pvs['pv3'].description, pvs['pv3'].id ]:
        res = fastdb_client.post( f'/procver/{suffix}' )
        assert res['id'] == str( pvs['pv3'].id )
        assert res['description'] == pvs['pv3'].description
        assert res['aliases'] == [ 'default' ]
        assert res['base_procvers'] == [ bpvs[i].description for i in [ 'bpv3' ] ]

    for suffix in [ pvs['pv2'].description, pvs['pv2'].id ]:
        res = fastdb_client.post( f'/procver/{suffix}' )
        assert res['id'] == str( pvs['pv2'].id )
        assert res['description'] == pvs['pv2'].description
        assert res['aliases'] == []
        assert res['base_procvers'] == [ bpvs[i].description for i in [ 'bpv2a', 'bpv2' ] ]

    for suffix in [ pvs['pv1'].description, pvs['pv1'].id ]:
        res = fastdb_client.post( f'/procver/{suffix}' )
        assert res['id'] == str( pvs['pv1'].id )
        assert res['description'] == pvs['pv1'].description
        assert res['aliases'] == []
        assert res['base_procvers'] == [ bpvs[i].description for i in [ 'bpv1b', 'bpv1a', 'bpv1' ] ]

    for suffix in [ pvs['realtime'].description, pvs['realtime'].id ]:
        res = fastdb_client.post( f'/procver/{suffix}' )
        assert res['id'] == str( pvs['realtime'].id )
        assert res['description'] == pvs['realtime'].description
        assert res['aliases'] == []
        assert res['base_procvers'] == [ bpvs[i].description for i in [ 'realtime' ] ]

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
    bpvs, _pvs = procver_collection

    for k, bpv in bpvs.items():
        for suffix in [ k, bpv.id ]:
            suffix = f'pvc_{k}' if k != 'realtime' else suffix
            res = fastdb_client.post( f'/baseprocver/{suffix}' )
            assert res['id'] == str( bpv.id )
            assert res['description'] == bpv.description
            if k == 'realtime':
                assert res['procvers'] == [ 'realtime' ]
            else:
                match = re.search( r'pv(\d)', k )
                pv = f'pvc_pv{match.group(1)}'
                assert res['procvers'] == [ pv ]


def test_countthings( set_of_lightcurves, test_user, fastdb_client ):
    for pv in ( '', 'pvc_pv2', 'pvc_pv3', 'default' ):
        for table in ( 'diaobject', 'object' ):
            suffix = table if pv == '' else f'{table}/{pv}'
            res = fastdb_client.post( f'/count/{suffix}' )
            assert res['status'] == 'ok'
            assert res['table'] == 'diaobject'
            assert res['count'] == ( 4 if pv == 'pvc_pv2' else 0 )

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


    for table in ( 'diaobject', 'object' ):
        res = fastdb_client.post( f'/count/{table}/realtime' )
        assert res['status'] == 'ok'
        assert res['table'] == 'diaobject'
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
        with pytest.raises( RuntimeError, match='Got status 500 trying to connect' ):
            res = fastdb_client.post( '/count/this_table_does_not_exist' )
    finally:
        fastdb_client.retries = orig_retries


def test_getdiaobjectinfo( fastdb_client, procver_collection, set_of_lightcurves ):
    roots = set_of_lightcurves

    # Temporarily reduce retries to 0 so the ones that are supposed to fail will fail fast
    orig_retries = fastdb_client.retries
    fastdb_client.retries = 0

    try:
        with pytest.raises( RuntimeError ):
            res = fastdb_client.post( '/getdiaobjectinfo' )
        with pytest.raises( RuntimeError ):
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

        # BROKEN.  Right now the set_of_lightcurves doesn't have objects inthe
        #   default processing version.  Issue #70
        # res = fasdtb_client.post( "/getdiaobjectinfo", json={ 'objectids': [ str(roots[0]['root'].id) ] } )

        res = fastdb_client.post( "/getdiaobjectinfo", json={ 'objectids': [ 200, 201, 202 ] } )
        assert res['diaobjectid'] == [ 200, 201, 202 ]
        assert res['rootid'] == [ str(roots[0]['root'].id), str(roots[1]['root'].id), str(roots[2]['root'].id) ]
        assert set( res.keys() ) == { 'diaobjectid', 'rootid', 'base_procver_id', 'radecmjdtai', 'validitystart',
                                      'validityend', 'ra', 'raerr', 'dec', 'decerr', 'ra_dec_cov',
                                      'nearbyextobj1', 'nearbyextobj1id', 'nearbyextobj1sep',
                                      'nearbyextobj2', 'nearbyextobj2id', 'nearbyextobj2sep',
                                      'nearbyextobj3', 'nearbyextobj3id', 'nearbyextobj3sep',
                                      'nearbylowzgal', 'nearbylowzgalsep', 'parallax', 'parallaxerr',
                                      'pmra', 'pmraerr', 'pmra_parallax_cov',
                                      'pmdec', 'pmdecerr', 'pmdec_parallax_cov', 'pmra_pmdec_cov' }

        res = fastdb_client.post( "/getdiaobjectinfo", json={ 'objectids': [ 200, 201, 202 ],
                                                              'columns': [ 'diaobjectid', 'ra', 'dec' ] } )
        assert res['diaobjectid'] == [ 200, 201, 202 ]
        assert set( res.keys() ) == { 'diaobjectid', 'ra', 'dec' }

    finally:
        fastdb_client.retries = orig_retries
