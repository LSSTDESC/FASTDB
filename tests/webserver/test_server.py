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
