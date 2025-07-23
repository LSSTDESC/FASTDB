import pytest
import db


# Include the procver fixture since it's a session scoped fixture,
#  so somebody else might already have included it.  The tests
#  need to take that into account, so put the fixture here tox make
#  sure it's run even if this test is run by itself.
@pytest.fixture( scope='module' )
def server_test_processing_versions( procver ):
    try:
        with db.DB() as con:
            cursor = con.cursor()
            cursor.execute( "INSERT INTO processing_version(id,description,validity_start,validity_end) "
                            "VALUES (64738, 'test_server_1', NOW(), NULL)" )
            cursor.execute( "INSERT INTO processing_version(id,description,validity_start,validity_end) "
                            "VALUES (64739, 'test_server_2', NOW(), NULL)" )
            cursor.execute( "INSERT INTO processing_version(id,description,validity_start,validity_end) "
                            "VALUES (64740, 'test_server_3', NOW(), NULL)" )
            cursor.execute( "INSERT INTO processing_version_alias(id,description) "
                            "VALUES (64738, 'test_server_1_alias_1')" )
            cursor.execute( "INSERT INTO processing_version_alias(id,description) "
                            "VALUES (64738, 'test_server_1_alias_2')" )
            cursor.execute( "INSERT INTO processing_version_alias(id,description) "
                            "VALUES (64739, 'test_server_2_alias_1')" )
            con.commit()

        yield True

    finally:
        with db.DB() as con:
            cursor = con.cursor()
            cursor.execute( "DELETE FROM processing_version_alias WHERE id IN (64738,64739,64740)" )
            cursor.execute( "DELETE FROM processing_version WHERE id IN (64738,64739,64740)" )
            con.commit()


# If you've manually loaded your test database, but haven't
#   manually inserted a user, from /code/tests run:
#      pytest -v --trace webserver/test_server.py::test_just_make_a_user
#   and, when you get to the Pdb() prompt, go look at the webap.
def test_just_make_a_user( test_user ):
    pass


def test_getprocvers( server_test_processing_versions, test_user, fastdb_client ):
    res = fastdb_client.post( '/getprocvers' )
    assert isinstance( res, dict )
    assert res['status'] == 'ok'
    assert res['procvers'] == [ 'test_procesing_version',
                                'test_server_1', 'test_server_1_alias_1', 'test_server_1_alias_2',
                                'test_server_2', 'test_server_2_alias_1', 'test_server_3' ]


def test_procver( server_test_processing_versions, test_user, fastdb_client ):
    for suffix in [ '64738', 'test_server_1', 'test_server_1_alias_1', 'test_server_1_alias_2' ]:
        res = fastdb_client.post( f'/procver/{suffix}' )
        assert isinstance( res, dict )
        assert res['status'] == 'ok'
        assert res['id'] == 64738
        assert res['description'] == 'test_server_1'
        assert res['aliases'] == [ 'test_server_1_alias_1', 'test_server_1_alias_2' ]

    for suffix in [ '64739', 'test_server_2', 'test_server_2_alias_1' ]:
        res = fastdb_client.post( f'/procver/{suffix}' )
        assert isinstance( res, dict )
        assert res['status'] == 'ok'
        assert res['id'] == 64739
        assert res['description'] == 'test_server_2'
        assert res['aliases'] == [ 'test_server_2_alias_1' ]

    for suffix in [ '64740', 'test_server_3' ]:
        res = fastdb_client.post( f'/procver/{suffix}' )
        assert isinstance( res, dict )
        assert res['status'] == 'ok'
        assert res['id'] == 64740
        assert res['description'] == 'test_server_3'
        assert res['aliases'] == []

    # Reduce retries so that these calls will fail fast
    orig_retries = fastdb_client.retries
    try:
        fastdb_client.retries = 0
        with pytest.raises( RuntimeError, match='Got status 500 trying to connect' ):
            res = fastdb_client.post( '/procver/64741' )
        with pytest.raises( RuntimeError, match='Got status 500 trying to connect' ):
            res = fastdb_client.post( '/procver/does_not_exist' )
    finally:
        fastdb_client.retries = orig_retries
