import pytest

from db import DBCon
from util import asUUID


# set_of_lightcurves is overkill for these tests but oh well
@pytest.fixture
def tagged_objects( set_of_lightcurves, admin_dbclient ):
    roots = set_of_lightcurves
    try:
        admin_dbclient.post( "/createtag/foo", { 'description': "First test tag" } )
        admin_dbclient.post( "/createtag/bar/Second+test_tag" )
        admin_dbclient.post( "/createtag/baz/Unused+tag" )

        admin_dbclient.post( "/tagobjects/foo", { 'objects': [ roots[0]['root'].id,
                                                               roots[1]['root'].id ] } )
        admin_dbclient.post( "/tagobjects/bar", { 'objects': [ 100, 1, 203 ],
                                                  'ids_are_diaobjectid': True } )

        yield True

    finally:
        with DBCon() as dbcon:
            # Trust the cascade to clear out root_diaobject_tag
            dbcon.execute( "DELETE FROM tags WHERE tag IN ('foo', 'bar', 'baz')" )
            dbcon.commit()


def test_user_failure( tagged_objects, dbclient ):
    with pytest.raises( RuntimeError, match=( r"Error response from server, status 422: "
                                              r"User must be in one of the groups: \['admin', 'tag_create'\]" ) ):
        dbclient.post( "/createtag/qux" )

    with pytest.raises( RuntimeError, match=( r"Error response from server, status 422: "
                                              r"User must be in one of the groups: \['admin', 'tag_create'\]" ) ):
        dbclient.post( "/tagobjects/foo", { 'objects': [ 202 ], 'ids_are_diaobjectid': True } )


def test_get_tags_for_object( tagged_objects, dbclient, set_of_lightcurves ):
    roots = set_of_lightcurves

    results = dbclient.post( f"/tagsforobject/{roots[0]['root'].id}" )
    assert set( results['tags'] ) == { 'foo', 'bar' }

    results = dbclient.post( "/tagsforobject/201" )
    assert set( results['tags'] ) == { 'foo', 'bar' }

    results = dbclient.post( "/tagsforobject/203" )
    assert set( results['tags'] ) == { 'bar' }

    results = dbclient.post( f"/tagsforobject/{roots[2]['root'].id}" )
    assert results['tags'] == []

    results = dbclient.post( "/tagsforobject/2" )
    assert results['tags'] == []

    with pytest.raises( RuntimeError, match=( r"Error response from server, status 422: "
                                              r"Invalid object id / root id foo" ) ):
        results = dbclient.post( "/tagsforobject/foo" )


def test_get_objects_for_tag( tagged_objects, dbclient, set_of_lightcurves ):
    roots = set_of_lightcurves

    results = dbclient.post( "/objectsfortag/foo" )
    assert results['tag'] == 'foo'
    assert set( asUUID(x) for x in results['rootids'] ) == set( roots[i]['root'].id for i in [0, 1] )

    results = dbclient.post( "objectsfortag/bar" )
    assert results['tag'] == 'bar'
    assert set( asUUID(x) for x in results['rootids'] ) == set( roots[i]['root'].id for i in [0, 1, 3] )

    results = dbclient.post( "objectsfortag/baz" )
    assert results['tag'] == 'baz'
    assert results['rootids'] == []

    with pytest.raises( RuntimeError, match=r"Error response from server, status 422: Unknown tag qux" ):
        results = dbclient.post( "objectsfortag/qux" )
