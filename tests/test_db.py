from db import construct_pgsql_where_clause


def test_construct_sql_where_clause():
    searchspec = { 'just':       { 'mult': False, 'substr': False, 'minmax': False },
                   'mult':       { 'mult': True,  'substr': False, 'minmax': False },
                   'substr':     { 'mult': False, 'substr': True,  'minmax': False },
                   'minmax':     { 'mult': False, 'substr': False, 'minmax': True },
                   'multsubstr': { 'mult': True,  'substr': True,  'minmax': False }
                  }


    q, subdict, missing, _where = construct_pgsql_where_clause( searchspec, just="A" )
    assert missing == set()
    assert q.as_string() == 'WHERE "just"=%(just)s'
    assert subdict == { 'just': 'A' }

    q, subdict, missing, _where = construct_pgsql_where_clause( searchspec, just="A", mult="B", does_not_exist=42 )
    assert missing == set( [ 'does_not_exist' ] )
    assert q.as_string() == 'WHERE "just"=%(just)s AND "mult"=%(mult)s'
    assert subdict == { 'just': 'A', 'mult': 'B' }

    q, subdict, missing, _where = construct_pgsql_where_clause( searchspec, just="A", mult=[ "B", "C" ] )
    assert missing == set()
    assert q.as_string() == 'WHERE "just"=%(just)s AND "mult"=ANY(%(mult)s)'
    assert subdict == { 'just': 'A', 'mult': [ 'B', 'C' ] }

    q, subdict, missing, _where = construct_pgsql_where_clause( searchspec, just="A", mult=( "B", "C" ) )
    assert missing == set()
    assert q.as_string() == 'WHERE "just"=%(just)s AND "mult"=ANY(%(mult)s)'
    assert subdict == { 'just': 'A', 'mult': [ 'B', 'C' ] }

    q, subdict, missing, _where = construct_pgsql_where_clause( searchspec, substr="B", substr_contains="C" )
    assert missing == set()
    assert q.as_string() == 'WHERE "substr"=%(substr)s AND "substr" LIKE %(substr_contains)s'
    assert subdict == { 'substr': 'B', 'substr_contains': '%C%' }

    q, subdict, missing, _where = construct_pgsql_where_clause( searchspec, multsubstr="C", multsubstr_contains="D" )
    assert missing == set()
    assert q.as_string() == 'WHERE "multsubstr"=%(multsubstr)s AND "multsubstr" LIKE %(multsubstr_contains)s'
    assert subdict == { 'multsubstr': "C", 'multsubstr_contains': '%D%' }

    q, subdict, missing, _where = construct_pgsql_where_clause( searchspec, multsubstr="C",
                                                                multsubstr_contains=[ "D", "E" ] )
    assert missing == set()
    assert q.as_string() == ( 'WHERE "multsubstr"=%(multsubstr)s AND '
                              '("multsubstr" LIKE %(multsubstr_contains_0)s OR '
                              '"multsubstr" LIKE %(multsubstr_contains_1)s)' )
    assert subdict == { 'multsubstr': "C", 'multsubstr_contains_0': '%D%', 'multsubstr_contains_1': "%E%" }

    # NOT DONE YET, MORE TESTS NEED TO BE WRITTEN
