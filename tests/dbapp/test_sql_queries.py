import pytest
import sys
import io
import pandas
import itertools

sys.path.insert( 0, '/code/client' )
from fastdb_client import FASTDBClient


@pytest.fixture
def test_sql_query_expecteddata( set_of_lightcurves ):
    expecteddata = set(
        itertools.chain(
            *[ itertools.chain(
                *[ itertools.chain( *[ [ ( s.diasourceid, s.diaobjectid, s.visit, str(s.base_procver_id) )
                                         for s in slist ] ] )
                   for slist in rstruct['src'].values() ] )
               for rstruct in set_of_lightcurves ] ) )

    return expecteddata


def test_short_query( test_user, test_sql_query_expecteddata ):
    fastdb = FASTDBClient( 'http://webap:8080', username='test', password='test_password' )

    res = fastdb.submit_short_sql_query( "SELECT * FROM diasource" )
    founddata = set( ( r['diasourceid'], r['diaobjectid'], r['visit'], r['base_procver_id'] ) for r in res )
    assert founddata == test_sql_query_expecteddata


def test_synchronous_long_query( test_user, test_sql_query_expecteddata ):
    fastdb = FASTDBClient( 'http://webap:8080', username='test', password='test_password' )

    res = fastdb.synchronous_long_sql_query( "SELECT * FROM diasource", checkeach=1, maxwait=20 )
    strio = io.StringIO( res )
    df = pandas.read_csv( strio, sep=',', header=0 )
    founddata = set( ( r.diasourceid, r.diaobjectid, r.visit, r.base_procver_id ) for r in df.itertuples() )
    assert founddata == test_sql_query_expecteddata
