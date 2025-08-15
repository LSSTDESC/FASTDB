import sys
import io
import pandas

sys.path.insert( 0, '/code/client' )
from fastdb_client import FASTDBClient


def test_short_query( obj1, src1, src1_pv2, test_user ):
    fastdb = FASTDBClient( 'http://webap:8080', username='test', password='test_password' )

    res = fastdb.submit_short_sql_query( "SELECT * FROM diasource" )
    assert len(res) == 2
    assert set( [ r['diaobjectid'] for r in res ] ) == { src1.diaobjectid, src1_pv2.diaobjectid }
    assert set( [ r['visit'] for r in res ] ) == { src1.visit, src1_pv2.visit }
    assert set( [ r['processing_version'] for r in res ] ) == { src1.processing_version, src1_pv2.processing_version }


def test_synchronous_long_query( obj1, src1, src1_pv2, test_user ):
    fastdb = FASTDBClient( 'http://webap:8080', username='test', password='test_password' )

    res = fastdb.synchronous_long_sql_query( "SELECT * FROM diasource", checkeach=1, maxwait=20 )
    strio = io.StringIO( res )
    df = pandas.read_csv( strio, sep=',', header=0 )
    assert len(df) == 2
    assert all( df.diaobjectid.values == [42,42] )
    assert all( df.visit.values == [64,64] )
    assert set( df.processing_version.values ) == { 23, 42 }
