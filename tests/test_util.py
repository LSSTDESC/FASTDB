import pathlib
import util

import numpy as np
import pandas

# OMG LOTS OF TESTS STILL NEED TO BE WRITTEN


def test_get_alert_schema():
    schema = util.get_alert_schema()
    assert set( schema.keys() ) == { 'alert', 'diaobject', 'diasource', 'diaforcedsource', 'mpc_orbits',
                                     'sssource', 'brokermessage', 'alert_schema_file', 'brokermessage_schema_file' }
    for key in [ 'alert', 'diaobject', 'diasource', 'diaforcedsource', 'mpc_orbits', 'sssource', 'brokermessage' ]:
        assert isinstance( schema[key], dict )
    assert isinstance( schema['alert_schema_file'], pathlib.Path )
    assert isinstance( schema['brokermessage_schema_file'], pathlib.Path )



def test_parse_sexigesimal():
    assert util.parse_sexigesimal( "00:00:00" ) == 0.
    assert util.parse_sexigesimal( "-00:00:00" ) == 0.
    assert util.parse_sexigesimal( "+00:00:00" ) == 0.

    assert util.parse_sexigesimal( "1:30:0" ) == 1.5
    assert util.parse_sexigesimal( "-00:30:00" ) == -0.5
    assert util.parse_sexigesimal( "+00:30:00" ) == 0.5

    # TODO : more, including lots of things with spaces in places, decimal seconds, etc.


def test_laboriously_construct_pandas():

    data = { 'first': { 'a': [1, 2, 3], 'b': [4, 5, None], 'c': [7, None, 9] },
             'second': { 'a': [1, 2, 3], 'b': [ 10, 11, 12], 'c': [13, 14, 15] }
            }
    df = util.laboriously_construct_pandas( data, int16cols=['a', 'b'], doublecols=['c'],
                                            keyname='which', indices=['a'], ignore_missing_cols=True )

    expected = np.empty( 6, dtype='object' )
    expected[:] = [ ( 'first', 1 ), ( 'first', 2 ), ( 'first', 3 ),
                    ( 'second', 1 ), ( 'second', 2 ), ( 'second', 3 ) ]
    assert all( df.index.values == expected )

    assert pandas.isna( df.loc[ ('first', 3), 'b' ] )
    assert pandas.isna( df.loc[ ('first', 2), 'c' ] )
    assert all( df.xs( 1, level='a' ).index.values == ['first', 'second'] )
    assert all( df.xs( 'second', level='which' )['c'].values == [13., 14., 15.] )
    assert df.b.dtype == 'int16[pyarrow]'
    assert df.c.dtype == 'double[pyarrow]'

    data = { 'cat': [ 1, 2, 3 ], 'dog': [ 4, None, 6 ], 'mouse': [ 7, 8, 9 ], 'wombat': [ 1.414, 2.718, 3.141 ] }
    df = util.laboriously_construct_pandas( data, int16cols='dog', ignore_missing_cols=True )
    assert df.cat.dtype == np.dtype('int64')      # The default pandas decided
    assert df.dog.dtype == 'int16[pyarrow]'
    assert df.mouse.dtype == np.dtype('int64')    # The default pandas decided
    assert df.wombat.dtype == np.dtype('float64') # The default pandas decided
    assert all( df.cat.values == [1, 2, 3] )
    assert all( df.dog.loc[ [ 0, 2] ].values == [4, 6] )
    assert pandas.isna( df.dog[1] )
    assert all( df.mouse.values == [7, 8, 9] )
    assert all( df.wombat.values == [1.414, 2.718, 3.141] )

    data = [ { 'cat': 1, 'dog': 4,    'mouse': 7, 'wombat': 1.414 },
             { 'cat': 2, 'dog': None, 'mouse': 8, 'wombat': 2.718 },
             { 'cat': 3, 'dog': 6,    'mouse': 9, 'wombat': 3.141 } ]
    df2 = util.laboriously_construct_pandas( data, int16cols='dog', ignore_missing_cols=True )
    assert all( df.columns == df2.columns )
    assert all( df[col].dtype == df2[col].dtype for col in df.columns )
    assert ( df == df2 ).all().all()

    df3 = util.laboriously_construct_pandas( data, int16cols='dog', indices='cat', ignore_missing_cols=True )
    assert ( df3.reset_index() == df2 ).all().all()
    assert df3.index.name == 'cat'
    assert all( df3.columns.values == [ 'dog', 'mouse', 'wombat' ] )
    assert all( df3.index.values == [1, 2, 3 ] )

    df4 = util.laboriously_construct_pandas( data, int16cols='dog', indices=('cat', 'mouse'), ignore_missing_cols=True )
    # Columns come out in a different order so a straight df4.reset_index() == df2 doesn't work.
    # ...and, in this case, we have to deal with NaN != NaN
    #   (it's not entirely clear to me why we didn't have to deal with that
    #   in ( df3.reset_index() == df2 ).all().all() above ...)
    assert all( all( ( df4.reset_index()[c] == df2[c] )
                     |
                     ( pandas.isna(df4.reset_index()[c]) & pandas.isna(df2[c]) )
                    )
                for c in df2.columns )
    expected[:] = [ (1, 7), (2, 8), (3, 9) ]
    # ...it is of course, totally intuitively obvious that you need
    #  to not have all() on the first line below, but you must
    #  have it on the second...
    assert df4.index.names == [ 'cat', 'mouse' ]
    assert all( df4.index.values == expected )
