# This test is in a separate file because it uses a different module-scope fixture from the tests in test_ltcv.py

import pytest
import re


def test_getbrokerinfo( alerts_90days_sent_received_and_imported, fastdb_client ):
    for suffix in [ "", "/realtime" ]:
        srcs = [ 2971700022, 174704200008, 198154000035 ]

        res = fastdb_client.post( f'/ltcv/getbrokerinfo{suffix}', json={ 'diasourceids': srcs } )
        assert len(res) == 3
        assert set( res.keys() ) == set( str(s) for s in srcs )
        for v in res.values():
            assert isinstance( v, list )
            assert len(v) == 2
            assert set( [ row['brokername'] for row in v ] ) == { 'FakeBroker-Nugent', 'FakeBroker-Random' }
            # Can't use the barf fixture because we ran the fixture that lodas a cached database,
            #  which cached it with different barf from what the current run of the barf fixture will generate
            assert all( re.search('^classifications-......$', row['topic']) for row in v )
            assert all( set(row['info'].keys()) == { 'brokerName', 'classifierName',
                                                     'classifierVersion', 'classifications' }
                        for row in v )

        actual_topic = res['2971700022'][0]['topic']

        # ...really should have made the fixtures so that there are multiple topics per brokername...
        res = fastdb_client.post( '/ltcv/getbrokerinfo',
                                  json={ 'diasourceids': srcs, 'brokername': 'FakeBroker-Nugent' } )
        assert len(res) == 3
        # I hate that json makes all dictionary keys strings
        assert set( res.keys() ) == set( str(s) for s in srcs )
        for v in res.values():
            assert len(v) == 1
            assert v[0]['brokername'] == 'FakeBroker-Nugent'

        res = fastdb_client.post( '/ltcv/getbrokerinfo', json={ 'diasourceids': srcs,
                                                                'brokername': 'FakeBroker-Nugent',
                                                                'topic': actual_topic } )
        assert len(res) == 3
        assert set( res.keys() ) == set( str(s) for s in srcs )
        for v in res.values():
            assert len(v) == 1
            assert v[0]['brokername'] == 'FakeBroker-Nugent'
            assert v[0]['topic'] == actual_topic


        res = fastdb_client.post( '/ltcv/getbrokerinfo', json={ 'diasourceids': srcs,
                                                                'brokername': 'FakeBroker-Nugent',
                                                                'topic': 'foo' } )
        assert len(res) == 0


        res = fastdb_client.post( '/ltcv/getbrokerinfo', json={ 'diasourceids': srcs, 'brokername': 'foo' } )
        assert len(res) == 0

        with pytest.raises( RuntimeError, match=( '^Error response from server, status 422: '
                                                  'Unknown processing version foo$' ) ):
            res = fastdb_client.post( '/ltcv/getbrokerinfo/foo', json={ 'diasourceids': srcs } )

        with pytest.raises( RuntimeError, match=( '^Error response from server, status 422: Post data was not a '
                                                  'JSON dict, expected a dict as JSON post data.' ) ):
            res = fastdb_client.post( '/ltcv/getbrokerinfo/realtime' )

        with pytest.raises( RuntimeError, match=( '^Error response from server, status 422: Post data dict must '
                                                  'include key diasourceids with list of source ids' ) ):
            res = fastdb_client.post( '/ltcv/getbrokerinfo/realtime', json={} )
