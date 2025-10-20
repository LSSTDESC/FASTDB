import io

import pandas as pd
import nested_pandas as npd

from parquet_export import dump_to_parquet


def test_dump_to_parquet( set_of_lightcurves ):
    roots = set_of_lightcurves
    bio = io.BytesIO()

    # There are 3 objects in realtime
    dump_to_parquet( bio, 'realtime' )
    bio.seek(0)
    nf = npd.read_parquet( bio )
    assert set( nf.columns ) == { 'diaobjectid', 'rootid', 'base_procver_id', 'validitystartmjdtai',
                                  'ra', 'raerr', 'dec', 'decerr', 'ra_dec_cov',
                                  'nearbyextobj1', 'nearbyextobj1id', 'nearbyextobj1sep',
                                  'nearbyextobj2', 'nearbyextobj2id', 'nearbyextobj2sep',
                                  'nearbyextobj3', 'nearbyextobj3id', 'nearbyextobj3sep',
                                  'nearbylowzgal', 'nearbylowzgalsep',
                                  'diasource', 'diaforcedsource' }
    assert len(nf) == 3
    # Results should be sorted by diaobjectid
    assert list( nf.diaobjectid ) == [ 0, 1, 2 ]
    assert set( nf.rootid ) == set( roots[i]['root'].id for i in [ 0, 1, 2 ] )

    # All 4 objects are in pv2
    bio.truncate(0)
    bio.seek(0)
    dump_to_parquet( bio, 'pvc_pv2' )
    bio.seek(0)
    nf = npd.read_parquet( bio )
    assert len(nf) == 4
    assert list( nf.diaobjectid ) == [ 200, 201, 202, 203 ]
    assert set( nf.rootid ) == set( r['root'].id for r in roots )
    assert isinstance( nf.iloc[0].diasource, pd.DataFrame )
    assert isinstance( nf.iloc[0].diaforcedsource, pd.DataFrame )
    assert set( nf.iloc[0].diasource.columns ) == { 'visit', 'midpointmjdtai', 'band', 'psfflux', 'psffluxerr' }
    assert set( nf.iloc[0].diaforcedsource.columns ) == { 'visit', 'midpointmjdtai', 'band', 'psfflux', 'psffluxerr' }

    # Test limit and offset
    bio.truncate(0)
    bio.seek(0)
    dump_to_parquet( bio, 'pvc_pv2', limit=2 )
    bio.seek(0)
    nf = npd.read_parquet( bio )
    assert len(nf) == 2
    assert list( nf.diaobjectid ) == [ 200, 201 ]

    bio.truncate(0)
    bio.seek(0)
    dump_to_parquet( bio, 'pvc_pv2', offset=1 )
    bio.seek(0)
    nf = npd.read_parquet( bio )
    assert len(nf) == 3
    assert list( nf.diaobjectid ) == [ 201, 202, 203 ]

    bio.truncate(0)
    bio.seek(0)
    dump_to_parquet( bio, 'pvc_pv2', offset=1, limit=2 )
    bio.seek(0)
    nf = npd.read_parquet( bio )
    assert len(nf) == 2
    assert list( nf.diaobjectid ) == [ 201, 202 ]
