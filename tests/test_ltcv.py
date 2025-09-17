import pytest
import numpy as np

import db
import ltcv


def test_get_object_infos( set_of_lightcurves ):
    roots = set_of_lightcurves

    info = ltcv.get_object_infos( [ 200, 201, 202 ], return_format='pandas' )
    assert info.index.name == 'diaobjectid'
    assert set(info.columns.values) == set( [ i for i in db.DiaObject().tablemeta.keys() if i != 'diaobjectid' ] )
    assert len(info) == 3
    assert list( info.index.values ) == [ 200, 201, 202 ]
    assert info.rootid.values.tolist() == [ roots[i]['root'].id for i in [0, 1, 2] ]

    jsinfo = ltcv.get_object_infos( [ 200, 201, 202 ], return_format='json' )
    assert jsinfo['diaobjectid'] == [ 200, 201, 202 ]
    info.reset_index( inplace=True )
    for col in info.columns:
        assert ( info.loc[ :, col ].values == np.array( jsinfo[col] ) ).all()

    with pytest.raises( ValueError, match='Passing root ids requires a processing_version' ):
        info = ltcv.get_object_infos( [ roots[i]['root'].id for i in [0, 1, 2] ] )

    info = ltcv.get_object_infos( [ roots[i]['root'].id for i in [0, 1, 2] ], processing_version='pvc_pv2' )
    assert info['diaobjectid'] == [ 200, 201, 202 ]

    info = ltcv.get_object_infos( [ 200, 201, 202 ], columns=['ra', 'dec'] )
    assert info['diaobjectid'] == [ 200, 201, 202 ]
    assert set( info.keys() ) == { 'diaobjectid', 'ra', 'dec' }

    with db.DBCon() as dbcon:
        dbcon.execute( "CREATE TEMP TABLE tempthing(diaobjectid bigint)", explain=False )
        dbcon.execute( "INSERT INTO tempthing(diaobjectid) VALUES ( 200 )" )
        dbcon.execute( "INSERT INTO tempthing(diaobjectid) VALUES ( 202 )" )
        info = ltcv.get_object_infos( objids_table='tempthing', dbcon=dbcon )
        assert info['diaobjectid'] == [ 200, 202 ]

        with pytest.raises( ValueError, match='objids_table requires dbcon' ):
            ltcv.get_object_infos( objids_table='tempthing' )

        with pytest.raises( ValueError, match='objids_table and objids cannot be used together' ):
            ltcv.get_object_infos( objids_table='tempthing', objids=[0, 1, 2], dbcon=dbcon )

        with pytest.raises( ValueError, match='objids_table and objids cannot be used together' ):
            ltcv.get_object_infos( [0, 1, 2], objids_table='tempthing', dbcon=dbcon )

        with pytest.raises( ValueError, match='Cannot pass processing_version when passing objids_table' ):
            ltcv.get_object_infos( objids_table='tempthing', processing_version='pvc_pv2', dbcon=dbcon )


def test_object_ltcv( procver_collection, set_of_lightcurves ):
    roots = set_of_lightcurves
    _bpvs, pvs = procver_collection

    # The fixture loads up lightcurves every 2.5 days

    # Try to get the object lightcurve for the second object of roots[0] using pv1
    # Should get detections starting 60000, forced starting 50090,
    # sources through 60015 and forced through 60010 in bpv1a, then sources through 60030
    # and forced through 60050 in bpv1

    sources = ltcv.object_ltcv( pvs['pv1'].id, roots[0]['objs'][2]['obj'].diaobjectid,
                                return_format='pandas', which='detections', include_base_procver=True )
    forced = ltcv.object_ltcv( pvs['pv1'].id, roots[0]['objs'][2]['obj'].diaobjectid,
                               return_format='pandas', which='forced', include_base_procver=True )
    df = ltcv.object_ltcv( pvs['pv1'].id, roots[0]['objs'][2]['obj'].diaobjectid,
                           return_format='pandas', which='patch', include_base_procver=True )

    assert len(sources) == 13
    assert len(forced) == 15
    assert len(df) == 17

    assert np.all( ( sources.mjd >= 60000 ) & ( sources.mjd <= 60030 ) )
    assert np.all( sources[ sources.mjd <= 60015 ].base_procver == 'pvc_bpv1a' )
    assert np.all( sources[ sources.mjd > 60015 ].base_procver == 'pvc_bpv1' )
    assert np.all( sources.isdet )
    assert np.all( forced[ forced.mjd <= 60010 ].base_procver == 'pvc_bpv1a' )
    assert np.all( forced[ forced.mjd > 60010 ].base_procver == 'pvc_bpv1' )
    assert np.all( forced[ ( forced.mjd >= 60000 ) & ( forced.mjd <= 60030 ) ].isdet )
    assert np.all( ~forced[ ( forced.mjd < 60000 ) | ( forced.mjd > 60030 ) ].isdet )
    assert np.all( df[ df.mjd <= 60010 ].base_procver == 'pvc_bpv1a' )
    assert np.all( df[ df.mjd > 60010 ].base_procver == 'pvc_bpv1' )
    assert np.all( df[ df.mjd > 60025 ].ispatch )
    assert np.all( ~df[ df.mjd <= 60025 ].ispatch )
    assert np.all( df[ ( df.mjd >= 60000 ) & ( df.mjd <= 60030 ) ].isdet )
    assert np.all( ~df[ ( df.mjd < 60000 ) | ( df.mjd > 60030 ) ].isdet )


    # If we ask for roots[1] from pv1, we shouldn't get anything.
    # (Also trying using the root object this time.)

    with pytest.raises( RuntimeError, match="Could not find object for diaobjectid" ):
        df = ltcv.object_ltcv( pvs['pv1'].id, roots[1]['root'].id, return_format='pandas', which='patch',
                               include_base_procver=True )

    # But if we ask for roots[1] from pv2, we should get stuff.  In this case, patch and forced
    # should be the same

    sources = ltcv.object_ltcv( pvs['pv2'].id, roots[1]['root'].id, return_format='pandas',
                                which='detections', include_base_procver=True )
    forced = ltcv.object_ltcv( pvs['pv2'].id, roots[1]['root'].id, return_format='pandas',
                               which='forced', include_base_procver=True )
    df = ltcv.object_ltcv( pvs['pv2'].id, roots[1]['root'].id, return_format='pandas',
                           which='patch', include_base_procver=True )
    assert ( df.loc[ :, [ c for c in df.columns if c!='ispatch'] ] == forced ).all().all()
    assert ( ~df.ispatch ).all()
    assert len( df[ df.isdet ] ) == len( sources )
    assert df[ df.isdet ].isdet.all()
    assert ( ~df[ df.ispatch ].ispatch ).all()
    assert ( df.base_procver == 'pvc_bpv2a' ).all()

    # Make sure json output is consistent

    j_sources = ltcv.object_ltcv( pvs['pv2'].id, roots[1]['root'].id, return_format='json',
                                  which='detections', include_base_procver=True )
    j_forced = ltcv.object_ltcv( pvs['pv2'].id, roots[1]['root'].id, return_format='json',
                                 which='forced', include_base_procver=True )
    j_df = ltcv.object_ltcv( pvs['pv2'].id, roots[1]['root'].id, return_format='json',
                             which='patch', include_base_procver=True )

    for js, pd in zip( [ j_sources, j_forced, j_df ], [ sources, forced, df ] ):
        assert isinstance( js, dict )
        for col in pd.columns:
            assert ( pd[col] == np.array( js[col] ) ).all()

    # Make sure we can pass a string processing version

    df2 = ltcv.object_ltcv( 'pvc_pv2', roots[1]['root'].id, return_format='pandas',
                            which='patch', include_base_procver=True )
    assert ( df == df2 ).all().all()

    # Test mjd_now
    df = ltcv.object_ltcv( pvs['pv2'].id, roots[1]['root'].id, return_format='pandas',
                           which='patch', include_base_procver=True, mjd_now=60040 )
    assert ( df.mjd <= 60040 ).all()
    assert len( df ) == 13


def test_many_object_ltcvs( procver_collection, set_of_lightcurves ):
    roots = set_of_lightcurves
    _bpvs, pvs = procver_collection

    # First, reproduce the tests from test_object_ltcv.  We'll ask for two lightcurves,
    #   but only one is going to be present.

    sources = ltcv.many_object_ltcvs( pvs['pv1'].id, [ roots[i]['root'].id for i in [0,1] ],
                                      return_format='pandas', which='detections', include_base_procver=True )
    forced = ltcv.many_object_ltcvs( pvs['pv1'].id, [ roots[i]['root'].id for i in [0,1] ],
                                     return_format='pandas', which='forced', include_base_procver=True )
    df = ltcv.many_object_ltcvs( pvs['pv1'].id, [ roots[i]['root'].id for i in [0,1] ],
                                 return_format='pandas', which='patch', include_base_procver=True )

    assert sources.index.get_level_values( 'diaobjectid' ).unique().values == np.array( [100] )
    assert forced.index.get_level_values( 'diaobjectid' ).unique().values == np.array( [100] )
    assert df.index.get_level_values( 'diaobjectid' ).unique().values == np.array( [100] )

    sources.reset_index( inplace=True )
    sources.drop( 'diaobjectid', axis='columns', inplace=True )
    forced.reset_index( inplace=True )
    forced.drop( 'diaobjectid', axis='columns', inplace=True )
    df.reset_index( inplace=True )
    df.drop( 'diaobjectid', axis='columns', inplace=True )

    # These checks are (sorta) copied from test_object_ltcv
    assert len(sources) == 13
    assert len(forced) == 15
    assert len(df) == 17

    assert np.all( ( sources.mjd >= 60000 ) & ( sources.mjd <= 60030 ) )
    assert np.all( sources[ sources.mjd <= 60015 ].base_procver == 'pvc_bpv1a' )
    assert np.all( sources[ sources.mjd > 60015 ].base_procver == 'pvc_bpv1' )
    assert np.all( sources.isdet )
    assert np.all( forced[ forced.mjd <= 60010 ].base_procver == 'pvc_bpv1a' )
    assert np.all( forced[ forced.mjd > 60010 ].base_procver == 'pvc_bpv1' )
    assert np.all( forced[ ( forced.mjd >= 60000 ) & ( forced.mjd <= 60030 ) ].isdet )
    assert np.all( ~forced[ ( forced.mjd < 60000 ) | ( forced.mjd > 60030 ) ].isdet )
    assert np.all( df[ df.mjd <= 60010 ].base_procver == 'pvc_bpv1a' )
    assert np.all( df[ df.mjd > 60010 ].base_procver == 'pvc_bpv1' )
    assert np.all( df[ df.mjd > 60025 ].ispatch )
    assert np.all( ~df[ df.mjd <= 60025 ].ispatch )
    assert np.all( df[ ( df.mjd >= 60000 ) & ( df.mjd <= 60030 ) ].isdet )
    assert np.all( ~df[ ( df.mjd < 60000 ) | ( df.mjd > 60030 ) ].isdet )

    # Now ask for two lightcurves where we expect to get two lightcurves
    sources = ltcv.many_object_ltcvs( pvs['pv2'].id, [ roots[i]['root'].id for i in [0,2] ],
                                      return_format='pandas', which='detections', include_base_procver=True )
    forced = ltcv.many_object_ltcvs( pvs['pv2'].id, [ roots[i]['root'].id for i in [0,2] ],
                                     return_format='pandas', which='forced', include_base_procver=True )
    df = ltcv.many_object_ltcvs( pvs['pv2'].id, [ roots[i]['root'].id for i in [0,2] ],
                                 return_format='pandas', which='patch', include_base_procver=True )
    # We know we're going to be sorted by diaobjectid
    assert ( sources.index.get_level_values( 'diaobjectid' ).unique().values == np.array( [200, 202] ) ).all()
    assert ( forced.index.get_level_values( 'diaobjectid' ).unique().values == np.array( [200, 202] ) ).all()
    assert ( df.index.get_level_values( 'diaobjectid' ).unique().values == np.array( [200, 202] ) ).all()
    assert len( sources ) == 30
    assert len( sources.xs( 200, level='diaobjectid' ) ) == 13
    assert len( sources.xs( 202, level='diaobjectid' ) ) == 17
    assert len( forced ) == 54
    assert len( forced.xs( 200, level='diaobjectid' ) ) == 25
    assert len( forced.xs( 202, level='diaobjectid' ) ) == 29
    assert len( df ) == 54
    assert len( df.xs( 200, level='diaobjectid' ) ) == 25
    assert len( df.xs( 202, level='diaobjectid' ) ) == 29
    tmp = df.drop( 'ispatch', axis='columns' )
    assert ( tmp == forced ).all().all()

    # Make sure the dict returns are consistent
    sourcesjs = ltcv.many_object_ltcvs( pvs['pv2'].id, [ roots[i]['root'].id for i in [0,2] ],
                                        return_format='json', which='detections', include_base_procver=True )
    forcedjs = ltcv.many_object_ltcvs( pvs['pv2'].id, [ roots[i]['root'].id for i in [0,2] ],
                                       return_format='json', which='forced', include_base_procver=True )
    dfjs = ltcv.many_object_ltcvs( pvs['pv2'].id, [ roots[i]['root'].id for i in [0,2] ],
                                   return_format='json', which='patch', include_base_procver=True )
    assert list( sourcesjs.keys() ) == [ 200, 202 ]
    assert list( forcedjs.keys() ) == [ 200, 202 ]
    assert list( dfjs.keys() ) == [ 200, 202 ]
    for js, pd in zip( [ sourcesjs, forcedjs, dfjs ], [ sources, forced, df ] ):
        for objid in [ 200, 202 ]:
            subpd = pd.xs( objid, level='diaobjectid' ).reset_index()
            subjs = js[ objid ]
            for col in subpd.columns:
                assert ( subpd[col] == np.array( subjs[col] ) ).all()

    # Make sure the lightcurves are actually right
    for diaobjectid in df.index.get_level_values( 'diaobjectid' ).unique():
        rdex = [ roots[i]['objs'][1]['obj'].diaobjectid for i in range(len(roots)) ].index( diaobjectid )
        tmpsources = sources.xs( diaobjectid, level='diaobjectid' ).reset_index()
        tmpforced = forced.xs( diaobjectid, level='diaobjectid' ).reset_index()
        tmpdf = df.xs( diaobjectid, level='diaobjectid' ).reset_index()

        assert all( s.mjd == pytest.approx( roots[rdex]['objs'][1]['src']['bpv2'][j].midpointmjdtai, abs=1./3600./24. )
                    for j, s in enumerate( tmpsources.itertuples() ) )
        assert all( s.band == roots[rdex]['objs'][1]['src']['bpv2'][j].band
                    for j, s in enumerate( tmpsources.itertuples() ) )
        assert all( s.flux == pytest.approx( roots[rdex]['objs'][1]['src']['bpv2'][j].psfflux, rel=1e-6 )
                    for j, s in enumerate( tmpsources.itertuples() ) )
        assert all( s.fluxerr == pytest.approx( roots[rdex]['objs'][1]['src']['bpv2'][j].psffluxerr, rel=1e-6 )
                    for j, s in enumerate( tmpsources.itertuples() ) )
        assert all( tmpsources.isdet )
        assert all( tmpsources.base_procver == 'pvc_bpv2a' )

        assert all( s.mjd == pytest.approx( roots[rdex]['objs'][1]['frc']['bpv2'][j].midpointmjdtai, abs=1./3600./24. )
                    for j, s in enumerate( tmpforced.itertuples() ) )
        assert all( s.band == roots[rdex]['objs'][1]['frc']['bpv2'][j].band
                    for j, s in enumerate( tmpforced.itertuples() ) )
        assert all( s.flux == pytest.approx( roots[rdex]['objs'][1]['frc']['bpv2'][j].psfflux, rel=1e-6 )
                    for j, s in enumerate( tmpforced.itertuples() ) )
        assert all( s.fluxerr == pytest.approx( roots[rdex]['objs'][1]['frc']['bpv2'][j].psffluxerr, rel=1e-6 )
                    for j, s in enumerate( tmpforced.itertuples() ) )
        assert all( tmpforced.isdet[j] if ( tmpforced.mjd[j] >= tmpsources.mjd.min() and
                                            tmpforced.mjd[j] <= tmpsources.mjd.max() )
                    else not tmpforced.isdet[j]
                    for j in range( len(tmpforced) ) )
        assert all( tmpforced.base_procver == 'pvc_bpv2a' )

        assert all( s.mjd == pytest.approx( roots[rdex]['objs'][1]['frc']['bpv2'][j].midpointmjdtai, abs=1./3600./24. )
                    for j, s in enumerate( tmpdf.itertuples() ) )
        assert all( s.band == roots[rdex]['objs'][1]['frc']['bpv2'][j].band
                    for j, s in enumerate( tmpdf.itertuples() ) )
        assert all( s.flux == pytest.approx( roots[rdex]['objs'][1]['frc']['bpv2'][j].psfflux, rel=1e-6 )
                    for j, s in enumerate( tmpdf.itertuples() ) )
        assert all( s.fluxerr == pytest.approx( roots[rdex]['objs'][1]['frc']['bpv2'][j].psffluxerr, rel=1e-6 )
                    for j, s in enumerate( tmpdf.itertuples() ) )
        assert all( tmpdf.isdet[j] if ( tmpdf.mjd[j] >= tmpsources.mjd.min() and
                                        tmpdf.mjd[j] <= tmpsources.mjd.max() )
                    else not tmpdf.isdet[j]
                    for j in range( len(tmpdf) ) )
        assert all( ~tmpdf.ispatch )
        assert all( tmpdf.base_procver == 'pvc_bpv2a' )


    # make sure that if we ask using the right diaobjectid, we get the same things back
    sources2 = ltcv.many_object_ltcvs( pvs['pv2'].id, [ roots[i]['objs'][1]['obj'].diaobjectid for i in [0,2] ],
                                       return_format='pandas', which='detections', include_base_procver=True )
    forced2 = ltcv.many_object_ltcvs( pvs['pv2'].id, [ roots[i]['objs'][1]['obj'].diaobjectid for i in [0,2] ],
                                      return_format='pandas', which='forced', include_base_procver=True )
    df2 = ltcv.many_object_ltcvs( pvs['pv2'].id, [ roots[i]['objs'][1]['obj'].diaobjectid for i in [0,2] ],
                                  return_format='pandas', which='patch', include_base_procver=True )
    assert ( sources == sources2 ).all().all()
    assert ( forced == forced2 ).all().all()
    assert ( df == df2 ).all().all()

    # But we get nothing back if we ask for the wrong diaobjectid
    sources2 = ltcv.many_object_ltcvs( pvs['pv2'].id, [ roots[i]['objs'][0]['obj'].diaobjectid for i in [0,2] ],
                                       return_format='pandas', which='detections', include_base_procver=True )
    forced2 = ltcv.many_object_ltcvs( pvs['pv2'].id, [ roots[i]['objs'][0]['obj'].diaobjectid for i in [0,2] ],
                                      return_format='pandas', which='forced', include_base_procver=True )
    df2 = ltcv.many_object_ltcvs( pvs['pv2'].id, [ roots[i]['objs'][0]['obj'].diaobjectid for i in [0,2] ],
                                  return_format='pandas', which='patch', include_base_procver=True )
    assert len(sources2) == 0
    assert len(forced2) == 0
    assert len(df2) == 0

    # test mjd_now
    assert not ( sources.index.get_level_values( level='mjd' ) <= 60041. ).all()
    assert not ( forced.index.get_level_values( level='mjd' ) <= 60041. ).all()
    assert not ( df.index.get_level_values( level='mjd' ) <= 60041. ).all()
    sources = ltcv.many_object_ltcvs( pvs['pv2'].id, [ roots[i]['root'].id for i in [0,2] ],
                                      return_format='pandas', which='detections', include_base_procver=True,
                                      mjd_now=60041. )
    forced = ltcv.many_object_ltcvs( pvs['pv2'].id, [ roots[i]['root'].id for i in [0,2] ],
                                     return_format='pandas', which='forced', include_base_procver=True,
                                     mjd_now=60041. )
    df = ltcv.many_object_ltcvs( pvs['pv2'].id, [ roots[i]['root'].id for i in [0,2] ],
                                 return_format='pandas', which='patch', include_base_procver=True,
                                 mjd_now=60041. )
    assert ( sources.index.get_level_values( level='mjd' ) <= 60041. ).all()
    assert ( forced.index.get_level_values( level='mjd' ) <= 60041. ).all()
    assert ( df.index.get_level_values( level='mjd' ) <= 60041. ).all()


    # Make sure bands works
    sources = ltcv.many_object_ltcvs( pvs['pv2'].id, [ roots[i]['root'].id for i in [0,2] ],
                                      return_format='pandas', which='detections', include_base_procver=True,
                                      bands='r' )
    forced = ltcv.many_object_ltcvs( pvs['pv2'].id, [ roots[i]['root'].id for i in [0,2] ],
                                     return_format='pandas', which='forced', include_base_procver=True,
                                     bands=[ 'r' ] )
    df = ltcv.many_object_ltcvs( pvs['pv2'].id, [ roots[i]['root'].id for i in [0,2] ],
                                 return_format='pandas', which='patch', include_base_procver=True,
                                 bands='r' )
    assert len(sources) == 14
    assert len(forced) == 26
    assert len(df) == 26
    assert ( sources.band == 'r' ).all()
    assert ( forced.band == 'r' ).all()
    assert ( df.band == 'r' ).all()
    tmp = df.drop( 'ispatch', axis='columns' )
    assert ( tmp == forced ).all().all()


# There is another test of ltcv_object_search that uses loaded SNANA data
#   in test_ltcv_object_search.py
def test_object_search( set_of_lightcurves ):
    with pytest.raises( ValueError, match="Unknown search keywords: {'foo'}" ):
        _ = ltcv.object_search( processing_version='pvc_pv3', return_format='pandas', foo='bar' )

    # Search on ra/dec

    # A 17" search around (42,13) should find diaobjectids 200 and 201
    df = ltcv.object_search( processing_version='pvc_pv3', return_format='pandas',
                             ra=42., dec=13., radius=17. )
    assert len(df) == 2
    assert set( df.diaobjectid ) == { 200, 201 }
    assert df[ df.diaobjectid==200 ].ra.values[0] == 42.
    assert df[ df.diaobjectid==200 ].dec.values[0] == 13.
    assert df[ df.diaobjectid==200 ].numdet.values[0] == 13
    assert df[ df.diaobjectid==200 ].firstdetmjd.values[0] == 60000.
    assert df[ df.diaobjectid==200 ].firstdetband.values[0] == 'i'
    assert df[ df.diaobjectid==200 ].lastdetmjd.values[0] == 60030.
    assert df[ df.diaobjectid==200 ].lastdetband.values[0] == 'i'
    assert df[ df.diaobjectid==200 ].maxdetmjd.values[0] == 60010.
    assert df[ df.diaobjectid==200 ].maxdetband.values[0] == 'i'
    assert df[ df.diaobjectid==200 ].lastforcedmjd.values[0] == 60050.
    assert df[ df.diaobjectid==200 ].lastforcedband.values[0] == 'i'
    assert df[ df.diaobjectid==200 ].numdetinwindow.values[0] is None
    assert df[ df.diaobjectid==201 ].ra.values[0] == 42.
    assert df[ df.diaobjectid==201 ].dec.values[0] == 13.0036
    assert df[ df.diaobjectid==201 ].numdet.values[0] == 17
    assert df[ df.diaobjectid==201 ].firstdetmjd.values[0] == 60020.
    assert df[ df.diaobjectid==201 ].firstdetband.values[0] == 'r'
    assert df[ df.diaobjectid==201 ].lastdetmjd.values[0] == 60060.
    assert df[ df.diaobjectid==201 ].lastdetband.values[0] == 'r'
    assert df[ df.diaobjectid==201 ].maxdetmjd.values[0] == 60035.
    assert df[ df.diaobjectid==201 ].maxdetband.values[0] == 'r'
    assert df[ df.diaobjectid==201 ].lastforcedmjd.values[0] == 60080.
    assert df[ df.diaobjectid==201 ].lastforcedband.values[0] == 'r'
    assert df[ df.diaobjectid==201 ].numdetinwindow.values[0] is None

    # Check that json return works
    j = ltcv.object_search( processing_version='pvc_pv3', return_format='json',
                            ra=42., dec=13., radius=17. )
    for col in df.columns:
        # == isn't working in the None case... ?  Weird.  Different kind of None?
        if col == 'numdetinwindow':
            assert all( n is None for n in j[col] )
            assert all( n is None for n in df[col].values )
        else:
            assert ( df[col] == np.array( j[col] ) ).all()

    # Quick check that just_objids works
    df = ltcv.object_search( processing_version='pvc_pv3', return_format='pandas',
                             ra=42., dec=13., radius=17., just_objids=True )
    assert df.columns == [ 'diaobjectid' ]
    assert set( df.diaobjectid ) == { 200, 201 }
    j = ltcv.object_search( processing_version='pvc_pv3', return_format='json',
                            ra=42., dec=13., radius=17., just_objids=True )
    assert list( j.keys() ) == [ 'diaobjectid' ]
    assert set( j['diaobjectid'] ) == { 200, 201 }

    # Quick check that noforced works.  Well, sort of.  It tests that we don't
    #   get the forced columns back, but doesn't test that the function actually
    #   skipped searching that table.  But, whatevs.
    df = ltcv.object_search( processing_version='pvc_pv3', return_format='pandas',
                             ra=42., dec=13., radius=17., noforced=True )
    assert set( df.diaobjectid ) == { 200, 201 }
    assert all( i not in df.columns for i in [ 'lastforcedmjd', 'lastforcedband',
                                               'lastforcedflux', 'lastforcedfluxerr' ] )
    j = ltcv.object_search( processing_version='pvc_pv3', return_format='json',
                            ra=42., dec=13., radius=17., noforced=True )
    assert set( j['diaobjectid'] ) == { 200, 201 }
    assert all( i not in j.keys() for i in [ 'lastforcedmjd', 'lastforcedband',
                                             'lastforcedflux', 'lastforcedfluxerr' ] )

    # Quick bigger search; should get 3 of the 4 objects, ginormous search all of them
    df = ltcv.object_search( processing_version='pvc_pv3', return_format='pandas',
                             ra=42., dec=13., radius=1800. )
    assert len(df) == 3
    assert set( df.diaobjectid ) == { 200, 201, 202 }
    df = ltcv.object_search( processing_version='pvc_pv3', return_format='pandas',
                             ra=42., dec=13., radius=7200. )
    assert len(df) == 4
    assert set( df.diaobjectid ) == { 200, 201, 202, 203 }

    # If we ask for pvc_pv1, we should only find diaobject 200, and it should have different latest thingies
    df = ltcv.object_search( processing_version='pvc_pv1', return_format='pandas',
                             ra=42., dec=13., radius=7200. )
    assert len(df) == 1
    assert set( df.diaobjectid ) == { 100 }
    assert df.ra.values[0] == 42.
    assert df.dec.values[0] == 13.
    assert df.numdet.values[0] == 13
    assert df.lastforcedmjd.values[0] == 60025.

    # Try an earlier mjd_now
    df = ltcv.object_search( processing_version='pvc_pv3', return_format='pandas',
                             ra=42., dec=13., radius=17., mjd_now=60026. )
    assert len(df) == 2
    assert set( df.diaobjectid ) == { 200, 201 }
    assert df[ df.diaobjectid==200 ].ra.values[0] == 42.
    assert df[ df.diaobjectid==200 ].dec.values[0] == 13.
    assert df[ df.diaobjectid==200 ].numdet.values[0] == 11
    assert df[ df.diaobjectid==200 ].firstdetmjd.values[0] == 60000.
    assert df[ df.diaobjectid==200 ].firstdetband.values[0] == 'i'
    assert df[ df.diaobjectid==200 ].lastdetmjd.values[0] == 60025.
    assert df[ df.diaobjectid==200 ].lastdetband.values[0] == 'i'
    assert df[ df.diaobjectid==200 ].maxdetmjd.values[0] == 60010.
    assert df[ df.diaobjectid==200 ].maxdetband.values[0] == 'i'
    assert df[ df.diaobjectid==200 ].lastforcedmjd.values[0] == 60025.
    assert df[ df.diaobjectid==200 ].lastforcedband.values[0] == 'i'
    assert df[ df.diaobjectid==200 ].numdetinwindow.values[0] is None
    assert df[ df.diaobjectid==201 ].ra.values[0] == 42.
    assert df[ df.diaobjectid==201 ].dec.values[0] == 13.0036
    assert df[ df.diaobjectid==201 ].numdet.values[0] == 3
    assert df[ df.diaobjectid==201 ].firstdetmjd.values[0] == 60020.
    assert df[ df.diaobjectid==201 ].firstdetband.values[0] == 'r'
    assert df[ df.diaobjectid==201 ].lastdetmjd.values[0] == 60025.
    assert df[ df.diaobjectid==201 ].lastdetband.values[0] == 'r'
    assert df[ df.diaobjectid==201 ].maxdetmjd.values[0] == 60025.
    assert df[ df.diaobjectid==201 ].maxdetband.values[0] == 'r'
    assert df[ df.diaobjectid==201 ].lastforcedmjd.values[0] == 60025.
    assert df[ df.diaobjectid==201 ].lastforcedband.values[0] == 'r'

    # Throw in a window
    df = ltcv.object_search( processing_version='pvc_pv3', return_format='pandas',
                             ra=42., dec=13., radius=17., window_t0=60010, window_t1=60025 )
    assert len(df) == 2
    assert set( df.diaobjectid ) == { 200, 201 }
    assert df[ df.diaobjectid==200 ].numdetinwindow.values[0] == 7
    assert df[ df.diaobjectid==201 ].numdetinwindow.values[0] == 3
    # All of the following is cut and paste from the first search above
    assert df[ df.diaobjectid==200 ].ra.values[0] == 42.
    assert df[ df.diaobjectid==200 ].dec.values[0] == 13.
    assert df[ df.diaobjectid==200 ].numdet.values[0] == 13
    assert df[ df.diaobjectid==200 ].firstdetmjd.values[0] == 60000.
    assert df[ df.diaobjectid==200 ].firstdetband.values[0] == 'i'
    assert df[ df.diaobjectid==200 ].lastdetmjd.values[0] == 60030.
    assert df[ df.diaobjectid==200 ].lastdetband.values[0] == 'i'
    assert df[ df.diaobjectid==200 ].maxdetmjd.values[0] == 60010.
    assert df[ df.diaobjectid==200 ].maxdetband.values[0] == 'i'
    assert df[ df.diaobjectid==200 ].lastforcedmjd.values[0] == 60050.
    assert df[ df.diaobjectid==200 ].lastforcedband.values[0] == 'i'
    assert df[ df.diaobjectid==201 ].ra.values[0] == 42.
    assert df[ df.diaobjectid==201 ].dec.values[0] == 13.0036
    assert df[ df.diaobjectid==201 ].numdet.values[0] == 17
    assert df[ df.diaobjectid==201 ].firstdetmjd.values[0] == 60020.
    assert df[ df.diaobjectid==201 ].firstdetband.values[0] == 'r'
    assert df[ df.diaobjectid==201 ].lastdetmjd.values[0] == 60060.
    assert df[ df.diaobjectid==201 ].lastdetband.values[0] == 'r'
    assert df[ df.diaobjectid==201 ].maxdetmjd.values[0] == 60035.
    assert df[ df.diaobjectid==201 ].maxdetband.values[0] == 'r'
    assert df[ df.diaobjectid==201 ].lastforcedmjd.values[0] == 60080.
    assert df[ df.diaobjectid==201 ].lastforcedband.values[0] == 'r'

    # Now filter on numdetinwindow
    df = ltcv.object_search( processing_version='pvc_pv3', return_format='pandas',
                             ra=42., dec=13., radius=17., window_t0=60010, window_t1=60025,
                             min_window_numdetections=4 )
    assert len(df) == 1
    assert set( df.diaobjectid ) == { 200 }
    df = ltcv.object_search( processing_version='pvc_pv3', return_format='pandas',
                             ra=42., dec=13., radius=17., window_t0=60010, window_t1=60025,
                             max_window_numdetections=5 )
    assert len(df) == 1
    assert set( df.diaobjectid ) == { 201 }
    df = ltcv.object_search( processing_version='pvc_pv3', return_format='pandas',
                             ra=42., dec=13., radius=17., window_t0=60010, window_t1=60025,
                             min_window_numdetections=4, max_window_numdetections=5 )
    assert len(df) == 0


    # Filter on first detection
    df = ltcv.object_search( processing_version='pvc_pv3', return_format='pandas',
                             mint_firstdetection=60010 )
    assert len(df) == 3
    assert set( df.diaobjectid ) == { 201, 202, 203 }
    df = ltcv.object_search( processing_version='pvc_pv3', return_format='pandas',
                             maxt_firstdetection=60030 )
    assert len(df) == 2
    assert set( df.diaobjectid ) == { 200, 201 }
    df = ltcv.object_search( processing_version='pvc_pv3', return_format='pandas',
                             mint_firstdetection=60010, maxt_firstdetection=60030 )
    assert len(df) == 1
    assert set( df.diaobjectid ) == { 201 }

    df = ltcv.object_search( processing_version='pvc_pv3', return_format='pandas',
                             minmag_firstdetection=25.6 )
    assert len(df) == 2
    assert set( df.diaobjectid ) == { 200, 203 }
    df = ltcv.object_search( processing_version='pvc_pv3', return_format='pandas',
                             maxmag_firstdetection=25.9 )
    assert len(df) == 3
    assert set( df.diaobjectid ) == { 201, 202, 203 }
    df = ltcv.object_search( processing_version='pvc_pv3', return_format='pandas',
                             minmag_firstdetection=25.6, maxmag_firstdetection=25.9 )
    assert len(df) == 1
    assert set( df.diaobjectid ) == { 203 }

    # Filter on last detection
    df = ltcv.object_search( processing_version='pvc_pv3', return_format='pandas',
                             mint_lastdetection=60035 )
    assert len( df ) == 3
    assert set( df.diaobjectid ) == { 201, 202, 203 }
    df = ltcv.object_search( processing_version='pvc_pv3', return_format='pandas',
                             maxt_lastdetection=60065 )
    assert len( df ) == 3
    assert set( df.diaobjectid ) == { 200, 201, 203 }
    df = ltcv.object_search( processing_version='pvc_pv3', return_format='pandas',
                             mint_lastdetection=60035, maxt_lastdetection=60065 )
    assert len( df ) == 2
    assert set( df.diaobjectid ) == { 201, 203 }

    df = ltcv.object_search( processing_version='pvc_pv3', return_format='pandas',
                             minmag_lastdetection=25.5 )
    assert len(df) == 3
    assert set( df.diaobjectid ) == { 200, 202, 203 }
    df = ltcv.object_search( processing_version='pvc_pv3', return_format='pandas',
                             maxmag_lastdetection=25.8 )
    assert len(df) == 2
    assert set( df.diaobjectid ) == { 201, 202 }
    df = ltcv.object_search( processing_version='pvc_pv3', return_format='pandas',
                             minmag_lastdetection=25.5, maxmag_lastdetection=25.8 )
    assert len(df) == 1
    assert set( df.diaobjectid ) == { 202 }

    # Filter on max detection
    df = ltcv.object_search( processing_version='pvc_pv3', return_format='pandas',
                             mint_maxdetection=60020 )
    assert len(df) == 3
    assert set( df.diaobjectid ) == { 201, 202, 203 }
    df = ltcv.object_search( processing_version='pvc_pv3', return_format='pandas',
                             maxt_maxdetection=60052 )
    assert len(df) == 3
    assert set( df.diaobjectid ) == { 200, 201, 202 }
    df = ltcv.object_search( processing_version='pvc_pv3', return_format='pandas',
                             mint_maxdetection=60020, maxt_maxdetection=60052 )
    assert len(df) == 2
    assert set( df.diaobjectid ) == { 201, 202 }

    df = ltcv.object_search( processing_version='pvc_pv3', return_format='pandas',
                             minmag_maxdetection=22.5 )
    assert len(df) == 3
    assert set( df.diaobjectid ) == { 200, 202, 203 }
    df = ltcv.object_search( processing_version='pvc_pv3', return_format='pandas',
                             maxmag_maxdetection=24.5 )
    assert len(df) == 3
    assert set( df.diaobjectid ) == { 200, 201, 202 }
    df = ltcv.object_search( processing_version='pvc_pv3', return_format='pandas',
                             minmag_maxdetection=22.5, maxmag_maxdetection=24.5 )
    assert len(df) == 2
    assert set( df.diaobjectid ) == { 200, 202 }

    # To filter on lastmag, we need to use mjd_now, because the fixture filled out
    #   forced photometry all down to mag 32.
    df = ltcv.object_search( processing_version='pvc_pv3', return_format='pandas', mjd_now=60056. )
    df.set_index( 'diaobjectid', inplace=True )
    dexen = np.array( [ 200, 201, 202, 203 ] )
    assert list( df.loc[ dexen, 'lastforcedmjd' ] ) == [ 60050., 60055., 60055., 60055. ]
    df = ltcv.object_search( processing_version='pvc_pv3', return_format='pandas', mjd_now=60056.,
                             min_lastmag=24. )
    assert len(df) == 3
    assert set( df.diaobjectid ) == { 200, 201, 203 }
    df = ltcv.object_search( processing_version='pvc_pv3', return_format='pandas', mjd_now=60056.,
                             max_lastmag=24.8 )
    assert len(df) == 2
    assert set( df.diaobjectid ) == { 201, 202 }
    df = ltcv.object_search( processing_version='pvc_pv3', return_format='pandas', mjd_now=60056.,
                             min_lastmag= 24., max_lastmag=24.8 )
    assert len(df) == 1
    assert set( df.diaobjectid ) == { 201 }


    # TODO : test statbands.  (It is tested in test_ltcv_object_search.py)
    # (...and a good thing too, becasue it was broken.)


def test_get_hot_ltcvs( set_of_lightcurves ):
    # TODO : test hostdf (fixture doesn't currently load any hosts)

    # ...not sure how to test this without mjd_now since it uses the current time,
    #    and that will be different based on when this is run

    df, objdf, _ = ltcv.get_hot_ltcvs( 'pvc_pv3', detected_since_mjd=60035, mjd_now=60056 )
    assert set( df.index.names ) == { 'diaobjectid', 'mjd' }
    assert set( df.columns ) == { 'visit', 'band', 'flux', 'fluxerr', 'isdet' }
    assert set( df.index.get_level_values('diaobjectid') ) == { 201, 202, 203 }
    assert set( objdf.index.get_level_values('diaobjectid') ) == set( df.index.get_level_values('diaobjectid') )

    df, objdf, _ = ltcv.get_hot_ltcvs( 'pvc_pv3', detected_since_mjd=60035, mjd_now=60046 )
    assert set( df.index.get_level_values('diaobjectid') ) == { 201, 202 }
    assert set( objdf.index.get_level_values('diaobjectid') ) == set( df.index.get_level_values('diaobjectid') )

    df, objdf, _ = ltcv.get_hot_ltcvs( 'pvc_pv3', detected_in_last_days=2, mjd_now=60021 )
    assert set( df.index.get_level_values('diaobjectid') ) == { 200, 201 }
    assert set( objdf.index.get_level_values('diaobjectid') ) == set( df.index.get_level_values('diaobjectid') )

    df, objdf, _ = ltcv.get_hot_ltcvs( 'pvc_pv3', detected_in_last_days=2, mjd_now=60041 )
    assert set( df.index.get_level_values('diaobjectid') ) == { 201, 202 }
    assert set( objdf.index.get_level_values('diaobjectid') ) == set( df.index.get_level_values('diaobjectid') )

    # detected_in_last_days defaults to 30
    df, objdf, _ = ltcv.get_hot_ltcvs( 'pvc_pv3', mjd_now=60085 )
    assert set( df.index.get_level_values('diaobjectid') ) == { 201, 202, 203 }
    assert set( objdf.index.get_level_values('diaobjectid') ) == set( df.index.get_level_values('diaobjectid') )
    df, objdf, _ = ltcv.get_hot_ltcvs( 'pvc_pv3', mjd_now=60095 )
    assert set( df.index.get_level_values('diaobjectid') ) == { 202 }
    assert set( objdf.index.get_level_values('diaobjectid') ) == set( df.index.get_level_values('diaobjectid') )

    # Now let's look a the pulled forced photometry
    df, _, _ = ltcv.get_hot_ltcvs( 'pvc_pv3', detected_in_last_days=2, mjd_now=60041 )
    assert ( df.index.get_level_values('mjd') <= 60041. ).all()
    assert ( df.reset_index().groupby( 'diaobjectid' ).max().mjd == 60040. ).all()
    assert len( df.xs( 201, level='diaobjectid' ) ) == 13
    assert len( df.xs( 202, level='diaobjectid' ) ) == 5
    # ...should I look at the actual values?  I should.

    # Test source patch.  Gotta use pvc_pv1 for this.  (I guess we coulda also used realtime.)

    df, objdf, _ = ltcv.get_hot_ltcvs( 'pvc_pv1', mjd_now=60031 )
    assert set( df.index.get_level_values('diaobjectid') ) == { 100 }
    assert set( objdf.index.get_level_values('diaobjectid') ) == set( df.index.get_level_values('diaobjectid') )
    assert df.index.get_level_values('mjd').max() == 60025.

    df2, objdf2, _ = ltcv.get_hot_ltcvs( 'pvc_pv1', mjd_now=60031, source_patch=True )
    assert set( df2.columns ) == { 'visit', 'band', 'flux', 'fluxerr', 'isdet', 'ispatch' }
    assert set( df2.index.get_level_values('diaobjectid') ) == { 100 }
    assert set( objdf2.index.get_level_values('diaobjectid') ) == set( df2.index.get_level_values('diaobjectid') )
    assert df2.index.get_level_values('mjd').max() == 60030.
    assert len(df2) == len(df) + 2
    assert ( df == df2.loc[ df2.index.get_level_values('mjd') <= 60025., df2.columns != 'ispatch' ] ).all().all()
    assert df2[ df2.index.get_level_values('mjd') > 60025. ].isdet.all()
    assert not df2[ df2.index.get_level_values('mjd') <= 20025. ].ispatch.any()
    assert df2[ df2.index.get_level_values('mjd') > 60025. ].ispatch.all()
