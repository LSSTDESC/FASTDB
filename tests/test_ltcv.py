import pytest
import numpy as np
import ltcv


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
    assert ( df == forced ).all().all()
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


    # TODO : test statbands


def test_get_hot_ltcvs( set_of_lightcurves ):
    # TODO : test hostdf (fixture doesn't currently load any hosts)

    # ...not sure how to test this without mjd_now since it uses the current time,
    #    and that will be different based on when this is run

    df, _ = ltcv.get_hot_ltcvs( 'pvc_pv3', detected_since_mjd=60035, mjd_now=60056 )
    assert len( df.diaobjectid.unique() ) == 3
    assert set( df.diaobjectid ) == { 201, 202, 203 }

    df, _ = ltcv.get_hot_ltcvs( 'pvc_pv3', detected_since_mjd=60035, mjd_now=60046 )
    assert len( df.diaobjectid.unique() ) == 2
    assert set( df.diaobjectid ) == { 201, 202 }

    df, _ = ltcv.get_hot_ltcvs( 'pvc_pv3', detected_in_last_days=2, mjd_now=60021 )
    assert len( df.diaobjectid.unique() ) == 2
    assert set( df.diaobjectid ) == { 200, 201 }

    df, _ = ltcv.get_hot_ltcvs( 'pvc_pv3', detected_in_last_days=2, mjd_now=60041 )
    assert len( df.diaobjectid.unique() ) == 2
    assert set( df.diaobjectid ) == { 201, 202 }

    # detected_in_last_days defaults to 30
    df, _ = ltcv.get_hot_ltcvs( 'pvc_pv3', mjd_now=60085 )
    assert set( df.diaobjectid ) == { 201, 202, 203 }
    df, _ = ltcv.get_hot_ltcvs( 'pvc_pv3', mjd_now=60095 )
    assert set( df.diaobjectid ) == { 202 }

    # Now let's look a the pulled forced photometry
    df, _ = ltcv.get_hot_ltcvs( 'pvc_pv3', detected_in_last_days=2, mjd_now=60041 )
    assert ( df.midpointmjdtai <= 60041. ).all()
    assert ( df.groupby( 'diaobjectid' ).max().midpointmjdtai == 60040. ).all()
    assert len( df[ df.diaobjectid==201 ] ) == 13
    assert len( df[ df.diaobjectid==202 ] ) == 5
    assert ( ~df.is_source ).all()
    # ...should I look at the actual values?  I should.

    # Test source patch.  Gotta use pvc_pv1 for this.

    df, _ = ltcv.get_hot_ltcvs( 'pvc_pv1', mjd_now=60031 )
    assert set( df.diaobjectid ) == { 100 }
    assert df.midpointmjdtai.max() == 60025.

    df2, _ = ltcv.get_hot_ltcvs( 'pvc_pv1', mjd_now=60031, source_patch=True )
    assert set( df2.diaobjectid ) == { 100 }
    assert df2.midpointmjdtai.max() == 60030.
    assert len(df2) == len(df) + 2
    assert ( df == df2[ df2.midpointmjdtai <= 60025. ] ).all().all()
    assert df2[ df2.midpointmjdtai > 60025. ].is_source.all()



# def test_get_hot_ltcvs( procver, alerts_90days_sent_received_and_imported ):
#     nobj, nroot, nsrc, nfrc = alerts_90days_sent_received_and_imported
#     assert nobj == 37
#     assert nroot == 37
#     assert nsrc == 181
#     assert nfrc == 855

#     df, hostdf = ltcv.get_hot_ltcvs( procver.description, detected_since_mjd=60325., mjd_now=60328. )

#     # Should have found 88 lightcurve points on 4 objects
#     assert len(df.rootid.unique()) == 4
#     assert len(df) == 88
#     # Make sure we don't have anything newer than now
#     assert df.midpointmjdtai.max() < 60328.
#     # We didn't ask for hosts
#     assert hostdf is None

#     # Now patch in sources where there aren't forced sources
#     df, hostdf = ltcv.get_hot_ltcvs( procver.description, detected_since_mjd=60325.,
#                                      mjd_now=60328., source_patch=True )

#     # Should have picked up 3 additional light curve points
#     assert len(df.rootid.unique()) == 4
#     assert len(df) == 91
#     assert df.is_source.sum() == 3

#     # Make sure everything was detected since 60325.  (We don't
#     #   actually have a detected flag, but check S/N > 5.)
#     # (Note that this next assert will *not* pass with the df you get
#     #   with source_patch=False, because there are some detections that
#     #   don't have corresponding forced photometry.)
#     assert ( set( df[ ( df.midpointmjdtai >= 60325. )
#                       & ( df.psfflux/df.psffluxerr > 5. )
#                     ].rootid.unique() )
#              == set( df.rootid.unique() ) )

#     # Make sure that we get the same thing using detected_in_last_days
#     df, hostdf = ltcv.get_hot_ltcvs( procver.description, detected_in_last_days=3.,
#                                      mjd_now=60328., source_patch=True )
#     assert hostdf is None
#     assert len(df.rootid.unique()) == 4
#     assert len(df) == 91
#     assert df.is_source.sum() == 3
#     assert ( set( df[ ( df.midpointmjdtai >= 60325. )
#                       & ( df.psfflux/df.psffluxerr > 5. )
#                     ].rootid.unique() )
#              == set( df.rootid.unique() ) )

#     # We should get more without passing a date limit, since it will do detected in the last 30 days
#     df, hostdf = ltcv.get_hot_ltcvs( procver.description, mjd_now=60328., source_patch=True )
#     assert df.midpointmjdtai.max() < 60328.
#     assert len(df.rootid.unique()) == 14
#     assert len(df) == 310
#     # In case you are surprised that this next value is more than before (since we're
#     # stopping at the same day, so we should have the same missing forced photometry),
#     # remember that at least as of right now, we don't have any "import updated forced
#     # photometry from the PPDB" routine going.  (We'll eventually want to think about
#     # that!  Issue #10.)  As such, there are going to be an number of objects whose last
#     # alert was not in the last three days, and the latest point from all of those alerts
#     # will not have forced photometry because there will not have been a later alert
#     # that would have it!
#     # (....in fact, that's the explanation for *all* of the missing forced photometry,
#     # because the database has times later than mjd 60328 based on what we ran in the fixture.)
#     assert df.is_source.sum() == 12

#     # Empirically, there's a detection that only has a S/N of ~2.8.  This highlights that
#     #   detection is more complicated than S/N > 5.  So, throw in "is_source=True" to
#     #   capture this detection.  is_source=True by itself is not enough to tell if it's a
#     #   detection, because we will only have included sources for which there was no
#     #   diaforcedsource.
#     assert ( set( df[ ( df.midpointmjdtai >= 60297. )
#                       & ( ( df.psfflux/df.psffluxerr > 5. ) | ( df.is_source ) )
#                     ].rootid.unique() )
#              == set( df.rootid.unique() ) )


#     # Now lets get hosts
#     df, hostdf = ltcv.get_hot_ltcvs( procver.description, detected_since_mjd=60325., mjd_now=60328.,
#                                      source_patch=True, include_hostinfo=True )
#     assert len(df) == 91
#     assert len(hostdf) == 4
#     assert set( hostdf.rootid ) == set( df.rootid.unique() )

#     # TODO : more stringent tests
