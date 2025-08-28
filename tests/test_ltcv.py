import pytest
import numpy as np
import ltcv


def test_object_ltcv( procver_collection, set_of_lightcurves ):
    roots = set_of_lightcurves
    bpvs, pvs = procver_collection

    # The fixture loads up lightcurves every 2.5 days

    # Try to get the object lightcurve for the second object of roots[0] using pv1
    # Should get detections starting 60000, forced starting 50090,
    # sources through 60015 and forced through 60010 in bpv1a, then sources through 60030
    # and forced through 60050 in bpv1

    sources = ltcv.object_ltcv( pvs['pv1'].id, roots[0]['objs'][1]['obj'].diaobjectid,
                                return_format='pandas', which='detections', include_base_procver=True )
    forced = ltcv.object_ltcv( pvs['pv1'].id, roots[0]['objs'][1]['obj'].diaobjectid,
                               return_format='pandas', which='forced', include_base_procver=True )
    df = ltcv.object_ltcv( pvs['pv1'].id, roots[0]['objs'][1]['obj'].diaobjectid,
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

    import pdb; pdb.set_trace()
    pass

    # ROB YOU ARE HERE



#     bpvs, pvs = procver_collection
#     nobj, nroot, nsrc, nfrc = alerts_90days_sent_received_and_imported
#     assert nobj == 37
#     assert nroot == 37
#     assert nsrc == 181
#     assert nfrc == 855

#     objid = 1981540

#     sources = ltcv.object_ltcv( procver.id, objid, return_format='pandas', which='detections' )
#     forced = ltcv.object_ltcv( procver.id, objid, return_format='pandas', which='forced' )
#     df = ltcv.object_ltcv( procver.id, objid, return_format='pandas', which='patch' )

#     # I know that the patch version of the lightcurve should have 41 points, and that
#     #   it's more than either sources or forced individfually
#     assert len(df) == 41
#     assert len(df) > len(sources)
#     assert len(df) > len(forced)

#     # I know the filters we should expect
#     assert set( np.unique( sources.band.values ) ) == { 'g', 'r', 'i', 'z', 'Y' }
#     assert set( np.unique( forced.band.values ) ) == { 'u', 'g', 'r', 'i', 'z', 'Y' }
#     assert set( np.unique( df.band.values ) ) == { 'u', 'g', 'r', 'i', 'z', 'Y' }
#     # All sources are detections.  No forced sources are patched
#     assert np.all( sources.isdet )
#     assert not np.any( forced.ispatch )
#     # There are some sources for which there are no forced photometry points yet
#     assert sources.mjd.max() > forced.mjd.max()
#     # The combined df should include that latest source
#     assert df.mjd.max() == sources.mjd.max()
#     # Some of df should be patches, but not all
#     assert np.any( df.ispatch )
#     assert not np.all( df.ispatch )
#     # All of the forced photometry should be in df
#     assert np.all( df[ ~df.ispatch ].mjd.values == forced.mjd.values )
#     assert np.all( df[ ~df.ispatch ].psfflux.values == forced.psfflux.values )
#     assert np.all( df[ ~df.ispatch ].psffluxerr.values == forced.psffluxerr.values )
#     # Make sure the patches match up
#     tmpdf = df[ df.ispatch ].set_index( [ 'mjd', 'band' ] )
#     tmpsrc = sources.set_index( [ 'mjd', 'band' ] )
#     tmpjoin = tmpdf.join( tmpsrc, how='inner', lsuffix='_j', rsuffix='_s' )
#     assert len(tmpjoin) == len(tmpdf)
#     assert np.all( tmpjoin.psfflux_j.values == tmpjoin.psfflux_s.values )
#     assert np.all( tmpjoin.psffluxerr_j.values == tmpjoin.psffluxerr_s.values )

#     # Make sure that the json return matches the pandas return
#     jsondict = ltcv.object_ltcv( procver.id, objid, return_format='json', which='patch' )
#     assert set( jsondict.keys() ) == set( df.columns )
#     assert all( np.all( df[c].values == np.array(jsondict[c]) ) for c in df.columns )


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
