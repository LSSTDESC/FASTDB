import pytest
import numpy as np
import pandas

import db
import ltcv


def test_get_object_infos( set_of_lightcurves, procver_collection ):
    bpvs, _pvs = procver_collection
    roots = set_of_lightcurves

    info = ltcv.get_object_infos( [ 200, 201, 202 ], return_format='pandas',
                                  processing_version='pvc_pv2', position_processing_version='pvc_pv1' )
    assert info.index.name == 'diaobjectid'
    assert set(info.columns.values) == { 'rootid', 'obj_base_procver', 'pos_base_procver',
                                         'ra', 'dec', 'raerr', 'decerr', 'ra_dec_cov' }
    assert len(info) == 3
    assert list( info.index.values ) == [ 200, 201, 202 ]
    assert info.rootid.values.tolist() == [ roots[i]['root'].id for i in [ 0, 1, 2 ] ]
    # Since we gave a position processing versoin that was inconsistent with the diaobject
    #   processing version, none of the position fields should be filled.
    assert all( all( ( i is None ) or pandas.isna(i) for i in info[col] )
                for col in ['pos_base_procver', 'ra', 'dec', 'raerr', 'decerr', 'ra_dec_cov'] )

    # Make sure we get position information if we use the position processing default
    #   to the processing version
    info = ltcv.get_object_infos( [ 200, 201, 202 ], return_format='pandas', processing_version='pvc_pv2' )
    assert list( info.index.values ) == [ 200, 201, 202 ]
    assert all( all( ( i is not None ) and ( not pandas.isna(i) ) for i in info[col] )
                for col in ['pos_base_procver', 'ra', 'dec', 'raerr', 'decerr', 'ra_dec_cov'] )
    assert info.loc[ 200, 'pos_base_procver'] == bpvs['bpv2a_diaobject_position_60030'].description
    assert info.loc[ 201, 'pos_base_procver'] == bpvs['bpv2a_diaobject_position_60030'].description
    assert info.loc[ 202, 'pos_base_procver'] == bpvs['bpv2_diaobject_position_60080'].description
    info2 = ltcv.get_object_infos( [ 200, 201, 202 ], return_format='pandas', processing_version='pvc_pv2',
                                   position_processing_version='pvc_pv2' )
    assert info2.equals( info )

    # Make sure json return gives the same stuff
    jsinfo = ltcv.get_object_infos( [ 200, 201, 202 ], processing_version='pvc_pv2', return_format='json' )
    assert jsinfo['diaobjectid'] == [ 200, 201, 202 ]
    info.reset_index( inplace=True )
    for col in info.columns:
        if col in { 'ra', 'dec', 'raerr', 'decerr', 'ra_dec_cov' }:
            assert np.array( info.loc[:, col].values ) == pytest.approx( np.array( jsinfo[col] ), rel=1e-5 )
        else:
            assert ( np.array( info.loc[:, col].values ) == np.array( jsinfo[col] ) ).all()

    # TODO : right now there are no diaobjects in the default processing version!  Fix that in Issue #70.
    info = ltcv.get_object_infos( [ roots[i]['root'].id for i in [0, 1, 2] ], return_format='pandas' )
    assert list( info.index.values ) == [ 200, 201, 202, 2011 ]
    assert all( b == bpvs['bpv2_diaobject'].description for b in info['obj_base_procver'] )
    assert info.loc[ 200, 'pos_base_procver' ] == bpvs['bpv2a_diaobject_position_60030'].description
    assert info.loc[ 201, 'pos_base_procver' ] == bpvs['bpv2a_diaobject_position_60030'].description
    assert info.loc[ 2011, 'pos_base_procver' ] is None
    assert info.loc[ 202, 'pos_base_procver' ] == bpvs['bpv2_diaobject_position_60080'].description

    info2 = ltcv.get_object_infos( [ roots[i]['root'].id for i in [0, 1, 2] ], processing_version='pvc_pv2',
                                   return_format='pandas' )
    assert ( info2.rootid == info.rootid ).all()
    assert ( info2.obj_base_procver == info.obj_base_procver ).all()
    # The None/<NA> values aren't comparing as equal, probably because of the whole "all nan tests are False" thing
    assert ( info2.loc[ [ 200, 201, 202 ], : ] == info.loc[ [ 200, 201, 202], : ] ).all().all()



    info = ltcv.get_object_infos( [ 200, 201, 202 ], columns=['ra', 'dec'], processing_version='pvc_pv2',
                                  return_format='pandas' )
    assert info.index.values.tolist() == [ 200, 201, 202 ]
    assert set( info.keys() ) == { 'ra', 'dec' }

    with db.DBCon() as dbcon:
        dbcon.execute( "CREATE TEMP TABLE tempthing(diaobjectid bigint)", explain=False )
        dbcon.execute( "INSERT INTO tempthing(diaobjectid) VALUES ( 200 )" )
        dbcon.execute( "INSERT INTO tempthing(diaobjectid) VALUES ( 202 )" )
        info = ltcv.get_object_infos( objids_table='tempthing', dbcon=dbcon, processing_version='pvc_pv2',
                                      return_format='pandas' )
        assert info.index.values.tolist() == [ 200, 202 ]
        assert all( info['rootid'] == [ roots[i]['root'].id for i in [ 0, 2 ] ] )
        assert all ( all( ( i is not None ) and ( not pandas.isna(i) ) for i in info[col] )
                     for col in [ 'pos_base_procver', 'ra', 'dec', 'raerr', 'decerr', 'ra_dec_cov' ] )
        # If we pass an inconjsistent processing version, we should get nothing back
        info = ltcv.get_object_infos( objids_table='tempthing', dbcon=dbcon, processing_version='pvc_pv3',
                                      return_format='pandas' )
        assert len( info ) == 0

        dbcon.execute( "DROP TABLE tempthing" )
        dbcon.execute( "CREATE TEMP TABLE tempthing(rootid uuid)", explain=False )
        dbcon.execute( "INSERT INTO tempthing(rootid) VALUES (%(id)s)", { 'id': roots[1]['root'].id } )
        dbcon.execute( "INSERT INTO tempthing(rootid) VALUES (%(id)s)", { 'id': roots[3]['root'].id } )
        info = ltcv.get_object_infos( objids_table='tempthing', dbcon=dbcon, processing_version='pvc_pv2',
                                      return_format='pandas' )
        assert info.index.values.tolist() == [ 201, 203, 2011 ]
        info = ltcv.get_object_infos( objids_table='tempthing', dbcon=dbcon, processing_version='realtime',
                                      return_format='pandas' )
        assert info.index.values.tolist() == [ 1 ]
        assert all( all( ( i is not None ) and ( not pandas.isna(i) ) for i in info[col] )
                    for col in [ 'pos_base_procver', 'ra', 'dec', 'raerr', 'decerr', 'ra_dec_cov' ] )

        # If the temp table has both rootid and diaobjectid, it should use diaobjectid.  Test this
        # By passing an inconsistent input
        dbcon.execute( "DROP TABLE tempthing" )
        dbcon.execute( "CREATE TABLE tempthing(rootid uuid, diaobjectid bigint)" )
        dbcon.execute( "INSERT INTO tempthing VALUES (%(id)s, 200)", { 'id': roots[1]['root'].id } )
        dbcon.execute( "INSERT INTO tempthing VALUES (%(id)s, 202)", { 'id': roots[3]['root'].id } )
        info = ltcv.get_object_infos( objids_table='tempthing', dbcon=dbcon, processing_version='pvc_pv2',
                                     return_format='pandas' )
        assert info.index.values.tolist() == [ 201, 203, 2011 ]

        # Check failures
        with pytest.raises( ValueError, match='objids_table requires dbcon' ):
            ltcv.get_object_infos( objids_table='tempthing', processing_version='pvc_pv2' )

        with pytest.raises( ValueError, match='objids_table and objids cannot be used together' ):
            ltcv.get_object_infos( objids_table='tempthing', objids=[0, 1, 2], dbcon=dbcon,
                                   processing_version='pvc_pv2' )

        with pytest.raises( ValueError, match='objids_table and objids cannot be used together' ):
            ltcv.get_object_infos( [0, 1, 2], objids_table='tempthing', dbcon=dbcon,
                                   processing_version='pvc_pv2' )


def test_object_ltcv( procver_collection, set_of_lightcurves ):
    # TODO : write a test for the case where there are multiple objects within the
    #   same processing version that point to the same root object!

    roots = set_of_lightcurves
    _bpvs, pvs = procver_collection

    # The fixture loads up lightcurves every 2.5 days

    # Try to get the object lightcurve for diaobjectid 100 using pv1
    # Should get detections starting 60000, forced starting 59990,
    # sources through 60015 and forced through 60010 in bpv1a,
    # sources through 60030 and forced through 60025 in bpv1

    srcs = ltcv.object_ltcv( pvs['pv1'].id, 100, return_format='pandas', which='detections', include_base_procver=True )
    forced = ltcv.object_ltcv( pvs['pv1'].id, 100, return_format='pandas', which='forced', include_base_procver=True )
    df = ltcv.object_ltcv( pvs['pv1'].id, 100, return_format='pandas', which='patch', include_base_procver=True )

    assert len(srcs) == 13
    assert len(forced) == 15
    assert len(df) == 17

    assert np.all( ( srcs.mjd >= 60000 ) & ( srcs.mjd <= 60030 ) )
    assert np.all( srcs[ srcs.mjd <= 60015 ].base_procver_s == 'pvc_bpv1a' )
    assert np.all( srcs[ srcs.mjd > 60015 ].base_procver_s == 'pvc_bpv1' )
    assert np.all( srcs.isdet )
    assert np.all( forced[ forced.mjd <= 60010 ].base_procver_f == 'pvc_bpv1a' )
    assert np.all( forced[ forced.mjd > 60010 ].base_procver_f == 'pvc_bpv1' )
    assert np.all( forced[ ( forced.mjd >= 60000 ) & ( forced.mjd <= 60030 ) ].isdet )
    assert np.all( ~forced[ ( forced.mjd < 60000 ) | ( forced.mjd > 60030 ) ].isdet )
    assert np.all( pandas.isna( df[ df.mjd < 60000 ].base_procver_s ) )
    assert np.all( df[ ( df.mjd >= 60000 ) & ( df.mjd <= 60015 ) ].base_procver_s == 'pvc_bpv1a' )
    assert np.all( df[ df.mjd > 60015 ].base_procver_s == 'pvc_bpv1' )
    assert np.all( df[ df.mjd <= 60010 ].base_procver_f == 'pvc_bpv1a' )
    assert np.all( df[ ( df.mjd > 60010 ) & ( df.mjd <= 60025 ) ].base_procver_f == 'pvc_bpv1' )
    assert np.all( pandas.isna( df[ df.mjd > 60025 ].base_procver_f ) )
    assert np.all( df[ df.mjd > 60025 ].ispatch )
    assert np.all( ~df[ df.mjd <= 60025 ].ispatch )
    assert np.all( df[ ( df.mjd >= 60000 ) & ( df.mjd <= 60030 ) ].isdet )
    assert np.all( ~df[ ( df.mjd < 60000 ) | ( df.mjd > 60030 ) ].isdet )
    # Because we didn't say include_source_positions, there should be no source columns
    for f in [ df, forced, srcs ]:
        for c in [ 'ra', 'dec', 'raerr', 'decerr', 'ra_dec_cov' ]:
            assert c not in f.columns

    # If we ask for roots[1] from pv1, we shouldn't get anything.
    # (Also trying using the root object this time.)

    with pytest.raises( RuntimeError, match="Could not find object for diaobjectid" ):
        df = ltcv.object_ltcv( pvs['pv1'].id, roots[1]['root'].id, return_format='pandas', which='patch',
                               include_base_procver=True )

    # But if we ask for roots[1] from pv2, we should get stuff.  In this case, patch and forced
    # should be the same (except for patch having the ispatch column)

    srcs = ltcv.object_ltcv( pvs['pv2'].id, roots[1]['root'].id, return_format='pandas',
                             which='detections', include_base_procver=True )
    forced = ltcv.object_ltcv( pvs['pv2'].id, roots[1]['root'].id, return_format='pandas',
                               which='forced', include_base_procver=True )
    df = ltcv.object_ltcv( pvs['pv2'].id, roots[1]['root'].id, return_format='pandas',
                           which='patch', include_base_procver=True )
    assert ( df.ispatch == 0 ).all()
    assert len( df[ df.isdet == 1 ] ) == len( srcs )
    assert df[ df.isdet == 1 ].isdet.all()
    assert ( df[ df.ispatch == 1 ].ispatch ).all()
    assert all( pandas.isna( df[ df.mjd < 60020 ].base_procver_s ) )
    assert all( pandas.isna( df[ df.mjd > 60060 ].base_procver_s ) )
    assert all( df[ (df.mjd >= 60020) & (df.mjd <= 60030) ] == 'pvc_bpv2a' )
    assert all( df[ (df.mjd > 60030) & (df.mjd <= 60060) ] == 'pvc_bpv2' )
    assert all( df[ (df.mjd < 60020) | (df.mjd >60025) ].base_procver_f == 'pvc_bpv2' )
    assert all( df[ (df.mjd >= 60020) & (df.mjd <= 60025) ].base_procver_f == 'pvc_bpv2a' )
    # df and forced should match, only doing a straight comparison doesn't work again
    #   because of the whole (na == na) being false thing.
    assert ( df[ ~pandas.isna( df.base_procver_s ) ].loc[ :, [c for c in df.columns if c != 'ispatch'] ]
             == forced[ ~pandas.isna( forced.base_procver_s ) ]
            ).all().all()
    # Look at just the columns that have no nulls
    assert ( df.loc[ :, [c for c in df.columns if c not in [ 'ispatch', 'diasourceid', 'base_procver_s' ] ] ]
             == forced.loc[ :, [c for c in forced.columns if c not in [ 'diasourceid', 'base_procver_s' ] ] ]
            ).all().all()

    # Make sure json output is consistent

    j_srcs = ltcv.object_ltcv( pvs['pv2'].id, roots[1]['root'].id, return_format='json',
                               which='detections', include_base_procver=True )
    j_forced = ltcv.object_ltcv( pvs['pv2'].id, roots[1]['root'].id, return_format='json',
                                 which='forced', include_base_procver=True )
    j_df = ltcv.object_ltcv( pvs['pv2'].id, roots[1]['root'].id, return_format='json',
                             which='patch', include_base_procver=True )

    for js, pd in zip( [ j_srcs, j_forced, j_df ], [ srcs, forced, df ] ):
        assert isinstance( js, dict )
        for col in pd.columns:
            if col == 'rootid':
                continue
            wnonnull = np.where( ~pandas.isna( pd[col] ) )[0]
            assert ( pd[col][wnonnull] == np.array( js[col] )[wnonnull] ).all()
            wnull = np.where( pandas.isna( pd[col] ) )[0]
            # pandas.isna catches both pandas-specific <NA> and None
            assert all( pandas.isna( np.array( js[col] )[wnull] ) )

    # Make sure we can pass a string processing version

    df2 = ltcv.object_ltcv( 'pvc_pv2', roots[1]['root'].id, return_format='pandas',
                            which='patch', include_base_procver=True )
    assert len(df) == len(df2)
    assert ( df[ ~pandas.isna(df.diasourceid) ] == df2[ ~pandas.isna(df2.diasourceid) ] ).all().all()
    assert ( df.loc[ :, [ c for c in df.columns if c not in [ 'diasourceid', 'base_procver_s' ] ] ] ==
             df2.loc[ :, [ c for c in df2.columns if c not in [ 'diasourceid', 'base_procver_s' ] ] ]
            ).all().all()

    # Test mjd_now
    df = ltcv.object_ltcv( pvs['pv2'].id, roots[1]['root'].id, return_format='pandas',
                           which='patch', include_base_procver=True, mjd_now=60040 )
    assert ( df.mjd <= 60040 ).all()
    assert len( df ) == 13

    # Test include_source_positions

    srcs = ltcv.object_ltcv( pvs['pv2'].id, 200, return_format='pandas', which='detections',
                             include_base_procver=True, include_source_positions=True )
    forced = ltcv.object_ltcv( pvs['pv2'].id, 200, return_format='pandas', which='forced',
                               include_base_procver=True, include_source_positions=True )
    df = ltcv.object_ltcv( pvs['pv2'].id, 200, return_format='pandas', which='patch',
                           include_base_procver=True, include_source_positions=True )
    for field in [ 'det_ra', 'det_dec' ]:
        assert not any( srcs[field].isna() )
        assert not any( forced[ forced.isdet == True ][field].isna() )
        assert all( forced[ forced.isdet == False ][field].isna() )
        assert not any( df[ df.isdet == True ][field].isna() )
        assert all( df[ df.isdet == False ][field].isna() )


    # Test return_object_info

    srcs2, srcinfo = ltcv.object_ltcv( pvs['pv2'].id, 200, return_format='pandas', which='detections',
                                       include_base_procver=True, include_source_positions=True,
                                       return_object_info=True )
    forced2, frcinfo = ltcv.object_ltcv( pvs['pv2'].id, 200, return_format='pandas', which='forced',
                                         include_base_procver=True, include_source_positions=True,
                                         return_object_info=True )
    df2, dfinfo = ltcv.object_ltcv( pvs['pv2'].id, 200, return_format='pandas', which='patch',
                                    include_base_procver=True, include_source_positions=True,
                                    return_object_info=True )
    assert ( srcinfo == frcinfo ).all().all()
    assert ( srcinfo == dfinfo ).all().all()
    assert list( dfinfo.obj_base_procver )[0]== 'pvc_bpv2'
    assert list( dfinfo.pos_base_procver )[0] == 'pvc_bpv2a_60030'
    assert list( dfinfo.rootid )[0] == roots[0]['root'].id
    assert list( dfinfo.ra )[0] == pytest.approx( roots[0]['pos'][(200, 'bpv2a_diaobject_position_60030')].ra,
                                                  rel=1e-14 )
    assert list( dfinfo.dec )[0] == pytest.approx( roots[0]['pos'][(200, 'bpv2a_diaobject_position_60030')].dec,
                                                   rel=1e-14 )
    assert list( dfinfo.raerr )[0] == pytest.approx( roots[0]['pos'][(200, 'bpv2a_diaobject_position_60030')].raerr,
                                                     rel=1e-6 )
    assert list( dfinfo.decerr )[0] == pytest.approx( roots[0]['pos'][(200, 'bpv2a_diaobject_position_60030')].decerr,
                                                      rel=1e-6 )
    assert list( dfinfo.ra_dec_cov )[0] == pytest.approx( roots[0]['pos']
                                                          [(200, 'bpv2a_diaobject_position_60030')].ra_dec_cov,
                                                          rel=1e-6 )
    for olddf, newdf in zip( [ srcs, forced, df ], [ srcs2, forced2, df2 ] ):
        assert len(olddf) == len(newdf)
        assert ( olddf[ ~pandas.isna(df.diasourceid) ] == newdf[ ~pandas.isna(df2.diasourceid) ] ).all().all()
        scarycolumns = [ 'diasourceid', 'base_procver_s', 'det_ra', 'det_dec',
                         'det_raerr', 'det_decerr', 'det_ra_dec_cov' ]
        assert ( olddf[ ~pandas.isna(df.det_ra) ] == newdf[ ~pandas.isna(df2.det_ra) ] ).all().all()
        for col in scarycolumns:
            assert ( olddf[ pandas.isna(olddf[col]) ].loc[ :, 'mjd' ]
                     == newdf[ pandas.isna(newdf[col]) ].loc[ :, 'mjd' ] ).all()
        assert ( olddf.loc[ :, [ c for c in olddf.columns if c not in scarycolumns ] ] ==
                 newdf.loc[ :, [ c for c in newdf.columns if c not in scarycolumns ] ]
                ).all().all()

    # return_object_info and json

    jsrc2, jsrcinfo = ltcv.object_ltcv( pvs['pv2'].id, 200, return_format='json', which='detections',
                                        include_base_procver=True, include_source_positions=True,
                                        return_object_info=True )
    jforced2, jfrcinfo = ltcv.object_ltcv( pvs['pv2'].id, 200, return_format='json', which='forced',
                                           include_base_procver=True, include_source_positions=True,
                                           return_object_info=True )
    jdf2, jdfinfo = ltcv.object_ltcv( pvs['pv2'].id, 200, return_format='json', which='patch',
                                      include_base_procver=True, include_source_positions=True,
                                      return_object_info=True )
    for j, p in zip( [ jsrcinfo, jfrcinfo, jdfinfo ], [ srcinfo, frcinfo, dfinfo ] ):
        assert j['diaobjectid'] == list( p.index.values )
        for col in p.columns:
            assert list( j[col] ) == list( p[col] )
    for js, pd in zip( [ jsrc2, jforced2, jdf2 ], [ srcs2, forced2, df2 ] ):
        assert isinstance( js, dict )
        for col in pd.columns:
            if col == 'rootid':
                continue
            wnonnull = np.where( ~pandas.isna( pd[col] ) )[0]
            assert ( pd[col][wnonnull] == np.array( js[col] )[wnonnull] ).all()
            wnull = np.where( pandas.isna( pd[col] ) )[0]
            # pandas.isna catches both pandas-specific <NA> and None
            assert all( pandas.isna( np.array( js[col] )[wnull] ) )

    # test use_weighted_source_position; diaobject 203 has no positions.
    # First, make sure we see that
    src, srcinfo = ltcv.object_ltcv( pvs['pv2'].id, 203, return_format='pandas', which='detections',
                                     return_object_info=True )
    frc, frcinfo = ltcv.object_ltcv( pvs['pv2'].id, 203, return_format='pandas', which='forced',
                                     return_object_info=True )
    pat, patinfo = ltcv.object_ltcv( pvs['pv2'].id, 203, return_format='pandas', which='patch',
                                     return_object_info=True )
    for info in [ srcinfo, frcinfo, patinfo ]:
        assert all( pandas.isna( info.loc[ :, [ 'ra', 'dec', 'raerr', 'decerr', 'ra_dec_cov' ] ] ) )

    # # Now check positions

    raise RuntimeError( """
    TODO : fix this next functon so that you pass a list of keys, and it will then go through
      and find sources from the priority list.  That lets us compare to something where we'll
      get a mix of base processing versions.
    """ )

    def check_pos( infodf, srces ):
        srcra = np.array( [ i.ra for i in srces ] )
        srcdec = np.array( [ i.dec for i in srces ] )
        sn = np.array( [ i.psfflux / i.psffluxerr for i in srces ] )
        w = np.where( sn > 3 )[0]
        srcra = srcra[w]
        srcdec = srcdec[w]
        weight = sn[w] ** 2
        meanra = ( srcra * weight ).sum() / ( weight.sum() )
        meandec = ( srcdec * weight ).sum() / ( weight.sum() )
        raerr = np.sqrt( ( weight * ( srcra - meanra )**2 ).sum() / weight.sum() )
        decerr = np.sqrt( ( weight * ( srcdec - meandec )**2 ).sum() / weight.sum() )
        ra_dec_cov = ( weight * ( srcra - meanra ) * ( srcdec - meandec ) ) / weight.sum()

        assert list( infodf.ra )[0] == pytest.approx( meanra, rel=1e-14 )
        assert list( infodf.dec )[0] == pytest.approx( meandec, rel=1e-14 )
        assert list( infodf.raerr )[0] == pytest.approx( raerr, rel=1e-6 )
        assert list( infodf.decerr )[0] == pytest.approx( decerr, rel=1e-6 )
        # ...this next line checking that 0 = 0 to a relative precision of 1e-6, which is hopeless.
        # assert list( infodf.ra_dec_cov )[0] == pytest.approx( ra_dec_cov, rel=1e-6 )
        assert list( infodf.ra_dec_cov )[0] == pytest.approx( 0., abs=1e-10 )
        assert ra_dec_cov == pytest.approx( 0., abs=1e-10 )

    # src, srcinfo = ltcv.object_ltcv( pvs['pv2'].id, 203, return_format='pandas', which='detections',
    #                                  return_object_info=True, use_weighted_source_positions=True )
    # frc, frcinfo = ltcv.object_ltcv( pvs['pv2'].id, 203, return_format='pandas', which='forced',
    #                                  return_object_info=True, use_weighted_source_positions=True )
    # pat, patinfo = ltcv.object_ltcv( pvs['pv2'].id, 203, return_format='pandas', which='patch',
    #                                  return_object_info=True, use_weighted_source_positions=True )
    # check_pos( srcinfo, roots[3]['src']['bpv2_diasource'] )
    # check_pos( frcinfo, roots[3]['src']['bpv2_diasource'] )
    # check_pos( patinfo, roots[3]['src']['bpv2_diasource'] )

    # Next, check always_use_source_positions by passing object 200, which does have positions

    # First, if we use use_weighted_source_positions but no always, we should get the
    #   diaobject_position position back
    src, srcinfo = ltcv.object_ltcv( pvs['pv2'].id, 200, return_format='pandas', which='detections',
                                     return_object_info=True, use_weighted_source_positions=True,
                                     include_base_procver=True )
    frc, frcinfo = ltcv.object_ltcv( pvs['pv2'].id, 200, return_format='pandas', which='forced',
                                     return_object_info=True, use_weighted_source_positions=True,
                                     include_base_procver=True )
    pat, patinfo = ltcv.object_ltcv( pvs['pv2'].id, 200, return_format='pandas', which='patch',
                                     return_object_info=True, use_weighted_source_positions=True,
                                     include_base_procver=True )
    for info in [ srcinfo, frcinfo, patinfo ]:
        assert info.pos_base_procver.iloc[0] == 'pvc_bpv2a_60030'
        dex = ( 200, 'bpv2a_diaobject_position_60030' )
        assert info.ra.iloc[0] == pytest.approx( roots[0]['pos'][dex].ra, rel=1e-14 )
        assert info.dec.iloc[0] == pytest.approx( roots[0]['pos'][dex].dec, rel=1e-14 )
        assert info.raerr.iloc[0] == pytest.approx( roots[0]['pos'][dex].raerr, rel=1e-6 )
        assert info.decerr.iloc[0] == pytest.approx( roots[0]['pos'][dex].decerr, rel=1e-6 )
        assert info.ra_dec_cov.iloc[0] == pytest.approx( roots[0]['pos'][dex].ra_dec_cov, rel=1e-6 )

    # Now put in always_....

    src, srcinfo = ltcv.object_ltcv( pvs['pv1'].id, 200, return_format='pandas', which='detections',
                                     return_object_info=True, always_use_weighted_source_positions=True,
                                     include_base_procver=True )
    frc, frcinfo = ltcv.object_ltcv( pvs['pv1'].id, 200, return_format='pandas', which='forced',
                                     return_object_info=True, always_use_weighted_source_positions=True,
                                     include_base_procver=True )
    pat, patinfo = ltcv.object_ltcv( pvs['pv2'].id, 200, return_format='pandas', which='patch',
                                     return_object_info=True, always_use_weighted_source_positions=True,
                                     include_base_procver=True )
    for info in [ srcinfo, frcinfo, patinfo ]:
        # Should *not* match the diaobject_position value
        assert all( pandas.isna( info.pos_base_procver ) )
        dex = ( 200, 'bpv2a_diaobject_position_60030' )
        assert info.ra.iloc[0] != pytest.approx( roots[0]['pos'][dex].ra, abs=0.01/3600 )
        assert info.dec.iloc[0] != pytest.approx( roots[0]['pos'][dex].dec, abs=0.01/3600 )

    # But should match the mean source position
    import pdb; pdb.set_trace()
    check_pos( srcinfo, roots[2]['src']['bpv2_diasource'] )
    check_pos( frcinfo, roots[2]['src']['bpv2_diasource'] )
    check_pos( patinfo, roots[2]['src']['bpv2_diasource'] )



    import pdb; pdb.set_trace()
    pass



def test_many_object_ltcvs( procver_collection, set_of_lightcurves ):
    roots = set_of_lightcurves
    _bpvs, pvs = procver_collection

    # First, reproduce the tests from test_object_ltcv.  We'll ask for two lightcurves,
    #   but only one is going to be present.

    srcs = ltcv.many_object_ltcvs( pvs['pv1'].id, [ roots[i]['root'].id for i in [0,1] ],
                                   return_format='pandas', which='detections', include_base_procver=True )
    forced = ltcv.many_object_ltcvs( pvs['pv1'].id, [ roots[i]['root'].id for i in [0,1] ],
                                     return_format='pandas', which='forced', include_base_procver=True )
    df = ltcv.many_object_ltcvs( pvs['pv1'].id, [ roots[i]['root'].id for i in [0,1] ],
                                 return_format='pandas', which='patch', include_base_procver=True )

    assert set( srcs.index.get_level_values( 'rootid' ).unique().values ) == { roots[0]['root'].id }
    assert set( forced.index.get_level_values( 'rootid' ).unique().values ) == { roots[0]['root'].id }
    assert set( df.index.get_level_values( 'rootid' ).unique().values ) == { roots[0]['root'].id }

    srcs.reset_index( inplace=True )
    srcs.drop( 'rootid', axis='columns', inplace=True )
    forced.reset_index( inplace=True )
    forced.drop( 'rootid', axis='columns', inplace=True )
    df.reset_index( inplace=True )
    df.drop( 'rootid', axis='columns', inplace=True )

    # These checks are (sorta) copied from test_object_ltcv
    assert len(srcs) == 13
    assert len(forced) == 15
    assert len(df) == 17

    assert np.all( ( srcs.mjd >= 60000 ) & ( srcs.mjd <= 60030 ) )
    assert np.all( srcs[ srcs.mjd <= 60015 ].base_procver == 'pvc_bpv1a' )
    assert np.all( srcs[ srcs.mjd > 60015 ].base_procver == 'pvc_bpv1' )
    assert np.all( srcs.isdet )
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
    # Because we didn't say include_source_positions, there should be no source columns
    for f in [ df, forced, srcs ]:
        for c in [ 'ra', 'dec', 'raerr', 'decerr', 'ra_dec_cov' ]:
            assert c not in f.columns

    # Now ask for two lightcurves where we expect to get two lightcurves
    srcs = ltcv.many_object_ltcvs( pvs['pv2'].id, [ roots[i]['root'].id for i in [0,2] ],
                                   return_format='pandas', which='detections', include_base_procver=True )
    forced = ltcv.many_object_ltcvs( pvs['pv2'].id, [ roots[i]['root'].id for i in [0,2] ],
                                     return_format='pandas', which='forced', include_base_procver=True )
    df = ltcv.many_object_ltcvs( pvs['pv2'].id, [ roots[i]['root'].id for i in [0,2] ],
                                 return_format='pandas', which='patch', include_base_procver=True )
    assert ( set( srcs.index.get_level_values( 'rootid' ).unique().values )
             == { roots[0]['root'].id, roots[2]['root'].id } )
    assert ( set( forced.index.get_level_values( 'rootid' ).unique().values )
             == { roots[0]['root'].id, roots[2]['root'].id } )
    assert ( set( df.index.get_level_values( 'rootid' ).unique().values )
             == { roots[0]['root'].id, roots[2]['root'].id } )
    assert len( srcs ) == 30
    assert len( srcs.xs( roots[0]['root'].id, level='rootid' ) ) == 13
    assert len( srcs.xs( roots[2]['root'].id, level='rootid' ) ) == 17
    assert len( forced ) == 54
    assert len( forced.xs( roots[0]['root'].id, level='rootid' ) ) == 25
    assert len( forced.xs( roots[2]['root'].id, level='rootid' ) ) == 29
    assert len( df ) == 54
    assert len( df.xs( roots[0]['root'].id, level='rootid' ) ) == 25
    assert len( df.xs( roots[2]['root'].id, level='rootid' ) ) == 29
    tmp = df.drop( 'ispatch', axis='columns' )
    assert ( tmp == forced ).all().all()

    # Make sure the dict returns are consistent
    srcsjs = ltcv.many_object_ltcvs( pvs['pv2'].id, [ roots[i]['root'].id for i in [0,2] ],
                                     return_format='json', which='detections', include_base_procver=True )
    forcedjs = ltcv.many_object_ltcvs( pvs['pv2'].id, [ roots[i]['root'].id for i in [0,2] ],
                                       return_format='json', which='forced', include_base_procver=True )
    dfjs = ltcv.many_object_ltcvs( pvs['pv2'].id, [ roots[i]['root'].id for i in [0,2] ],
                                   return_format='json', which='patch', include_base_procver=True )
    assert set( [ r['rootid'] for r in srcsjs ] ) == { roots[0]['root'].id, roots[2]['root'].id }
    assert set( [ r['rootid'] for r in forcedjs ] ) == { roots[0]['root'].id, roots[2]['root'].id }
    assert set( [ r['rootid'] for r in dfjs ] ) == { roots[0]['root'].id, roots[2]['root'].id }
    for js, pd in zip( [ srcsjs, forcedjs, dfjs ], [ srcs, forced, df ] ):
        for subjs in js:
            subpd = pd.xs( subjs['rootid'], level='rootid' ).reset_index()
            for col in subpd.columns:
                assert ( subpd[col] == np.array( subjs[col] ) ).all()

    # Make sure the lightcurves are actually right
    for rootid in df.index.get_level_values( 'rootid' ).unique():
        rdex = [ roots[i]['root'].id for i in range(len(roots)) ].index( rootid )
        tmpsrcs = srcs.xs( rootid, level='rootid' ).reset_index()
        tmpforced = forced.xs( rootid, level='rootid' ).reset_index()
        tmpdf = df.xs( rootid, level='rootid' ).reset_index()

        assert all( s.mjd == pytest.approx( roots[rdex]['objs'][1]['src']['bpv2'][j].midpointmjdtai, abs=1./3600./24. )
                    for j, s in enumerate( tmpsrcs.itertuples() ) )
        assert all( s.band == roots[rdex]['objs'][1]['src']['bpv2'][j].band
                    for j, s in enumerate( tmpsrcs.itertuples() ) )
        assert all( s.flux == pytest.approx( roots[rdex]['objs'][1]['src']['bpv2'][j].psfflux, rel=1e-6 )
                    for j, s in enumerate( tmpsrcs.itertuples() ) )
        assert all( s.fluxerr == pytest.approx( roots[rdex]['objs'][1]['src']['bpv2'][j].psffluxerr, rel=1e-6 )
                    for j, s in enumerate( tmpsrcs.itertuples() ) )
        assert all( tmpsrcs.isdet )
        assert all( tmpsrcs.base_procver == 'pvc_bpv2a' )

        assert all( s.mjd == pytest.approx( roots[rdex]['objs'][1]['frc']['bpv2'][j].midpointmjdtai, abs=1./3600./24. )
                    for j, s in enumerate( tmpforced.itertuples() ) )
        assert all( s.band == roots[rdex]['objs'][1]['frc']['bpv2'][j].band
                    for j, s in enumerate( tmpforced.itertuples() ) )
        assert all( s.flux == pytest.approx( roots[rdex]['objs'][1]['frc']['bpv2'][j].psfflux, rel=1e-6 )
                    for j, s in enumerate( tmpforced.itertuples() ) )
        assert all( s.fluxerr == pytest.approx( roots[rdex]['objs'][1]['frc']['bpv2'][j].psffluxerr, rel=1e-6 )
                    for j, s in enumerate( tmpforced.itertuples() ) )
        assert all( tmpforced.isdet[j] if ( tmpforced.mjd[j] >= tmpsrcs.mjd.min() and
                                            tmpforced.mjd[j] <= tmpsrcs.mjd.max() )
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
        assert all( tmpdf.isdet[j] if ( tmpdf.mjd[j] >= tmpsrcs.mjd.min() and
                                        tmpdf.mjd[j] <= tmpsrcs.mjd.max() )
                    else not tmpdf.isdet[j]
                    for j in range( len(tmpdf) ) )
        assert all( ~tmpdf.ispatch )
        assert all( tmpdf.base_procver == 'pvc_bpv2a' )


    # make sure that if we ask using the right diaobjectid, we get the same things back
    srcs2 = ltcv.many_object_ltcvs( pvs['pv2'].id, [ roots[i]['objs'][1]['obj'].diaobjectid for i in [0,2] ],
                                    return_format='pandas', which='detections', include_base_procver=True )
    forced2 = ltcv.many_object_ltcvs( pvs['pv2'].id, [ roots[i]['objs'][1]['obj'].diaobjectid for i in [0,2] ],
                                      return_format='pandas', which='forced', include_base_procver=True )
    df2 = ltcv.many_object_ltcvs( pvs['pv2'].id, [ roots[i]['objs'][1]['obj'].diaobjectid for i in [0,2] ],
                                  return_format='pandas', which='patch', include_base_procver=True )
    assert ( srcs == srcs2 ).all().all()
    assert ( forced == forced2 ).all().all()
    assert ( df == df2 ).all().all()

    # But we get nothing back if we ask for the wrong diaobjectid
    srcs2 = ltcv.many_object_ltcvs( pvs['pv2'].id, [ roots[i]['objs'][0]['obj'].diaobjectid for i in [0,2] ],
                                    return_format='pandas', which='detections', include_base_procver=True )
    forced2 = ltcv.many_object_ltcvs( pvs['pv2'].id, [ roots[i]['objs'][0]['obj'].diaobjectid for i in [0,2] ],
                                      return_format='pandas', which='forced', include_base_procver=True )
    df2 = ltcv.many_object_ltcvs( pvs['pv2'].id, [ roots[i]['objs'][0]['obj'].diaobjectid for i in [0,2] ],
                                  return_format='pandas', which='patch', include_base_procver=True )
    assert len(srcs2) == 0
    assert len(forced2) == 0
    assert len(df2) == 0

    # test mjd_now
    assert not ( srcs.index.get_level_values( level='mjd' ) <= 60041. ).all()
    assert not ( forced.index.get_level_values( level='mjd' ) <= 60041. ).all()
    assert not ( df.index.get_level_values( level='mjd' ) <= 60041. ).all()
    srcs = ltcv.many_object_ltcvs( pvs['pv2'].id, [ roots[i]['root'].id for i in [0,2] ],
                                   return_format='pandas', which='detections', include_base_procver=True,
                                   mjd_now=60041. )
    forced = ltcv.many_object_ltcvs( pvs['pv2'].id, [ roots[i]['root'].id for i in [0,2] ],
                                     return_format='pandas', which='forced', include_base_procver=True,
                                     mjd_now=60041. )
    df = ltcv.many_object_ltcvs( pvs['pv2'].id, [ roots[i]['root'].id for i in [0,2] ],
                                 return_format='pandas', which='patch', include_base_procver=True,
                                 mjd_now=60041. )
    assert ( srcs.index.get_level_values( level='mjd' ) <= 60041. ).all()
    assert ( forced.index.get_level_values( level='mjd' ) <= 60041. ).all()
    assert ( df.index.get_level_values( level='mjd' ) <= 60041. ).all()


    # Make sure bands works
    srcs = ltcv.many_object_ltcvs( pvs['pv2'].id, [ roots[i]['root'].id for i in [0,2] ],
                                   return_format='pandas', which='detections', include_base_procver=True,
                                   bands='r' )
    forced = ltcv.many_object_ltcvs( pvs['pv2'].id, [ roots[i]['root'].id for i in [0,2] ],
                                     return_format='pandas', which='forced', include_base_procver=True,
                                     bands=[ 'r' ] )
    df = ltcv.many_object_ltcvs( pvs['pv2'].id, [ roots[i]['root'].id for i in [0,2] ],
                                 return_format='pandas', which='patch', include_base_procver=True,
                                 bands='r' )
    assert len(srcs) == 14
    assert len(forced) == 26
    assert len(df) == 26
    assert ( srcs.band == 'r' ).all()
    assert ( forced.band == 'r' ).all()
    assert ( df.band == 'r' ).all()
    tmp = df.drop( 'ispatch', axis='columns' )
    assert ( tmp == forced ).all().all()

    # Test include_source_positions

    srcs = ltcv.many_object_ltcvs( pvs['pv1'].id, [ roots[i]['root'].id for i in [0,1] ],
                                   return_format='pandas', which='detections',
                                   include_base_procver=True, include_source_positions=True )
    forced = ltcv.many_object_ltcvs( pvs['pv1'].id, [ roots[i]['root'].id for i in [0,1] ],
                                     return_format='pandas', which='forced',
                                     include_base_procver=True, include_source_positions=True )
    df = ltcv.many_object_ltcvs( pvs['pv1'].id, [ roots[i]['root'].id for i in [0,1] ],
                                 return_format='pandas', which='patch',
                                 include_base_procver=True, include_source_positions=True )
    for field in [ 'det_ra', 'det_dec' ]:
        assert not any( srcs[field].isna() )
        assert not any( forced[ forced.isdet == True ][field].isna() )
        assert all( forced[ forced.isdet == False ][field].isna() )
        assert not any( df[ df.isdet == True ][field].isna() )
        assert all( df[ df.isdet == False ][field].isna() )


# There is another test of ltcv_object_search that uses loaded SNANA data
#   in test_ltcv_object_search.py
def test_object_search( set_of_lightcurves ):
    roots = set_of_lightcurves
    with pytest.raises( ValueError, match="Unknown search keywords: {'foo'}" ):
        _ = ltcv.object_search( processing_version='pvc_pv3', return_format='pandas', foo='bar' )

    # TODO -- tests without ignore_object_processing_version

    # Search on ra/dec

    # A 17" search around (42,13) should find diaobjectids 200 and 201
    df = ltcv.object_search( processing_version='pvc_pv3', return_format='pandas',
                             ra=42., dec=13., radius=17., ignore_object_processing_version=True )
    assert len(df) == 2
    assert set( df.rootid ) == { roots[0]['root'].id, roots[1]['root'].id }
    assert df[ df.rootid==roots[0]['root'].id ].ra.values[0] == pytest.approx( 42., abs=0.2/3600. )
    assert df[ df.rootid==roots[0]['root'].id ].dec.values[0] == pytest.approx( 13., abs=0.2/3600. )
    assert df[ df.rootid==roots[0]['root'].id ].numdet.values[0] == 13
    assert df[ df.rootid==roots[0]['root'].id ].firstdetmjd.values[0] == 60000.
    assert df[ df.rootid==roots[0]['root'].id ].firstdetband.values[0] == 'i'
    assert df[ df.rootid==roots[0]['root'].id ].lastdetmjd.values[0] == 60030.
    assert df[ df.rootid==roots[0]['root'].id ].lastdetband.values[0] == 'i'
    assert df[ df.rootid==roots[0]['root'].id ].maxdetmjd.values[0] == 60010.
    assert df[ df.rootid==roots[0]['root'].id ].maxdetband.values[0] == 'i'
    assert df[ df.rootid==roots[0]['root'].id ].lastforcedmjd.values[0] == 60050.
    assert df[ df.rootid==roots[0]['root'].id ].lastforcedband.values[0] == 'i'
    assert df[ df.rootid==roots[0]['root'].id ].numdetinwindow.values[0] is None
    assert df[ df.rootid==roots[1]['root'].id ].ra.values[0] == pytest.approx( 42., abs=0.2/3600. )
    assert df[ df.rootid==roots[1]['root'].id ].dec.values[0] == pytest.approx( 13.0036, abs=0.2/3600. )
    assert df[ df.rootid==roots[1]['root'].id ].numdet.values[0] == 17
    assert df[ df.rootid==roots[1]['root'].id ].firstdetmjd.values[0] == 60020.
    assert df[ df.rootid==roots[1]['root'].id ].firstdetband.values[0] == 'r'
    assert df[ df.rootid==roots[1]['root'].id ].lastdetmjd.values[0] == 60060.
    assert df[ df.rootid==roots[1]['root'].id ].lastdetband.values[0] == 'r'
    assert df[ df.rootid==roots[1]['root'].id ].maxdetmjd.values[0] == 60035.
    assert df[ df.rootid==roots[1]['root'].id ].maxdetband.values[0] == 'r'
    assert df[ df.rootid==roots[1]['root'].id ].lastforcedmjd.values[0] == 60080.
    assert df[ df.rootid==roots[1]['root'].id ].lastforcedband.values[0] == 'r'
    assert df[ df.rootid==roots[1]['root'].id ].numdetinwindow.values[0] is None

    # Check that json return works
    j = ltcv.object_search( processing_version='pvc_pv3', return_format='json',
                            ra=42., dec=13., radius=17., ignore_object_processing_version=True  )
    for col in df.columns:
        # == isn't working in the None case... ?  Weird.  Different kind of None?
        if col == 'numdetinwindow':
            assert all( n is None for n in j[col] )
            assert all( n is None for n in df[col].values )
        else:
            assert ( df[col] == np.array( j[col] ) ).all()

    # Quick check that just_objids works
    df = ltcv.object_search( processing_version='pvc_pv3', return_format='pandas',
                             ra=42., dec=13., radius=17., just_objids=True, ignore_object_processing_version=True  )
    assert df.columns == [ 'rootid' ]
    assert set( df.rootid ) == { roots[0]['root'].id, roots[1]['root'].id }
    j = ltcv.object_search( processing_version='pvc_pv3', return_format='json',
                            ra=42., dec=13., radius=17., just_objids=True, ignore_object_processing_version=True  )
    assert list( j.keys() ) == [ 'rootid' ]
    assert set( j['rootid'] ) == { roots[0]['root'].id, roots[1]['root'].id }

    # Quick check that noforced works.  Well, sort of.  It tests that we don't
    #   get the forced columns back, but doesn't test that the function actually
    #   skipped searching that table.  But, whatevs.
    df = ltcv.object_search( processing_version='pvc_pv3', return_format='pandas',
                             ra=42., dec=13., radius=17., noforced=True, ignore_object_processing_version=True  )
    assert set( df.rootid ) == { roots[0]['root'].id, roots[1]['root'].id }
    assert all( i not in df.columns for i in [ 'lastforcedmjd', 'lastforcedband',
                                               'lastforcedflux', 'lastforcedfluxerr' ] )
    j = ltcv.object_search( processing_version='pvc_pv3', return_format='json',
                            ra=42., dec=13., radius=17., noforced=True, ignore_object_processing_version=True  )
    assert set( j['rootid'] ) == { roots[0]['root'].id, roots[1]['root'].id }
    assert all( i not in j.keys() for i in [ 'lastforcedmjd', 'lastforcedband',
                                             'lastforcedflux', 'lastforcedfluxerr' ] )

    # Quick bigger search; should get 3 of the 4 objects, ginormous search all of them
    df = ltcv.object_search( processing_version='pvc_pv3', return_format='pandas',
                             ra=42., dec=13., radius=1800., ignore_object_processing_version=True  )
    assert len(df) == 3
    assert set( df.rootid ) == set( roots[i]['root'].id for i in range(3) )
    df = ltcv.object_search( processing_version='pvc_pv3', return_format='pandas',
                             ra=42., dec=13., radius=7200., ignore_object_processing_version=True  )
    assert len(df) == 4
    assert set( df.rootid ) == set( roots[i]['root'].id for i in range(4) )

    # If we ask for pvc_pv1, we should only find diaobject 200, and it should have different latest thingies
    df = ltcv.object_search( processing_version='pvc_pv1', return_format='pandas',
                             ra=42., dec=13., radius=7200., ignore_object_processing_version=True  )
    assert len(df) == 1
    assert set( df.rootid ) == { roots[0]['root'].id }
    assert df.ra.values[0] == pytest.approx( 42., abs=0.2/3600. )
    assert df.dec.values[0] == pytest.approx( 13., abs=0.4/3600. ) # , abs=0.2/3600. )
    assert df.numdet.values[0] == 13
    assert df.lastforcedmjd.values[0] == 60025.

    # Try an earlier mjd_now
    df = ltcv.object_search( processing_version='pvc_pv3', return_format='pandas',
                             ra=42., dec=13., radius=17., mjd_now=60026., ignore_object_processing_version=True  )
    assert len(df) == 2
    assert set( df.rootid ) == { roots[0]['root'].id, roots[1]['root'].id }
    assert df[ df.rootid==roots[0]['root'].id ].ra.values[0] == pytest.approx( 42., abs=0.2/3600. )
    assert df[ df.rootid==roots[0]['root'].id ].dec.values[0] == pytest.approx( 13., abs=0.2/3600. )
    assert df[ df.rootid==roots[0]['root'].id ].numdet.values[0] == 11
    assert df[ df.rootid==roots[0]['root'].id ].firstdetmjd.values[0] == 60000.
    assert df[ df.rootid==roots[0]['root'].id ].firstdetband.values[0] == 'i'
    assert df[ df.rootid==roots[0]['root'].id ].lastdetmjd.values[0] == 60025.
    assert df[ df.rootid==roots[0]['root'].id ].lastdetband.values[0] == 'i'
    assert df[ df.rootid==roots[0]['root'].id ].maxdetmjd.values[0] == 60010.
    assert df[ df.rootid==roots[0]['root'].id ].maxdetband.values[0] == 'i'
    assert df[ df.rootid==roots[0]['root'].id ].lastforcedmjd.values[0] == 60025.
    assert df[ df.rootid==roots[0]['root'].id ].lastforcedband.values[0] == 'i'
    assert df[ df.rootid==roots[0]['root'].id ].numdetinwindow.values[0] is None
    assert df[ df.rootid==roots[1]['root'].id ].ra.values[0] == pytest.approx( 42., abs=0.2/3600. )
    assert df[ df.rootid==roots[1]['root'].id ].dec.values[0] == pytest.approx( 13.0036, abs=0.2/3600. )
    assert df[ df.rootid==roots[1]['root'].id ].numdet.values[0] == 3
    assert df[ df.rootid==roots[1]['root'].id ].firstdetmjd.values[0] == 60020.
    assert df[ df.rootid==roots[1]['root'].id ].firstdetband.values[0] == 'r'
    assert df[ df.rootid==roots[1]['root'].id ].lastdetmjd.values[0] == 60025.
    assert df[ df.rootid==roots[1]['root'].id ].lastdetband.values[0] == 'r'
    assert df[ df.rootid==roots[1]['root'].id ].maxdetmjd.values[0] == 60025.
    assert df[ df.rootid==roots[1]['root'].id ].maxdetband.values[0] == 'r'
    assert df[ df.rootid==roots[1]['root'].id ].lastforcedmjd.values[0] == 60025.
    assert df[ df.rootid==roots[1]['root'].id ].lastforcedband.values[0] == 'r'

    # Throw in a window
    df = ltcv.object_search( processing_version='pvc_pv3', return_format='pandas',
                             ra=42., dec=13., radius=17., window_t0=60010, window_t1=60025,
                             ignore_object_processing_version=True  )
    assert len(df) == 2
    assert set( df.rootid ) == { roots[0]['root'].id, roots[1]['root'].id }
    assert df[ df.rootid==roots[0]['root'].id ].numdetinwindow.values[0] == 7
    assert df[ df.rootid==roots[1]['root'].id ].numdetinwindow.values[0] == 3
    # All of the following is cut and paste from the first search above
    assert df[ df.rootid==roots[0]['root'].id ].ra.values[0] == pytest.approx( 42., abs=0.2/3600. )
    assert df[ df.rootid==roots[0]['root'].id ].dec.values[0] == pytest.approx( 13., abs=0.2/3600. )
    assert df[ df.rootid==roots[0]['root'].id ].numdet.values[0] == 13
    assert df[ df.rootid==roots[0]['root'].id ].firstdetmjd.values[0] == 60000.
    assert df[ df.rootid==roots[0]['root'].id ].firstdetband.values[0] == 'i'
    assert df[ df.rootid==roots[0]['root'].id ].lastdetmjd.values[0] == 60030.
    assert df[ df.rootid==roots[0]['root'].id ].lastdetband.values[0] == 'i'
    assert df[ df.rootid==roots[0]['root'].id ].maxdetmjd.values[0] == 60010.
    assert df[ df.rootid==roots[0]['root'].id ].maxdetband.values[0] == 'i'
    assert df[ df.rootid==roots[0]['root'].id ].lastforcedmjd.values[0] == 60050.
    assert df[ df.rootid==roots[0]['root'].id ].lastforcedband.values[0] == 'i'
    assert df[ df.rootid==roots[1]['root'].id ].ra.values[0] == pytest.approx( 42., abs=0.2/3600. )
    assert df[ df.rootid==roots[1]['root'].id ].dec.values[0] == pytest.approx( 13.0036, abs=0.2/3600. )
    assert df[ df.rootid==roots[1]['root'].id ].numdet.values[0] == 17
    assert df[ df.rootid==roots[1]['root'].id ].firstdetmjd.values[0] == 60020.
    assert df[ df.rootid==roots[1]['root'].id ].firstdetband.values[0] == 'r'
    assert df[ df.rootid==roots[1]['root'].id ].lastdetmjd.values[0] == 60060.
    assert df[ df.rootid==roots[1]['root'].id ].lastdetband.values[0] == 'r'
    assert df[ df.rootid==roots[1]['root'].id ].maxdetmjd.values[0] == 60035.
    assert df[ df.rootid==roots[1]['root'].id ].maxdetband.values[0] == 'r'
    assert df[ df.rootid==roots[1]['root'].id ].lastforcedmjd.values[0] == 60080.
    assert df[ df.rootid==roots[1]['root'].id ].lastforcedband.values[0] == 'r'

    # Now filter on numdetinwindow
    df = ltcv.object_search( processing_version='pvc_pv3', return_format='pandas',
                             ra=42., dec=13., radius=17., window_t0=60010, window_t1=60025,
                             min_window_numdetections=4, ignore_object_processing_version=True  )
    assert len(df) == 1
    assert set( df.rootid ) == { roots[0]['root'].id }
    df = ltcv.object_search( processing_version='pvc_pv3', return_format='pandas',
                             ra=42., dec=13., radius=17., window_t0=60010, window_t1=60025,
                             max_window_numdetections=5, ignore_object_processing_version=True  )
    assert len(df) == 1
    assert set( df.rootid ) == { roots[1]['root'].id }
    df = ltcv.object_search( processing_version='pvc_pv3', return_format='pandas',
                             ra=42., dec=13., radius=17., window_t0=60010, window_t1=60025,
                             min_window_numdetections=4, max_window_numdetections=5,
                             ignore_object_processing_version=True  )
    assert len(df) == 0


    # Filter on first detection
    df = ltcv.object_search( processing_version='pvc_pv3', return_format='pandas',
                             mint_firstdetection=60010, ignore_object_processing_version=True )
    assert len(df) == 3
    assert set( df.rootid ) == set( roots[i]['root'].id for i in [ 1, 2, 3 ] )
    df = ltcv.object_search( processing_version='pvc_pv3', return_format='pandas',
                             maxt_firstdetection=60030, ignore_object_processing_version=True )
    assert len(df) == 2
    assert set( df.rootid ) == set( roots[i]['root'].id for i in [ 0, 1 ] )
    df = ltcv.object_search( processing_version='pvc_pv3', return_format='pandas',
                             mint_firstdetection=60010, maxt_firstdetection=60030,
                             ignore_object_processing_version=True )
    assert len(df) == 1
    assert set( df.rootid ) == { roots[1]['root'].id }

    df = ltcv.object_search( processing_version='pvc_pv3', return_format='pandas',
                             minmag_firstdetection=25.6, ignore_object_processing_version=True )
    assert len(df) == 2
    assert set( df.rootid ) == set( roots[i]['root'].id for i in [ 0, 3 ] )
    df = ltcv.object_search( processing_version='pvc_pv3', return_format='pandas',
                             maxmag_firstdetection=25.9, ignore_object_processing_version=True )
    assert len(df) == 3
    assert set( df.rootid ) == set( roots[i]['root'].id for i in [ 1, 2, 3 ] )
    df = ltcv.object_search( processing_version='pvc_pv3', return_format='pandas',
                             minmag_firstdetection=25.6, maxmag_firstdetection=25.9,
                             ignore_object_processing_version=True )
    assert len(df) == 1
    assert set( df.rootid ) == { roots[3]['root'].id }

    # Filter on last detection
    df = ltcv.object_search( processing_version='pvc_pv3', return_format='pandas',
                             mint_lastdetection=60035, ignore_object_processing_version=True )
    assert len( df ) == 3
    assert set( df.rootid ) == { roots[i]['root'].id for i in [ 1, 2, 3 ] }
    df = ltcv.object_search( processing_version='pvc_pv3', return_format='pandas',
                             maxt_lastdetection=60065, ignore_object_processing_version=True )
    assert len( df ) == 3
    assert set( df.rootid ) == { roots[i]['root'].id for i in [ 0, 1, 3] }
    df = ltcv.object_search( processing_version='pvc_pv3', return_format='pandas',
                             mint_lastdetection=60035, maxt_lastdetection=60065,
                             ignore_object_processing_version=True  )
    assert len( df ) == 2
    assert set( df.rootid ) == { roots[i]['root'].id for i in [ 1, 3 ] }

    df = ltcv.object_search( processing_version='pvc_pv3', return_format='pandas',
                             minmag_lastdetection=25.5, ignore_object_processing_version=True )
    assert len(df) == 3
    assert set( df.rootid ) == { roots[i]['root'].id for i in [ 0, 2, 3 ] }
    df = ltcv.object_search( processing_version='pvc_pv3', return_format='pandas',
                             maxmag_lastdetection=25.8, ignore_object_processing_version=True )
    assert len(df) == 2
    assert set( df.rootid ) == { roots[i]['root'].id for i in [ 1, 2 ] }
    df = ltcv.object_search( processing_version='pvc_pv3', return_format='pandas',
                             minmag_lastdetection=25.5, maxmag_lastdetection=25.8,
                             ignore_object_processing_version=True )
    assert len(df) == 1
    assert set( df.rootid ) == { roots[2]['root'].id }

    # Filter on max detection
    df = ltcv.object_search( processing_version='pvc_pv3', return_format='pandas',
                             mint_maxdetection=60020, ignore_object_processing_version=True )
    assert len(df) == 3
    assert set( df.rootid ) == { roots[i]['root'].id for i in [ 1, 2, 3 ] }
    df = ltcv.object_search( processing_version='pvc_pv3', return_format='pandas',
                             maxt_maxdetection=60052, ignore_object_processing_version=True )
    assert len(df) == 3
    assert set( df.rootid ) == { roots[i]['root'].id for i in [ 0, 1, 2 ] }
    df = ltcv.object_search( processing_version='pvc_pv3', return_format='pandas',
                             mint_maxdetection=60020, maxt_maxdetection=60052,
                             ignore_object_processing_version=True )
    assert len(df) == 2
    assert set( df.rootid ) == { roots[i]['root'].id for i in [ 1, 2 ] }

    df = ltcv.object_search( processing_version='pvc_pv3', return_format='pandas',
                             minmag_maxdetection=22.5, ignore_object_processing_version=True )
    assert len(df) == 3
    assert set( df.rootid ) == { roots[i]['root'].id for i in [ 0, 2, 3 ] }
    df = ltcv.object_search( processing_version='pvc_pv3', return_format='pandas',
                             maxmag_maxdetection=24.5, ignore_object_processing_version=True )
    assert len(df) == 3
    assert set( df.rootid ) == { roots[i]['root'].id for i in [ 0,  1, 2 ] }
    df = ltcv.object_search( processing_version='pvc_pv3', return_format='pandas',
                             minmag_maxdetection=22.5, maxmag_maxdetection=24.5,
                             ignore_object_processing_version=True )
    assert len(df) == 2
    assert set( df.rootid ) == { roots[i]['root'].id for i in [ 0, 2 ] }

    # To filter on lastmag, we need to use mjd_now, because the fixture filled out
    #   forced photometry all down to mag 32.
    df = ltcv.object_search( processing_version='pvc_pv3', return_format='pandas', mjd_now=60056.,
                             ignore_object_processing_version=True )
    df.set_index( 'rootid', inplace=True )
    dexen = np.array( roots[i]['root'].id for i in range(4) )
    assert list( df.loc[ dexen, 'lastforcedmjd' ] ) == [ 60050., 60055., 60055., 60055. ]
    df = ltcv.object_search( processing_version='pvc_pv3', return_format='pandas', mjd_now=60056.,
                             min_lastmag=24., ignore_object_processing_version=True )
    assert len(df) == 3
    assert set( df.rootid ) == { roots[i]['root'].id for i in [ 0, 1, 3 ] }
    df = ltcv.object_search( processing_version='pvc_pv3', return_format='pandas', mjd_now=60056.,
                             max_lastmag=24.8, ignore_object_processing_version=True )
    assert len(df) == 2
    assert set( df.rootid ) == { roots[i]['root'].id for i in [ 1, 2 ] }
    df = ltcv.object_search( processing_version='pvc_pv3', return_format='pandas', mjd_now=60056.,
                             min_lastmag= 24., max_lastmag=24.8, ignore_object_processing_version=True )
    assert len(df) == 1
    assert set( df.rootid ) == { roots[1]['root'].id }


    # TODO : test statbands.  (It is tested in test_ltcv_object_search.py)
    # (...and a good thing too, becasue it was broken.)


def test_get_hot_ltcvs( set_of_lightcurves ):
    # ...not sure how to test this without mjd_now since it uses the current time,
    #    and that will be different based on when this is run

    roots = set_of_lightcurves

    df, objdf, _ = ltcv.get_hot_ltcvs( 'pvc_pv2', detected_since_mjd=60035, mjd_now=60056 )
    assert set( df.index.names ) == { 'rootid', 'mjd' }
    assert set( df.columns ) == { 'diaobjectid', 'diasourceid', 'diaforcedsourceid',
                                  'visit', 'band', 'flux', 'fluxerr', 'isdet' }
    assert set( df.index.get_level_values('rootid') ) == { roots[i]['root'].id for i in [ 1, 2, 3 ] }
    assert set( objdf.rootid ) == set( df.index.get_level_values('rootid') )

    df, objdf, _ = ltcv.get_hot_ltcvs( 'pvc_pv2', detected_since_mjd=60035, mjd_now=60046 )
    assert set( df.index.get_level_values('rootid') ) == { roots[i]['root'].id for i in [ 1, 2 ] }
    assert set( objdf.rootid ) == set( df.index.get_level_values('rootid') )

    df, objdf, _ = ltcv.get_hot_ltcvs( 'pvc_pv2', detected_in_last_days=2, mjd_now=60021 )
    assert set( df.index.get_level_values('rootid') ) == { roots[i]['root'].id for i in [ 0, 1 ] }
    assert set( objdf.rootid ) == set( df.index.get_level_values('rootid') )

    df, objdf, _ = ltcv.get_hot_ltcvs( 'pvc_pv2', detected_in_last_days=2, mjd_now=60041 )
    assert set( df.index.get_level_values('rootid') ) == { roots[i]['root'].id for i in [ 1, 2 ] }
    assert set( objdf.rootid ) == set( df.index.get_level_values('rootid') )

    # detected_in_last_days defaults to 30
    df, objdf, _ = ltcv.get_hot_ltcvs( 'pvc_pv2', mjd_now=60085 )
    assert set( df.index.get_level_values('rootid') ) == { roots[i]['root'].id for i in [ 1, 2, 3 ] }
    assert set( objdf.rootid ) == set( df.index.get_level_values('rootid') )
    df, objdf, _ = ltcv.get_hot_ltcvs( 'pvc_pv2', mjd_now=60095 )
    assert set( df.index.get_level_values('rootid') ) == { roots[2]['root'].id }
    assert set( objdf.rootid ) == set( df.index.get_level_values('rootid') )

    # Now let's look a the pulled forced photometry
    df, _, _ = ltcv.get_hot_ltcvs( 'pvc_pv2', detected_in_last_days=2, mjd_now=60041 )
    assert ( df.index.get_level_values('mjd') <= 60041. ).all()
    assert ( df.reset_index().groupby( 'rootid' ).max().mjd == 60040. ).all()
    assert len( df.xs( roots[1]['root'].id, level='rootid' ) ) == 13
    assert len( df.xs( roots[2]['root'].id, level='rootid' ) ) == 5
    # ...should I look at the actual values?  I should.

    # Test source patch.  Gotta use pvc_pv1 for this.  (I guess we coulda also used realtime.)

    df, objdf, _ = ltcv.get_hot_ltcvs( 'pvc_pv1', mjd_now=60031 )
    assert set( df.index.get_level_values('rootid') ) == { roots[0]['root'].id }
    assert set( objdf.rootid ) == set( df.index.get_level_values('rootid') )
    assert df.index.get_level_values('mjd').max() == 60025.

    df2, objdf2, _ = ltcv.get_hot_ltcvs( 'pvc_pv1', mjd_now=60031, source_patch=True )
    assert set( df2.columns ) == { 'diaobjectid', 'diasourceid', 'diaforcedsourceid',
                                   'visit', 'band', 'flux', 'fluxerr', 'isdet', 'ispatch' }
    assert set( df2.index.get_level_values('rootid') ) == { roots[0]['root'].id }
    assert set( objdf2.rootid ) == set( df2.index.get_level_values('rootid') )
    assert df2.index.get_level_values('mjd').max() == 60030.
    assert len(df2) == len(df) + 2
    assert ( df == df2.loc[ df2.index.get_level_values('mjd') <= 60025., df2.columns != 'ispatch' ] ).all().all()
    assert df2[ df2.index.get_level_values('mjd') > 60025. ].isdet.all()
    assert not df2[ df2.index.get_level_values('mjd') <= 20025. ].ispatch.any()
    assert df2[ df2.index.get_level_values('mjd') > 60025. ].ispatch.all()

    # Test using weighted positions.
    # First, a baseline redo of the first
    #   set of hot ltcvs we tested above, make sure that the positions are
    #   coming out as expected.
    # NOTE that the mjd_now stuff is not working right for position times!  It just
    #   sorts on processing versions, and there will be processing times saved
    #   later than 60056 for some of these sources.
    # positions are saved for [ 6000, 60030, 60050, 60060, 60080 ] (procver_postimes fixture)
    # 201 is detected trhough 60060, so will have 60060 as its position time
    # 202 is detected through 60080, so will have 60080 as its position time
    # 203 is detected through 60060, so will have 60006 as its position time
    postime_procver_60060 = db.BaseProcessingVersion.base_procver_id( 'pvc_bpv2a_60060', 'diaobject_position' )
    postime_procver_60080 = db.BaseProcessingVersion.base_procver_id( 'pvc_bpv2a_60080', 'diaobject_position' )

    df, objdf, _ = ltcv.get_hot_ltcvs( 'pvc_pv2', detected_since_mjd=60035, mjd_now=60056 )
    assert set( df.index.names ) == { 'rootid', 'mjd' }
    assert set( df.columns ) == { 'diaobjectid', 'diasourceid', 'diaforcedsourceid',
                                  'visit', 'band', 'flux', 'fluxerr', 'isdet' }
    assert set( df.index.get_level_values('rootid') ) == { roots[i]['root'].id for i in [ 1, 2, 3 ] }
    assert set( objdf.rootid ) == set( df.index.get_level_values('rootid') )
    assert objdf.loc[roots[1]['objs'][1]['obj'].diaobjectid, 'pos_base_procver_id'] == postime_procver_60060
    assert objdf.loc[roots[2]['objs'][1]['obj'].diaobjectid, 'pos_base_procver_id'] == postime_procver_60080
    assert objdf.loc[roots[3]['objs'][1]['obj'].diaobjectid, 'pos_base_procver_id'] == postime_procver_60060

    weightdf, weightobjdf, _ = ltcv.get_hot_ltcvs( 'pvc_pv2', detected_since_mjd=60035, mjd_now=60056,
                                                   always_use_weighted_source_positions=True )
    # The expected weighted postition will be from all detections before 60056.
    # Dig through the data that was generated (in roots) and compare to what we
    #   got back from the api.
    for dex in range(1, 4):
        rootid = roots[dex]['root'].id
        diaobjectid = roots[dex]['objs'][1]['obj'].diaobjectid

        # ...first, sanity check to make sure I'm indexing everything right.
        # doubles have 53 mantissa bits, and log10(2^53) = 15, so as long
        # as things stayed doubles everywhere, floating point roundoff
        # should still be good to 14 digits.
        assert all( weightdf.loc[ ( rootid, i.midpointmjdtai ), 'det_ra' ] == pytest.approx( i.ra, rel=1e-14 )
                    for i in roots[dex]['objs'][1]['src']['bpv2a']
                    if i.midpointmjdtai <= 60056 )
        assert all( weightdf.loc[ ( rootid, i.midpointmjdtai ), 'det_dec' ] == pytest.approx( i.dec, rel=1e-14 )
                    for i in roots[dex]['objs'][1]['src']['bpv2a']
                    if i.midpointmjdtai < 60056 )
        # (The roots data structure feels unnecessarily complicated... but maybe it is necessary given
        #  all this processing version shenanigans.)

        # bpv2a should take priority over bpv2
        # (...though it turns out in this case that everthing is in bpv2a,
        #  so we're not really testing the fallback....)
        srcs = { i.diasourceid: ( i.ra, i.dec, i.psfflux, i.psffluxerr, 'bpv2' )
                 for i in roots[dex]['objs'][1]['src']['bpv2']
                 if i.midpointmjdtai <= 60056 }
        srcs.update( { i.diasourceid: ( i.ra, i.dec, i.psfflux, i.psffluxerr, 'bpv2a' )
                       for i in roots[dex]['objs'][1]['src']['bpv2a']
                       if i.midpointmjdtai <= 60056 } )
        srcs = [ i for i in srcs.values() if ( i[2] / i[3] ) > 3 ]

        if len(srcs) == 0:
            assert weightobjdf.loc[ diaobjectid, 'ra' ] is None
            assert weightobjdf.loc[ diaobjectid, 'dec' ] is None
            assert objdf.loc[ diaobjectid, 'ra' ] is not None
            assert objdf.loc[ diaobjectid, 'dec' ] is not None
        else:
            ras = np.array( [ i[0] for i in srcs ] )
            decs = np.array( [ i[1] for i in srcs ] )
            weights = np.array( [ (i[2] / i[3])**2 for i in srcs ] )
            expectedra = ( ras * weights ).sum() / weights.sum()
            expecteddec = ( decs * weights ).sum() / weights.sum()

            assert weightobjdf.loc[ diaobjectid, 'ra' ] == pytest.approx( expectedra, abs=0.001/3600. )
            assert weightobjdf.loc[ diaobjectid, 'dec' ] == pytest.approx( expecteddec, abs=0.001/3600. )
            # ...and this weighted average position should not be the same as what we got when we used
            #   the diaobject_position values above
            assert ( weightobjdf.loc[ diaobjectid, 'ra' ] !=
                     pytest.approx( objdf.loc[ diaobjectid, 'ra' ], abs=0.001/3600. ) )
            assert ( weightobjdf.loc[ diaobjectid, 'dec' ] !=
                     pytest.approx( objdf.loc[ diaobjectid, 'dec' ], abs=0.001/3600. ) )


    # TODO : test object_processing_version and position_processing_version

    # TODO : test the case where some objects have no positions, and make sure the patch position goes in there.
    #  (This will require changing the set of lightcurves fixtures....)
