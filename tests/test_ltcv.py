import itertools
import io
import time
import pytest

import numpy as np
import pandas

import db
import ltcv
from util import FDBLogger


def test_get_object_infos( set_of_lightcurves, procver_collection ):
    bpvs, _pvs, _pvinfo = procver_collection
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

    # Test passing base_procvers
    with pytest.raises( ValueError, match="Must supply a position processing.version with base_procvers" ):
        info  = ltcv.get_object_infos( [ roots[i]['root'].id for i in [0, 1, 2] ], return_format='pandas',
                                       base_procvers=[ bpvs[i].id
                                                       for i in [ 'realtime_diaobject', 'bpv1_diaobject' ] ] )
    info  = ltcv.get_object_infos( [ roots[i]['root'].id for i in [0, 1, 2] ], return_format='pandas',
                                   base_procvers=[ bpvs[i].id for i in [ 'realtime_diaobject', 'bpv1_diaobject' ] ],
                                   position_processing_version='realtime' )
    assert set( info.rootid ) == set( roots[i]['root'].id for i in [0, 1, 2] )
    assert set( info.index.values ) == { 0, 1, 2, 100 }
    for objid in [ 0, 1, 2 ]:
        assert not pandas.isna( info.loc[ objid, 'ra' ] )
        assert not pandas.isna( info.loc[ objid, 'dec' ] )
    for col in [ 'ra', 'dec', 'raerr', 'decerr', 'ra_dec_cov' ]:
        assert pandas.isna( info.loc[ 100, col ] )

    info = ltcv.get_object_infos(  [ roots[i]['root'].id for i in [0, 1, 2] ], return_format='pandas',
                                   base_procvers=[ bpvs[i].id for i in [ 'realtime_diaobject', 'bpv2_diaobject' ] ],
                                   position_processing_version='pvc_pv2' )
    assert set( info.rootid ) == set( roots[i]['root'].id for i in [0, 1, 2] )
    assert set( info.index.values ) == { 0, 1, 2, 200, 201, 2011, 202 }
    for objid in [ 200, 201, 202 ]:
        assert not pandas.isna( info.loc[ objid, 'ra' ] )
        assert not pandas.isna( info.loc[ objid, 'dec' ] )
    for objid in [ 0, 1, 2, 2011 ]:
        for col in [ 'ra', 'dec', 'raerr', 'decerr', 'ra_dec_cov' ]:
            assert pandas.isna( info.loc[ objid, col ] )

    # Test passing an object id table
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


def find_bpv_key( bpvs, bpvid ):
    for k, value in bpvs.items():
        if value.id == bpvid:
            return k
    return None


def compare_ltcv_to_expected( srcdf, frcdf, patdf,
                              srcinfo=None, frcinfo=None, patinfo=None,
                              bands=None,
                              expected_roots=[],
                              expected_diaobjectids=None,
                              include_source_positions=False,
                              include_base_procver=False,
                              include_object_positions=False,
                              use_weighted_source_positions=False,
                              always_use_weighted_source_positions=False,
                              procver=None,
                              set_of_lightcurves=None,
                              procver_collection=None,
                              all_roots_in_srcdf=False,
                              all_roots_in_frcdf=False,
                              mjdnow=None,
                              all_source_diaobjectids_in_pat=True,
                              all_forced_diaobjectids_in_pat=True ):
    bpvcol, _pvs, _pvinfo = procver_collection
    roots = set_of_lightcurves

    if all( i is None for i in [ srcdf, frcdf, patdf ] ):
        raise ValueError( "Nothging to compare" )

    expected_root_ids = [ roots[i]['root'].id for i in expected_roots ]
    use_weighted_source_positions = use_weighted_source_positions or always_use_weighted_source_positions

    # This may have been called from test_object_ltcv, in which case there's no index,
    #   or from test_many_ltcvs, in which case (rootid, mjd) are indexes.
    # Reset the indexes so we have something consistent
    srcdf = srcdf.reset_index() if srcdf is not None else None
    frcdf = frcdf.reset_index() if frcdf is not None else None
    patdf = patdf.reset_index() if patdf is not None else None
    dfs = [ srcdf, frcdf, patdf ]

    multiobject = True
    if any( ( i is not None ) and ( 'rootid' in i.columns ) for i in dfs ):
        if patdf is not None:
            assert set( patdf.rootid ) == set( expected_root_ids )
        if srcdf is not None:
            if all_roots_in_srcdf:
                assert set( srcdf.rootid ) == set( expected_root_ids )
            else:
                assert set( srcdf.rootid ).issubset( set( expected_root_ids ) )
        if frcdf is not None:
            if all_roots_in_frcdf:
                assert set( frcdf.rootid ) == set( expected_root_ids )
            else:
                assert set( frcdf.rootid ).issubset( set( expected_root_ids ) )
    else:
        multiobject = False
        assert len(expected_root_ids) == 1

    if srcdf is not None:
        assert 'forced_diaobjectid' not in srcdf.columns

    if expected_diaobjectids is not None:
        if patdf is not None:
            # Oh, edgecases
            if all_source_diaobjectids_in_pat:
                assert ( set( patdf[ ~pandas.isna( patdf.source_diaobjectid) ].source_diaobjectid.values )
                         == set( expected_diaobjectids ) )
            else:
                assert ( set( patdf[ ~pandas.isna( patdf.source_diaobjectid) ].source_diaobjectid.values )
                        .issubset( set( expected_diaobjectids ) ) )
            if all_forced_diaobjectids_in_pat:
                assert ( set( patdf[ ~pandas.isna( patdf.forced_diaobjectid) ].forced_diaobjectid.values )
                         == set( expected_diaobjectids ) )
            else:
                assert ( set( patdf[ ~pandas.isna( patdf.forced_diaobjectid) ].forced_diaobjectid.values )
                         .issubset( set( expected_diaobjectids ) ) )

        if srcdf is not None:
            assert ( set( srcdf[ ~pandas.isna( srcdf.source_diaobjectid) ].source_diaobjectid.values )
                     == set( expected_diaobjectids ) )

        if frcdf is not None:
            assert ( set( frcdf[ ~pandas.isna( frcdf.source_diaobjectid) ].source_diaobjectid.values )
                     .issubset( expected_diaobjectids ) )
            assert ( set( frcdf[ ~pandas.isna( frcdf.forced_diaobjectid) ].forced_diaobjectid.values )
                     .issubset( expected_diaobjectids ) )

    src_bpvs = procver.base_procvers( 'diasource' )
    frc_bpvs = procver.base_procvers( 'diaforcedsource' )
    src_bpvkeys = [ find_bpv_key( bpvcol, b.id ) for b in src_bpvs ]
    frc_bpvkeys = [ find_bpv_key( bpvcol, b.id ) for b in frc_bpvs ]

    datacache = {}
    for rootid in expected_root_ids:
        rdex = None
        for i, root in enumerate( roots ):
            if root['root'].id == rootid:
                rdex = i
                break
        if rdex is None:
            raise ValueError( "Failed to find root object {rootid}" )

        if multiobject:
            subsrc = srcdf[ srcdf['rootid'] == rootid ] if srcdf is not None else None
            subfrc = frcdf[ frcdf['rootid'] == rootid ] if frcdf is not None else None
            subpat = patdf[ patdf['rootid'] == rootid ] if patdf is not None else None
        else:
            subsrc = srcdf
            subfrc = frcdf
            subpat = patdf

        if all( ( x is None ) or ( len(x) == 0 ) for x in ( subsrc, subfrc, subpat ) ):
            continue

        # OK, here's the deal.  For each rootid, there are multiple base processing versions that have
        #   data.  We need to extract the highest priority for each.  To do this, rewrangle the
        #   set_of_lightcurves data so that its indexed by base_procver, in descending priority order.

        if mjdnow is not None:
            if bands is not None:
                conds = lambda s: s.midpointmjdtai <= mjdnow and s.band in bands
            else:
                conds = lambda s: s.midpointmjdtai <= mjdnow
        elif bands is not None:
            conds = lambda s: s.band in bands
        else:
            conds = lambda s: True

        reindexed_srces = { k: { s.visit: s for s in roots[rdex]['src'][k] if conds(s) }
                            for k in src_bpvkeys if k in roots[rdex]['src'].keys() }
        reindexed_frced = { k: { f.visit: f for f in roots[rdex]['frc'][k] if conds(f) }
                            for k in frc_bpvkeys if k in roots[rdex]['frc'].keys() }

        # Visit is the thing that lets us decide if a diasource and a diaforcedsource are the same thing.
        # Get the set of visits defined for this object.
        allvisits = set( itertools.chain( *[ list(x.keys()) for x in reindexed_srces.values() ] ) )
        allvisits = allvisits.union( set( itertools.chain( *[ list(x.keys()) for x in reindexed_frced.values() ] ) ) )

        data = []
        for visit in allvisits:
            thisdata = {}

            for bpv, bpvkey in zip( src_bpvs, src_bpvkeys ):
                srcgotit = False
                if bpvkey in reindexed_srces:
                    if visit in reindexed_srces[bpvkey]:
                        thisdata['src'] = reindexed_srces[bpvkey][visit]
                        thisdata['src_bpv'] = bpv
                        srcgotit = True
                        break
                if not srcgotit:
                    thisdata['src'] = None
                    thisdata['src_bpv'] = None

            for bpv, bpvkey in zip( frc_bpvs, frc_bpvkeys ):
                frcgotit = False
                if bpvkey in reindexed_frced:
                    if visit in reindexed_frced[bpvkey]:
                        thisdata['frc'] = reindexed_frced[bpvkey][visit]
                        thisdata['frc_bpv'] = bpv
                        frcgotit = True
                        break
                if not frcgotit:
                    thisdata['frc'] = None
                    thisdata['frc_bpv'] = None

            if srcgotit or frcgotit:
                data.append( thisdata )

        # Sort data by mjd, since that's how the ltcv returns sort things
        data.sort( key=lambda x: ( x['src'].midpointmjdtai
                                   if x['src'] is not None
                                   else x['frc'].midpointmjdtai ) )

        # Save this as it may be used again below
        datacache[rootid] = data

        # OK, now we have wrangled things to the point where they can be compared

        justsrcs = [ d for d in data if d['src'] is not None ]
        justfrced = [ d for d in data if d['frc'] is not None ]
        if subsrc is not None:
            assert len(justsrcs) == len(subsrc)
            for row, dat in zip( subsrc.itertuples(), justsrcs ):
                assert row.diasourceid == dat['src'].diasourceid
                assert 'diaforcedsourceid' not in dir(row)
                assert row.visit == dat['src'].visit
                assert row.source_diaobjectid == dat['src'].diaobjectid
                assert row.band == dat['src'].band
                assert row.isdet == 1
                assert row.flux == pytest.approx( dat['src'].psfflux, rel=1e-6 )
                assert row.fluxerr == pytest.approx( dat['src'].psffluxerr, rel=1e-6 )
                if include_base_procver:
                    assert row.base_procver_s == dat['src_bpv'].description
                else:
                    assert 'base_procver_s' not in dir( row )
                assert 'base_procver_f' not in dir( row )
                if include_source_positions:
                    assert row.det_ra == pytest.approx( dat['src'].ra, abs=0.01/3600. )
                    assert row.det_dec == pytest.approx( dat['src'].dec, abs=0.01/3600. )
                    assert ( ( pandas.isna(row.det_raerr) and ( dat['src'].raerr is None ) ) or
                             ( row.det_raerr == pytest.approx( dat['src'].raerr, abs=0.01/3600. ) ) )
                    assert ( ( pandas.isna(row.det_decerr) and ( dat['src'].decerr is None ) ) or
                             ( row.det_decerr == pytest.approx( dat['src'].decerr, abs=0.01/3600. ) ) )
                    assert ( ( pandas.isna(row.det_ra_dec_cov) and ( dat['src'].ra_dec_cov is None ) ) or
                             ( row.det_ra_dec_cov == pytest.approx( dat['src'].ra_dec_cov, abs=(0.01/3600)**1.8 ) ) )
                else:
                    assert all( f'det_{x}' not in dir(row) for x in [ 'ra', 'dec', 'raerr', 'decerr', 'ra_dec_cov' ] )

        if subfrc is not None:
            assert len(justfrced) == len(subfrc)
            for row, dat in zip( subfrc.itertuples(), justfrced ):
                assert ( pandas.isna(row.diasourceid) if dat['src'] is None
                         else ( row.diasourceid == dat['src'].diasourceid ) )
                assert row.diaforcedsourceid == dat['frc'].diaforcedsourceid
                assert row.visit == dat['frc'].visit
                assert row.forced_diaobjectid == dat['frc'].diaobjectid
                assert row.isdet == ( 1 if dat['src'] is not None else 0 )
                assert row.flux == pytest.approx( dat['frc'].psfflux, rel=1e-6 )
                assert row.fluxerr == pytest.approx( dat['frc'].psffluxerr, rel=1e-6 )
                if include_base_procver:
                    assert row.base_procver_f == dat['frc_bpv'].description
                    assert ( pandas.isna(row.base_procver_s) if dat['src'] is None
                             else ( row.base_procver_s == dat['src_bpv'].description ) )
                else:
                    assert 'base_procver_s' not in dir( row )
                    assert 'base_procver_f' not in dir( row )
                if include_source_positions:
                    if dat['src'] is not None:
                        assert row.det_ra == pytest.approx( dat['src'].ra, abs=0.01/3600. )
                        assert row.det_dec == pytest.approx( dat['src'].dec, abs=0.01/3600. )
                        assert ( ( pandas.isna(row.det_raerr) and ( dat['src'].raerr is None ) ) or
                                 ( row.det_raerr == pytest.approx( dat['src'].raerr, abs=0.01/3600. ) ) )
                        assert ( ( pandas.isna(row.det_decerr) and ( dat['src'].decerr is None ) ) or
                                 ( row.det_decerr == pytest.approx( dat['src'].decerr, abs=0.01/3600. ) ) )
                        assert ( ( pandas.isna(row.det_ra_dec_cov) and ( dat['src'].ra_dec_cov is None ) ) or
                                 ( row.det_ra_dec_cov == pytest.approx( dat['src'].ra_dec_cov,
                                                                        abs=(0.01/3600)**1.8 ) ) )
                    else:
                        assert all( pandas.isna( getattr(row, f"det_{x}" ) ) for x in [ 'ra', 'dec', 'raerr',
                                                                                        'decerr', 'ra_dec_cov' ] )
                else:
                    assert all( f'det_{x}' not in dir(row) for x in [ 'ra', 'dec', 'raerr', 'decerr', 'ra_dec_cov' ] )

        if subpat is not None:
            for row, dat in zip( subpat.itertuples(), data ):
                assert row.isdet == ( 1 if dat['src'] is not None else 0 )
                assert row.ispatch == ( 1 if dat['frc'] is None else 0 )
                assert ( pandas.isna(row.diasourceid) if dat['src'] is None
                         else ( row.diasourceid == dat['src'].diasourceid ) )
                assert ( pandas.isna(row.diaforcedsourceid) if dat['frc'] is None
                         else ( row.diaforcedsourceid == dat['frc'].diaforcedsourceid ) )
                if dat['src'] is not None:
                    assert row.isdet == 1
                    assert row.source_diaobjectid == dat['src'].diaobjectid
                    assert row.visit == dat['src'].visit
                    assert row.band == dat['src'].band
                    if include_base_procver:
                        assert row.base_procver_s == dat['src_bpv'].description
                    else:
                        assert 'base_procver_s' not in dir(row)
                    if include_source_positions:
                        assert row.det_ra == pytest.approx( dat['src'].ra, abs=0.01/3600. )
                        assert row.det_dec == pytest.approx( dat['src'].dec, abs=0.01/3600. )
                        assert ( ( pandas.isna(row.det_raerr) and ( dat['src'].raerr is None ) ) or
                                 ( row.det_raerr == pytest.approx( dat['src'].raerr, abs=0.01/3600. ) ) )
                        assert ( ( pandas.isna(row.det_decerr) and ( dat['src'].decerr is None ) ) or
                                 ( row.det_decerr == pytest.approx( dat['src'].decerr, abs=0.01/3600. ) ) )
                        assert ( ( pandas.isna(row.det_ra_dec_cov) and ( dat['src'].ra_dec_cov is None ) ) or
                                 ( row.det_ra_dec_cov == pytest.approx( dat['src'].ra_dec_cov,
                                                                        abs=(0.01/3600.)**1.8 ) ) )
                    else:
                        assert all( f'det_{x}' not in dir(row) for x in [ 'ra', 'dec',
                                                                          'raerr', 'decerr', 'ra_dec_cov' ] )
                    if dat['frc'] is None:
                        assert row.ispatch == 1
                        assert row.flux == pytest.approx( dat['src'].psfflux, rel=1e-6 )
                        assert row.fluxerr == pytest.approx( dat['src'].psffluxerr, rel=1e-6 )
                else:
                    assert row.isdet == 0
                    if include_base_procver:
                        assert pandas.isna( row.base_procver_s )
                    else:
                        assert 'base_procver_s' not in dir(row)

                if dat['frc'] is not None:
                    assert row.ispatch == 0
                    assert row.forced_diaobjectid == dat['frc'].diaobjectid
                    assert row.visit == dat['frc'].visit
                    assert row.band == dat['frc'].band
                    assert row.flux == pytest.approx( dat['frc'].psfflux, rel=1e-6 )
                    assert row.fluxerr == pytest.approx( dat['frc'].psffluxerr, rel=1e-6 )
                    if include_base_procver:
                        assert row.base_procver_f == dat['frc_bpv'].description
                    else:
                        assert 'base_procver_f' not in dir(row)
                else:
                    if include_base_procver:
                        assert pandas.isna( row.base_procver_f )
                    else:
                        assert 'base_procver_f' not in dir(row)

    if any( i is not None for i in [ srcinfo, frcinfo, patinfo ] ):
        assert ( srcinfo is None ) == ( srcdf is None )
        assert ( frcinfo is None ) == ( frcdf is None )
        assert ( patinfo is None ) == ( patdf is None )

        pos_bpvs = procver.base_procvers( 'diaobject_position' )
        pos_bpvkeys = [ find_bpv_key( bpvcol, b.id ) for b in pos_bpvs ]

        expected_columns = { 'rootid' }
        if include_object_positions:
            expected_columns = expected_columns.union( { 'ra', 'dec', 'raerr', 'decerr', 'ra_dec_cov' } )
        if include_base_procver:
            expected_columns.add( 'obj_base_procver' )
            if include_object_positions:
                expected_columns.add( 'pos_base_procver')

        if patinfo is not None:
            assert set( patinfo.index.values ) == set( expected_diaobjectids )
        if srcinfo is not None:
            assert set( srcinfo.index.values ).issubset( set( expected_diaobjectids ) )
        if frcinfo is not None:
            assert set( frcinfo.index.values ).issubset( set( expected_diaobjectids ) )

        for rootid in expected_root_ids:
            rdex = None
            for i, root in enumerate( roots ):
                if root['root'].id == rootid:
                    rdex = i
                    break

            data = datacache[rootid]
            justsrcs = [ d for d in data if d['src'] is not None ]

            for info in [ srcinfo, frcinfo, patinfo ]:
                if info is None:
                    continue
                subinfo = info[ info['rootid'] == rootid ]
                diaobjectids = subinfo.index.values

                for diaobjectid in diaobjectids:
                    if diaobjectid not in info.index.values:
                        continue

                    obj_base_procver_id = roots[rdex]['obj'][diaobjectid].base_procver_id
                    obj_base_procver = [ v for v in bpvcol.values() if v.id==obj_base_procver_id ][0]

                    subsubinfo = info.loc[diaobjectid]
                    assert isinstance( subsubinfo, pandas.Series )
                    assert set( subsubinfo.index.values ) == expected_columns
                    if include_base_procver:
                        assert obj_base_procver.description == subsubinfo.obj_base_procver
                    assert rootid == subsubinfo.rootid

                    if not include_object_positions:
                        assert ( i not in subsubinfo.columns
                                 for i in [ 'ra', 'dec', 'raerr', 'decerr', 'ra_dec_cov' ] )
                    else:
                        objpos = None
                        posbpv = None
                        for bpv, bpvkey in zip( pos_bpvs, pos_bpvkeys ):
                            if ( diaobjectid, bpvkey ) in roots[rdex]['pos'].keys():
                                objpos = roots[rdex]['pos'][ (diaobjectid, bpvkey) ]
                                posbpv = bpvcol[bpvkey]
                                break

                        if ( objpos is None ) and ( not use_weighted_source_positions ):
                            assert all( pandas.isna( subsubinfo[x] )
                                        for x in [ 'ra', 'dec', 'raerr', 'decerr', 'ra_dec_cov' ] )
                            if include_base_procver:
                                assert pandas.isna( subsubinfo['pos_base_procver'] )
                        elif ( objpos is not None ) and ( not always_use_weighted_source_positions ):
                            if include_base_procver:
                                if posbpv is None:
                                    assert pandas.isna( subinfo.pos_base_procver )
                                else:
                                    assert posbpv.description == subsubinfo.pos_base_procver
                            assert objpos.ra == pytest.approx( subsubinfo.ra, abs=0.01/3600. )
                            assert objpos.dec == pytest.approx( subsubinfo.dec, abs=0.01/3600. )
                            assert ( ( ( objpos.raerr is None ) and pandas.isna( subsubinfo.raerr ) )
                                     or ( objpos.raerr == pytest.approx( subsubinfo.raerr, abs=0.01/3600. ) ) )
                            assert ( ( ( objpos.decerr is None ) and pandas.isna( subsubinfo.decerr ) )
                                     or ( objpos.decerr == pytest.approx( subsubinfo.decerr, abs=0.01/3600. ) ) )
                            assert ( ( ( objpos.ra_dec_cov is None ) and pandas.isna( subsubinfo.ra_dec_cov ) )
                                     or ( objpos.ra_dec_cov == pytest.approx( subsubinfo.ra_dec_cov,
                                                                              abs=(0.01/3600.)**1.8 ) ) )
                        else:
                            if include_base_procver:
                                assert pandas.isna( subsubinfo.pos_base_procver )

                            srcra = np.array( [ i['src'].ra for i in justsrcs ] )
                            srcdec = np.array( [ i['src'].dec for i in justsrcs ] )
                            sn = np.array( [ i['src'].psfflux / i['src'].psffluxerr for i in justsrcs ] )
                            w = np.where( sn > 3 )[0]
                            srcra = srcra[w]
                            srcdec = srcdec[w]
                            weight = sn[w] ** 2
                            meanra = ( srcra * weight ).sum() / ( weight.sum() )
                            meandec = ( srcdec * weight ).sum() / ( weight.sum() )
                            raerr = np.sqrt( ( weight * ( srcra - meanra )**2 ).sum() / weight.sum() )
                            decerr = np.sqrt( ( weight * ( srcdec - meandec )**2 ).sum() / weight.sum() )
                            ra_dec_cov = ( weight * ( srcra - meanra ) * ( srcdec - meandec ) ).sum() / weight.sum()

                            # In this case, the positions should *not* match the object positions, if they
                            #   exist...  The fixture scattered the positions with a σ of 0.2".
                            #   (But, I've found cases in the get_hot_ltcvs tests where
                            #   the weighted position matches the diaobject_position to 0.01"....)
                            if objpos is not None:
                                assert not subsubinfo.ra == pytest.approx( objpos.ra, abs=0.01/3600. )
                                assert not subsubinfo.dec == pytest.approx( objpos.dec, abs=0.01/3600. )

                            # ...but should be damn close to the calculated mean positions, since
                            #   other code did (almost) the same calculation.  (Not exactly, because numpy vs.
                            #   pandas, so floating-point roundoff means we can't expect doubles to be good
                            #   to better than 1e-14... and in fact it's worse than that, because the
                            #   weights are based on 32-bit floats, so if anything I'm surprsied they
                            #   match as well as they do.
                            assert subsubinfo.ra == pytest.approx( meanra, rel=1e-12 )
                            assert subsubinfo.dec == pytest.approx( meandec, rel=1e-12 )
                            assert subsubinfo.raerr == pytest.approx( raerr, rel=1e-6 )
                            assert subsubinfo.decerr == pytest.approx( decerr, rel=1e-6 )
                            # The fixtures had no correlations between ra and dec, so
                            #   the covariance should be close to 0, based on random
                            #   statistics of how many points went into it.
                            assert subsubinfo.ra_dec_cov == pytest.approx( 0., abs=1e-9 )
                            assert ra_dec_cov == pytest.approx( 0., abs=1e-9 )


def test_object_ltcv( procver_collection, set_of_lightcurves ):
    # TODO : refactor this to use lightcurve_checker
    
    # TODO : write a test for the case where there are multiple objects within the
    #   same processing version that point to the same root object!

    roots = set_of_lightcurves
    _bpvs, pvs, _pvinfo = procver_collection

    # The fixture loads up lightcurves every 2.5 days

    # Try to get the object lightcurve for diaobjectid 100 using pv1
    # Should get detections starting 60000, forced starting 59990,
    # sources through 60015 and forced through 60010 in bpv1a,
    # sources through 60030 and forced through 60025 in bpv1

    srcs = ltcv.object_ltcv( pvs['pv1'].id, 100, return_format='pandas', which='detections', include_base_procver=True )
    forced = ltcv.object_ltcv( pvs['pv1'].id, 100, return_format='pandas', which='forced', include_base_procver=True )
    df = ltcv.object_ltcv( pvs['pv1'].id, 100, return_format='pandas', which='patch', include_base_procver=True )
    # check_obj_100_in_pv1( roots[0]['src'], roots[0]['frc'], srcs, forced, df, procver_collection )
    compare_ltcv_to_expected( srcs, forced, df, expected_roots=[0], expected_diaobjectids=[100],
                              include_base_procver=True, procver=pvs['pv1'],
                              set_of_lightcurves=roots, procver_collection=procver_collection )

    srcs = ltcv.object_ltcv( pvs['pv1'].id, 100, return_format='pandas', which='detections',
                             include_base_procver=True, include_source_positions=True )
    forced = ltcv.object_ltcv( pvs['pv1'].id, 100, return_format='pandas', which='forced',
                               include_base_procver=True, include_source_positions=True )
    df = ltcv.object_ltcv( pvs['pv1'].id, 100, return_format='pandas', which='patch',
                           include_base_procver=True, include_source_positions=True )
    compare_ltcv_to_expected( srcs, forced, df, expected_roots=[0], expected_diaobjectids=[100],
                              include_base_procver=True, include_source_positions=True, procver=pvs['pv1'],
                              set_of_lightcurves=roots, procver_collection=procver_collection )
    # check_obj_100_in_pv1( roots[0]['src'], roots[0]['frc'], srcs, forced, df, procver_collection,
    #                       include_source_positions=True )

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
    compare_ltcv_to_expected( srcs, forced, df, expected_roots=[1], expected_diaobjectids=[201, 2011],
                              include_base_procver=True, procver=pvs['pv2'],
                              set_of_lightcurves=roots, procver_collection=procver_collection )

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

    srcs = ltcv.object_ltcv( 'pvc_pv2', roots[1]['root'].id, return_format='pandas',
                             which='detections', include_base_procver=True )
    forced = ltcv.object_ltcv( 'pvc_pv2', roots[1]['root'].id, return_format='pandas',
                               which='forced', include_base_procver=True )
    df = ltcv.object_ltcv( 'pvc_pv2', roots[1]['root'].id, return_format='pandas',
                           which='patch', include_base_procver=True )
    compare_ltcv_to_expected( srcs, forced, df, expected_roots=[1], expected_diaobjectids=[201, 2011],
                              include_base_procver=True, procver=pvs['pv2'],
                              set_of_lightcurves=roots, procver_collection=procver_collection )

    # assert len(df) == len(df2)
    # assert ( df[ ~pandas.isna(df.diasourceid) ] == df2[ ~pandas.isna(df2.diasourceid) ] ).all().all()
    # assert ( df.loc[ :, [ c for c in df.columns if c not in [ 'diasourceid', 'base_procver_s' ] ] ] ==
    #          df2.loc[ :, [ c for c in df2.columns if c not in [ 'diasourceid', 'base_procver_s' ] ] ]
    #         ).all().all()

    # Test mjd_now
    srcs = ltcv.object_ltcv( pvs['pv2'].id, roots[1]['root'].id, return_format='pandas',
                             which='detections', include_base_procver=True, mjd_now=60040 )
    forced = ltcv.object_ltcv( pvs['pv2'].id, roots[1]['root'].id, return_format='pandas',
                               which='forced', include_base_procver=True, mjd_now=60040 )
    df = ltcv.object_ltcv( pvs['pv2'].id, roots[1]['root'].id, return_format='pandas',
                           which='patch', include_base_procver=True, mjd_now=60040 )
    compare_ltcv_to_expected( srcs, forced, df, expected_roots=[1], expected_diaobjectids=[201, 2011],
                              include_base_procver=True, procver=pvs['pv2'],
                              set_of_lightcurves=roots, procver_collection=procver_collection,
                              mjdnow=60040 )

    # Test include_source_positions

    srcs = ltcv.object_ltcv( pvs['pv2'].id, 200, return_format='pandas', which='detections',
                             include_base_procver=True, include_source_positions=True )
    forced = ltcv.object_ltcv( pvs['pv2'].id, 200, return_format='pandas', which='forced',
                               include_base_procver=True, include_source_positions=True )
    df = ltcv.object_ltcv( pvs['pv2'].id, 200, return_format='pandas', which='patch',
                           include_base_procver=True, include_source_positions=True )
    compare_ltcv_to_expected( srcs, forced, df, expected_roots=[0], expected_diaobjectids=[200],
                              include_base_procver=True, include_source_positions=True, procver=pvs['pv2'],
                              set_of_lightcurves=roots, procver_collection=procver_collection )

    # Test return_object_info

    for inclpos in [ True, False ]:
        srcs2, srcinfo = ltcv.object_ltcv( pvs['pv2'].id, 200, return_format='pandas', which='detections',
                                           include_base_procver=True, include_source_positions=True,
                                           return_object_info=True, include_object_positions=inclpos )
        forced2, frcinfo = ltcv.object_ltcv( pvs['pv2'].id, 200, return_format='pandas', which='forced',
                                             include_base_procver=True, include_source_positions=True,
                                             return_object_info=True, include_object_positions=inclpos )
        df2, dfinfo = ltcv.object_ltcv( pvs['pv2'].id, 200, return_format='pandas', which='patch',
                                        include_base_procver=True, include_source_positions=True,
                                        return_object_info=True, include_object_positions=inclpos )
        compare_ltcv_to_expected( srcs2, forced2, df2, srcinfo=srcinfo, frcinfo=frcinfo, patinfo=dfinfo,
                                  expected_roots=[0], expected_diaobjectids=[200], include_source_positions=True,
                                  include_base_procver=True, procver=pvs['pv2'], include_object_positions=inclpos,
                                  set_of_lightcurves=roots, procver_collection=procver_collection )


        # return_object_info and json

        jsrc2, jsrcinfo = ltcv.object_ltcv( pvs['pv2'].id, 200, return_format='json', which='detections',
                                            include_base_procver=True, include_source_positions=True,
                                            return_object_info=True, include_object_positions=inclpos )
        jforced2, jfrcinfo = ltcv.object_ltcv( pvs['pv2'].id, 200, return_format='json', which='forced',
                                               include_base_procver=True, include_source_positions=True,
                                               return_object_info=True, include_object_positions=inclpos )
        jdf2, jdfinfo = ltcv.object_ltcv( pvs['pv2'].id, 200, return_format='json', which='patch',
                                          include_base_procver=True, include_source_positions=True,
                                          return_object_info=True, include_object_positions=inclpos )
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
                                     return_object_info=True, include_object_positions=True )
    frc, frcinfo = ltcv.object_ltcv( pvs['pv2'].id, 203, return_format='pandas', which='forced',
                                     return_object_info=True, include_object_positions=True )
    pat, patinfo = ltcv.object_ltcv( pvs['pv2'].id, 203, return_format='pandas', which='patch',
                                     return_object_info=True, include_object_positions=True )
    for info in [ srcinfo, frcinfo, patinfo ]:
        assert all( pandas.isna( info.loc[ :, [ 'ra', 'dec', 'raerr', 'decerr', 'ra_dec_cov' ] ] ) )
    compare_ltcv_to_expected( src, frc, pat, srcinfo=srcinfo, frcinfo=frcinfo, patinfo=patinfo,
                              expected_roots=[3], expected_diaobjectids=[203], procver=pvs['pv2'],
                              include_object_positions=True,
                              set_of_lightcurves=roots, procver_collection=procver_collection )

    # Now use weighted source positions, and make sure we get the right thing
    src, srcinfo = ltcv.object_ltcv( pvs['pv2'].id, 203, return_format='pandas', which='detections',
                                     return_object_info=True, use_weighted_source_positions=True,
                                     include_base_procver=True, include_object_positions=True )
    frc, frcinfo = ltcv.object_ltcv( pvs['pv2'].id, 203, return_format='pandas', which='forced',
                                     return_object_info=True, use_weighted_source_positions=True,
                                     include_base_procver=True, include_object_positions=True )
    pat, patinfo = ltcv.object_ltcv( pvs['pv2'].id, 203, return_format='pandas', which='patch',
                                     return_object_info=True, use_weighted_source_positions=True,
                                     include_base_procver=True, include_object_positions=True )
    compare_ltcv_to_expected( src, frc, pat, srcinfo=srcinfo, frcinfo=frcinfo, patinfo=patinfo,
                              expected_roots=[3], expected_diaobjectids=[203],
                              procver=pvs['pv2'], include_base_procver=True, use_weighted_source_positions=True,
                              include_object_positions=True,
                              set_of_lightcurves=roots, procver_collection=procver_collection )

    # Next, check always_use_source_positions by passing object 201, which does have positions.
    # First, don't use any weighting.  object 201 is redundant in pv2, as object 2011 is the
    #   same object (share the same rootid).  So, when we ask for a lightcurve for 201,
    #   we get both 201 and 2011 back in the object info.  2011 should have no position at all.
    src, srcinfo = ltcv.object_ltcv( pvs['pv2'].id, 201, return_format='pandas', which='detections',
                                     return_object_info=True, include_base_procver=True,
                                     include_object_positions=True )
    frc, frcinfo = ltcv.object_ltcv( pvs['pv2'].id, 201, return_format='pandas', which='forced',
                                     return_object_info=True, include_base_procver=True,
                                     include_object_positions=True )
    pat, patinfo = ltcv.object_ltcv( pvs['pv2'].id, 201, return_format='pandas', which='patch',
                                     return_object_info=True, include_base_procver=True,
                                     include_object_positions=True )
    for info in [ srcinfo, frcinfo, patinfo ]:
        assert set( info.index.values ) == { 201, 2011 }
        assert all( pandas.isna( info.xs(2011)[x] ) for x in [ 'pos_base_procver', 'ra', 'dec',
                                                               'raerr', 'decerr', 'ra_dec_cov' ] )
    compare_ltcv_to_expected( src, frc, pat, srcinfo=srcinfo, frcinfo=frcinfo, patinfo=patinfo,
                              expected_roots=[1], expected_diaobjectids=[201, 2011],
                              procver=pvs['pv2'], include_base_procver=True, include_object_positions=True,
                              set_of_lightcurves=roots, procver_collection=procver_collection )

    # Next, if we use use_weighted_source_positions but not always, we should get the
    #   diaobject_position position back for 201, but a weighted position for 2011.
    #   in the info array.  (Yes, this is perverse.  But, the data is complicated.)
    src, srcinfo = ltcv.object_ltcv( pvs['pv2'].id, 201, return_format='pandas', which='detections',
                                     return_object_info=True, use_weighted_source_positions=True,
                                     include_base_procver=True, include_object_positions=True )
    frc, frcinfo = ltcv.object_ltcv( pvs['pv2'].id, 201, return_format='pandas', which='forced',
                                     return_object_info=True, use_weighted_source_positions=True,
                                     include_base_procver=True, include_object_positions=True )
    pat, patinfo = ltcv.object_ltcv( pvs['pv2'].id, 201, return_format='pandas', which='patch',
                                     return_object_info=True, use_weighted_source_positions=True,
                                     include_base_procver=True, include_object_positions=True )
    for info in [ srcinfo, frcinfo, patinfo ]:
        assert len( info ) == 2
        assert set( info.index.values ) == { 201, 2011 }
        assert pandas.isna( info.xs(2011).pos_base_procver )
        # 201's position should match what's in diaobject_position
        assert info.xs(201).pos_base_procver == 'pvc_bpv2a_60030'
        pos = roots[1]['pos'][( 201, 'bpv2a_diaobject_position_60030' )]
        assert info.xs(201).ra == pytest.approx( pos.ra, rel=1e-14 )
        assert info.xs(201).dec == pytest.approx( pos.dec, rel=1e-14 )
        assert info.xs(201).raerr == pytest.approx( pos.raerr, rel=1e-6 )
        assert info.xs(201).decerr == pytest.approx( pos.decerr, rel=1e-6 )
        assert info.xs(201).ra_dec_cov == pytest.approx( pos.ra_dec_cov, rel=1e-6 )
        # But 2011 did not get position from diaobject_position
        assert pandas.isna( info.xs(2011).pos_base_procver )
    # This call will check that 2011 did get the right weighted source position
    compare_ltcv_to_expected( src, frc, pat, srcinfo=srcinfo, frcinfo=frcinfo, patinfo=patinfo,
                              expected_roots=[1], expected_diaobjectids=[201, 2011],
                              procver=pvs['pv2'], include_base_procver=True, use_weighted_source_positions=True,
                              include_object_positions=True,
                              set_of_lightcurves=roots, procver_collection=procver_collection )


    # Now put in always_....

    src, srcinfo = ltcv.object_ltcv( pvs['pv2'].id, 201, return_format='pandas', which='detections',
                                     return_object_info=True, always_use_weighted_source_positions=True,
                                     include_base_procver=True, include_object_positions=True )
    frc, frcinfo = ltcv.object_ltcv( pvs['pv2'].id, 201, return_format='pandas', which='forced',
                                     return_object_info=True, always_use_weighted_source_positions=True,
                                     include_base_procver=True, include_object_positions=True )
    pat, patinfo = ltcv.object_ltcv( pvs['pv2'].id, 201, return_format='pandas', which='patch',
                                     return_object_info=True, always_use_weighted_source_positions=True,
                                     include_base_procver=True, include_object_positions=True )
    for info in [ srcinfo, frcinfo, patinfo ]:
        # Because we did always_use_weighted_source_positions, info.pos_base_procver should be None
        #   for both the diaobjectid 201 and 2011 rows.
        assert len( info ) == 2
        assert set( info.index.values ) == { 201, 2011 }
        assert all( pandas.isna( info.pos_base_procver ) )
    compare_ltcv_to_expected( src, frc, pat, srcinfo=srcinfo, frcinfo=frcinfo, patinfo=patinfo,
                              expected_roots=[1], expected_diaobjectids=[201, 2011],
                              procver=pvs['pv2'], include_base_procver=True,
                              always_use_weighted_source_positions=True,
                              include_object_positions=True,
                              set_of_lightcurves=roots, procver_collection=procver_collection )


def test_many_object_ltcvs( procver_collection, set_of_lightcurves, lightcurve_checker ):
    # TODO : beef up these tests, think about more edge cases
    roots = set_of_lightcurves
    check_ltcv = lightcurve_checker

    ltcvlist = [
        # Object 1 is not in pv1, so ony expect object 0 back
        ( 'pvc_pv1', [ str(roots[i]['root'].id) for i in [0, 1] ], [0], [100], 'pv1' ),
        # pvc_pv2 should be the default
        ( None, [ str(roots[i]['root'].id) for i in [0, 2] ], [0, 2], [200, 202], 'pv2' ),
        # If we ask for diaobjects that are in the wrong processing version, we still get
        #   back the corresponding ones from the sources in this processing version
        ( 'pvc_pv2', [0, 2], [0, 2], [200, 202], 'pv2' ),
        ( 'pvc_pv2', [0, 1, 2], [0, 1, 2], [200, 201, 2011, 202], 'pv2' ),
        ( 'realtime', [0, 1, 2], [0, 1, 2], [0, 1, 2], 'realtime' ),
    ]

    extras = [
        {},
        { 'always_use_weighted_source_positions': 1, 'include_base_procver': 1,
          'include_source_positions': 1, 'include_object_positions': 1, 'return_object_info': 1 },
        { 'mjd_now': 60061., 'always_use_weighted_source_positions': 1, 'include_base_procver': 1,
          'include_source_positions': 1, 'include_object_positions': 1, 'return_object_info': 1 },
        { 'bands': 'r' },
        { 'bands': ['r'] },
        { 'include_source_positions': 1 },
        { 'return_object_info': 1 },
        { 'return_object_info': 1, 'include_object_positions': 1 },
        { 'return_object_info': 1, 'include_base_procver': 1 },
        { 'return_object_info': 1, 'include_base_procver': 1, 'include_object_positions': 1 },
        { 'use_weighted_source_positions': 1 },
        { 'always_use_weighted_source_positions': 1 },
        { 'mjd_now': 60041. }
    ]

    n = 0
    t0 = time.perf_counter()
    for ltcvreq in ltcvlist:
        for which in [ None, 'patch', 'detections', 'forced' ]:
            for extra in extras:
                if ltcvreq[0] is None:
                    url = '/ltcv/getmanyltcvs'
                else:
                    url = f'/ltcv/getmanyltcvs/{ltcvreq[0]}'

                kwargs = extra.copy()
                kwargs['objids'] = ltcvreq[1]
                if ltcvreq[0] is not None:
                    kwargs['processing_version'] = ltcvreq[0]
                if which is not None:
                    kwargs['which'] = which

                n += 1
                jsres = ltcv.many_object_ltcvs( return_format='json', **kwargs )
                pdres = ltcv.many_object_ltcvs( return_format='pandas', **kwargs )

                del kwargs['objids']
                if which is None:
                    kwargs['which'] = 'patch'
                if ltcvreq[0] is not None:
                    del kwargs['processing_version']

                # Verify the "json" (really, list/dict) return is right
                check_ltcv( ltcvreq[4], ltcvreq[2], ltcvreq[3], jsres, **kwargs )

                # Make sure pandas return is consistent
                n += 1
                if isinstance( pdres, tuple ):
                    # TODO, COMPARE infodf
                    pdltcvs = pdres[0]


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
            # Convert the pandas column to np.array so None=None will be true
            assert ( np.array( df[col] ) == np.array( j[col] ) ).all()

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
                             maxmag_maxdetection=23.8, ignore_object_processing_version=True )
    assert len(df) == 3
    assert set( df.rootid ) == { roots[i]['root'].id for i in [ 1, 2, 3 ] }
    df = ltcv.object_search( processing_version='pvc_pv3', return_format='pandas',
                             minmag_maxdetection=22.5, maxmag_maxdetection=23.8,
                             ignore_object_processing_version=True )
    assert len(df) == 2
    assert set( df.rootid ) == { roots[i]['root'].id for i in [ 2, 3 ] }

    # To filter on lastmag, we need to use mjd_now, because the fixture filled out
    #   forced photometry all down to mag 32.
    df = ltcv.object_search( processing_version='pvc_pv3', return_format='pandas', mjd_now=60056.,
                             ignore_object_processing_version=True )
    df.set_index( 'rootid', inplace=True )
    dexen = np.array( roots[i]['root'].id for i in range(4) )
    assert list( df.loc[ dexen, 'lastforcedmjd' ] ) == [ 60050., 60055., 60055., 60055. ]
    df = ltcv.object_search( processing_version='pvc_pv3', return_format='pandas', mjd_now=60056.,
                             min_lastmag=24., ignore_object_processing_version=True )
    assert len(df) == 2
    assert set( df.rootid ) == { roots[i]['root'].id for i in [ 0, 1 ] }
    df = ltcv.object_search( processing_version='pvc_pv3', return_format='pandas', mjd_now=60056.,
                             max_lastmag=24.8, ignore_object_processing_version=True )
    assert len(df) == 3
    assert set( df.rootid ) == { roots[i]['root'].id for i in [ 1, 2, 3 ] }
    df = ltcv.object_search( processing_version='pvc_pv3', return_format='pandas', mjd_now=60056.,
                             min_lastmag= 24., max_lastmag=24.8, ignore_object_processing_version=True )
    assert len(df) == 1
    assert set( df.rootid ) == { roots[1]['root'].id }

    strio = io.StringIO()
    strio.write( "Average timings:" )
    for k, v in ltcv._object_search_timings.items():
        n = ltcv._object_search_timings_count[ k ]
        strio.write( f"    {k:>34s} : {v/n:8.5f}\n" )
    FDBLogger.debug( strio.getvalue() )

    # TODO : test statbands.  (It is tested in test_ltcv_object_search.py)
    # (...and a good thing too, becasue it was broken.)


def test_get_hot_ltcvs( set_of_lightcurves, procver_collection ):
    # ...not sure how to test this without mjd_now since it uses the current time,
    #    and that will be different based on when this is run

    roots = set_of_lightcurves
    _bpvs, pvs, _pvinfo = procver_collection
    stuff = { 'set_of_lightcurves': roots, 'procver_collection': procver_collection }


    df, objdf = ltcv.get_hot_ltcvs( 'pvc_pv2', detected_since_mjd=60035, mjd_now=60056 )
    compare_ltcv_to_expected( None, None, df, patinfo=objdf, mjdnow=60056.,
                              include_object_positions=True, procver=pvs['pv2'],
                              expected_roots=[1, 2, 3], expected_diaobjectids=[201, 2011, 202, 203],
                              **stuff )

    df, objdf = ltcv.get_hot_ltcvs( 'pvc_pv2', detected_since_mjd=60035, mjd_now=60046 )
    compare_ltcv_to_expected( None, None, df, patinfo=objdf, mjdnow=60046,
                              include_object_positions=True, procver=pvs['pv2'],
                              expected_roots=[1, 2], expected_diaobjectids=[201, 2011, 202],
                              **stuff )

    df, objdf = ltcv.get_hot_ltcvs( 'pvc_pv2', detected_in_last_days=2, mjd_now=60021 )
    compare_ltcv_to_expected( None, None, df, patinfo=objdf, mjdnow=60021,
                              include_object_positions=True, procver=pvs['pv2'],
                              expected_roots=[0, 1], expected_diaobjectids=[200, 201, 2011],
                              all_source_diaobjectids_in_pat=False,
                              all_forced_diaobjectids_in_pat=True,
                              **stuff )

    df, objdf = ltcv.get_hot_ltcvs( 'pvc_pv2', detected_in_last_days=2, mjd_now=60041 )
    compare_ltcv_to_expected( None, None, df, patinfo=objdf, mjdnow=60021,
                              include_object_positions=True, procver=pvs['pv2'],
                              expected_roots=[1, 2], expected_diaobjectids=[201, 2011, 202],
                              **stuff )

    # detected_in_last_days defaults to 30
    df, objdf = ltcv.get_hot_ltcvs( 'pvc_pv2', mjd_now=60085 )
    compare_ltcv_to_expected( None, None, df, patinfo=objdf, mjdnow=60085,
                              include_object_positions=True, procver=pvs['pv2'],
                              expected_roots=[1, 2, 3], expected_diaobjectids=[201, 2011, 202, 203],
                              **stuff )

    df, objdf = ltcv.get_hot_ltcvs( 'pvc_pv2', mjd_now=60095 )
    compare_ltcv_to_expected( None, None, df, patinfo=objdf, mjdnow=60095,
                              include_object_positions=True, procver=pvs['pv2'],
                              expected_roots=[2], expected_diaobjectids=[202],
                              **stuff )

    # Weighted source positions
    df, objdf = ltcv.get_hot_ltcvs( 'pvc_pv2', mjd_now=60085, use_weighted_source_positions=True )
    compare_ltcv_to_expected( None, None, df, patinfo=objdf, mjdnow=60085,
                              include_object_positions=True, procver=pvs['pv2'],
                              use_weighted_source_positions=True,
                              expected_roots=[1, 2, 3], expected_diaobjectids=[201, 2011, 202, 203],
                              **stuff )

    df, objdf = ltcv.get_hot_ltcvs( 'pvc_pv2', mjd_now=60085, always_use_weighted_source_positions=True )
    compare_ltcv_to_expected( None, None, df, patinfo=objdf, mjdnow=60085,
                              include_object_positions=True, procver=pvs['pv2'],
                              always_use_weighted_source_positions=True,
                              expected_roots=[1, 2, 3], expected_diaobjectids=[201, 2011, 202, 203],
                              **stuff )

    # ...something
    df, objdf = ltcv.get_hot_ltcvs( 'realtime', mjd_now=60061 )
    compare_ltcv_to_expected( None, None, df, patinfo=objdf, mjdnow=60061,
                              include_object_positions=True, procver=pvs['realtime'],
                              expected_roots=[1, 2], expected_diaobjectids=[1, 2],
                              **stuff )
    df.reset_index( inplace=True )
    assert any( df.mjd > 60055 )
    assert any( df.mjd < 60055 )
    assert all( ~pandas.isna( df[ df.mjd<=60055 ].diaforcedsourceid ) )
    assert all( pandas.isna( df[ df.mjd>60055 ].diaforcedsourceid ) )

    # Forced
    df, objdf = ltcv.get_hot_ltcvs( 'realtime', mjd_now=60061, source_patch=False )
    compare_ltcv_to_expected( None, df, None, frcinfo=objdf, mjdnow=60061,
                              include_object_positions=True, procver=pvs['realtime'],
                              expected_roots=[1, 2], expected_diaobjectids=[1, 2],
                              **stuff )
    assert not any( df.reset_index().mjd > 60055 )
    assert all( ~pandas.isna( df.diaforcedsourceid ) )
