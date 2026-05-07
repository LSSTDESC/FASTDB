import time
import pytest

import numpy as np
import pandas
from psycopg import sql

import db
import ltcv
from util import FDBLogger


@pytest.fixture( scope='session' )
def objstats_cols():
    return { 'rootid', 'band', 'ra', 'dec',
             'firstdet_mjd', 'firstdet_flux', 'firstdet_fluxerr',
             'lastdet_mjd', 'lastdet_flux', 'lastdet_fluxerr',
             'maxdet_mjd', 'maxdet_flux', 'maxdet_fluxerr',
             'ndets', 'ndets24', 'ndets23', 'ndets22', 'ndets21',
             'nsn10', 'nsn7', 'nsn5' }


# THIS TEST MUST COME FIRST, because it depends on both the
#   objstats_realtime_view and set_of_lightcurves module-scope fixtures
#   not having been run.

def test_empty_objstats_realtime_view( procver_collection, objstats_cols ):
    try:
        ltcv.create_object_stats_materialized_view( 'realtime' )

        with db.DBCon() as con:
            rows, cols = con.execute( "SELECT * FROM objstats_realtime" )
            assert len(rows) == 0
            assert set(cols) == objstats_cols

            rows, cols = con.execute( "SELECT * FROM objstatscomb_realtime" )
            assert len(rows) == 0
            assert set(cols) == ( objstats_cols - { 'band' } )

            # TODO : test that indexes got created

    finally:
        with db.DBCon() as con:
            con.execute_nofetch( "DROP MATERIALIZED VIEW IF EXISTS objstatscomb_realtime" )
            con.execute_nofetch( "DROP MATERIALIZED VIEW IF EXISTS objstats_realtime" )
            con.commit()


# THIS TESTS MUST COME SECOND.  It depends on the set_of_lightcurve
#   module-scope fixture not having been run. The point of it is so that
#   the *next* fixture can test refreshing the view.

def test_create_objstats_realtime_view( objstats_realtime_view, objstats_cols ):
    with db.DBCon() as con:
        rows, cols = con.execute( "SELECT * FROM objstats_realtime" )
        assert len(rows) == 0
        assert set(cols) == objstats_cols

        rows, cols = con.execute( "SELECT * FROM objstatscomb_realtime" )
        assert len(rows) == 0
        assert set(cols) == ( objstats_cols - { 'band' } )


def test_objstats_view( objstats_realtime_view, set_of_lightcurves, check_db_rows_vs_expected ):
    # If this test is run as part of this whole file, then the test_create_objstats_realtime_view
    #   test will have created the view before set_of_lightcurves ran.
    # If we run this test by itself... I'm hoping the fixtures are evaluated in the
    #   order we list them.
    # Because the objstats_realtime_view was created before the diaobject, diasource
    #  tables were loaded, it should be empty
    with db.DBCon( dictcursor=True ) as con:
        rows = con.execute( "SELECT * FROM objstats_realtime" )
        assert len(rows) == 0

    # Now explicitly load it
    ltcv.create_object_stats_materialized_view( 'realtime' )
    with db.DBCon( dictcursor=True ) as con:
        rows = con.execute( "SELECT * FROM objstats_realtime" )
        combrows = con.execute( "SELECT * FROM objstatscomb_realtime" )
    check_db_rows_vs_expected( rows, combrows, expected_roots=[0,1,2] )

    # Now delete it, and then recreate it, to make sure it gets filled on creation
    with db.DBCon( dictcursor=True ) as con:
        con.execute( "DROP MATERIALIZED VIEW objstatscomb_realtime" )
        con.execute( "DROP MATERIALIZED VIEW objstats_realtime" )
        con.commit()

    with db.DBCon( dictcursor=True ) as con:
        rows = con.execute( "SELECT * FROM pg_class WHERE relname='objstats_realtime'" )
        assert len(rows) == 0
        rows = con.execute( "SELECT * FROM pg_class WHERE relname='objstatscomb_realtime'" )
        assert len(rows) == 0

    ltcv.create_object_stats_materialized_view( 'realtime' )
    with db.DBCon( dictcursor=True ) as con:
        rows = con.execute( "SELECT * FROM objstats_realtime" )
        combrows = con.execute( "SELECT * FROM objstatscomb_realtime" )
    check_db_rows_vs_expected( rows, combrows, expected_roots=[0,1,2] )

    # Check a different processing version, one which has a mix of bpvs (unlike realtime)
    try:
        ltcv.create_object_stats_materialized_view( 'pvc_pv2' )
        with db.DBCon( dictcursor=True ) as con:
            rows = con.execute( "SELECT * FROM objstats_pvc_pv2" )
            combrows = con.execute( "SELECT * FROM objstatscomb_pvc_pv2" )
        check_db_rows_vs_expected( rows, combrows, procver='pvc_pv2' )
    finally:
        with db.DBCon() as con:
            con.execute( "DROP MATERIALIZED VIEW IF EXISTS objstatscomb_pvc_pv2" )
            con.execute( "DROP MATERIALIZED VIEW IF EXISTS objstats_pvc_pv2" )
            con.commit()


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
    info = ltcv.get_object_infos( [ roots[i]['root'].id for i in [0, 1, 2] ], return_format='pandas',
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


def compare_pandas_to_json( pdltcvs, jsltcvs, pdobjinfo, jsobjinfo ):
    for thisltcv in jsltcvs:
        rootid = thisltcv['rootid']
        subpd = pdltcvs.xs( rootid, level='rootid' ).reset_index()
        for col in subpd.columns:
            cond = np.array( [ ( p == pytest.approx(val, rel=1e-12) )
                                 if col in ['mjd', 'det_ra', 'det_dec']
                               else ( p == pytest.approx(val, rel=1e-6) )
                                 if col in ['flux', 'fluxerr', 'det_raerr', 'det_decerr' ]
                               else ( p == pytest.approx(val, abs=0.0001/3600.) )
                                 if col == 'det_ra_dec_cov'
                               else ( p == val )
                               for p, val in zip( subpd[col], thisltcv[col] )
                              ] )
            assert ( cond
                     |
                     ( pandas.isna( subpd[col] ) & ( np.array( [ i is None for i in thisltcv[col] ] ) ) )
                    ).all()

    if jsobjinfo is not None:
        assert pdobjinfo.index.names == ['diaobjectid']
        pdobjinfo.reset_index( inplace=True )
        for k, v in jsobjinfo.items():
            cond = np.array( [ ( pandas.isna(p) & pandas.isna(val) )
                               or
                               ( ( p == pytest.approx(val, rel=1e-12) )
                                   if k in [ 'ra', 'dec' ]
                                 else ( p == pytest.approx(val, rel=1e-6 ) )
                                   if k in [ 'raerr', 'decerr' ]
                                 else ( p == pytest.approx(val, abs=0.0001/3600.) )
                                   if k == 'ra_dec_cov'
                                 else ( p == val )
                                )
                               for p, val in zip( pdobjinfo[k], v )
                              ] )
            assert cond.all()


def test_object_ltcv( set_of_lightcurves, procver_collection, lightcurve_checker ):
    check_ltcv = lightcurve_checker
    roots = set_of_lightcurves
    _bpvs, pvs, _pvinfo = procver_collection

    pvs_and_obs = [ { 'pv': 'pvc_pv1', 'obj': 100, 'exproot': [0], 'expobj': [100] },
                    { 'pv': 'pvc_pv1', 'obj': 200, 'exproot': [0], 'expobj': [100] },
                    { 'pv': 'pvc_pv1', 'obj': roots[0]['root'].id, 'exproot': [0], 'expobj': [100] },
                    { 'pv': pvs['pv1'], 'obj': 100, 'exproot': [0], 'expobj': [100] },
                    { 'pv': 'pvc_pv2', 'obj': 201, 'exproot': [1], 'expobj': [201, 2011] },
                    { 'pv': 'pvc_pv2', 'obj': roots[1]['root'].id, 'exproot': [1], 'expobj': [201, 2011] },
                    { 'pv': None, 'obj': 201, 'exproot': [1], 'expobj': [201, 2011] },
                    { 'pv': 'realtime', 'obj': 1, 'exproot': [1], 'expobj': [1] }
                   ]

    extras = [
        {},
        { 'mjd_now': 60041. },
        { 'bands': 'r' },
        { 'bands': ['r'] },
        { 'include_source_positions': 1 },
        { 'return_object_info': 1 },
        { 'return_object_info': 1, 'include_object_positions': 1 },
        { 'return_object_info': 1, 'include_base_procver': 1 },
        { 'return_object_info': 1, 'include_base_procver': 1, 'include_object_positions': 1 },
        { 'use_weighted_source_positions': 1, 'include_base_procver': 1 },
        { 'use_weighted_source_positions': 1, 'include_object_positions': 1 },
        { 'use_weighted_source_positions': 1, 'include_object_positions': 1, 'return_object_info': 1 },
        { 'use_weighted_source_positions': 1, 'include_source_positions': 1,
          'return_object_info': 1, 'include_object_positions': 1 },
        { 'always_use_weighted_source_positions': 1, 'include_source_positions': 1,
          'return_object_info': 1, 'include_object_positions': 1 },
        { 'mjd_now': 60061., 'always_use_weighted_source_positions': 1, 'include_base_procver': 1,
          'include_source_positions': 1, 'include_object_positions': 1, 'return_object_info': 1 },
    ]

    t0 = time.perf_counter()
    tjs = 0
    tpd = 0
    n = 0
    for ltcvreq in pvs_and_obs:
        for which in [ None, 'patch', 'detections', 'forced' ]:
            for extra in extras:
                kwargs = extra.copy()
                if ltcvreq['pv'] is not None:
                    kwargs['processing_version'] = ltcvreq['pv']
                if which is not None:
                    kwargs['which'] = which

                n += 1
                tj0 = time.perf_counter()
                jsres = ltcv.object_ltcv( diaobjectid=ltcvreq['obj'], return_format='json', **kwargs )
                tjs += time.perf_counter() - tj0

                n += 1
                tp0 = time.perf_counter()
                pdres = ltcv.object_ltcv( diaobjectid=ltcvreq['obj'], return_format='pandas', **kwargs )
                tpd += time.perf_counter() - tp0

                if which is None:
                    kwargs['which'] = 'patch'
                if ltcvreq['pv'] is not None:
                    del kwargs['processing_version']

                # Verfify the dict returns are right
                check_ltcv( 'pv2' if ltcvreq['pv'] is None else ltcvreq['pv'],
                            ltcvreq['exproot'], ltcvreq['expobj'], jsres, single=True, **kwargs )

                # Make sure pandas return is consistent
                if isinstance( pdres, tuple ):
                    jsltcv = jsres[0]
                    pdltcv = pdres[0]
                    jsobjinfo = jsres[1]
                    pdobjinfo = pdres[1]
                else:
                    jsltcv = jsres
                    pdltcv = pdres
                    jsobjinfo = None
                    pdobjinfo = None

                compare_pandas_to_json( pdltcv, [jsltcv], pdobjinfo, jsobjinfo )

    FDBLogger.info( f"{n} calls in {time.perf_counter()-t0:.2f} sec; js time={tjs:.2f}, pd time={tpd:.2f}" )


def test_many_object_ltcvs( procver_collection, set_of_lightcurves, lightcurve_checker ):
    # TODO : beef up these tests, think about more edge cases
    roots = set_of_lightcurves
    check_ltcv = lightcurve_checker

    ltcvlist = [
        # Object 1 is not in pv1, so ony expect object 0 back
        ( 'pvc_pv1', [ str(roots[i]['root'].id) for i in [0, 1] ], [0], [100], 'pv1' ),
        ( 'pvc_pv1', [100, 101], [0], [100], 'pv1' ),
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
        { 'mjd_now': 60041. },
        { 'bands': 'r' },
        { 'bands': ['r'] },
        { 'include_source_positions': 1 },
        { 'return_object_info': 1 },
        { 'return_object_info': 1, 'include_object_positions': 1 },
        { 'return_object_info': 1, 'include_base_procver': 1 },
        { 'return_object_info': 1, 'include_base_procver': 1, 'include_object_positions': 1 },
        { 'use_weighted_source_positions': 1, 'include_base_procver': 1 },
        { 'use_weighted_source_positions': 1, 'include_object_positions': 1 },
        { 'use_weighted_source_positions': 1, 'include_object_positions': 1, 'return_object_info': 1 },
        { 'use_weighted_source_positions': 1, 'include_source_positions': 1,
          'return_object_info': 1, 'include_object_positions': 1 },
        { 'always_use_weighted_source_positions': 1, 'include_source_positions': 1,
          'return_object_info': 1, 'include_object_positions': 1 },
        { 'mjd_now': 60061., 'always_use_weighted_source_positions': 1, 'include_base_procver': 1,
          'include_source_positions': 1, 'include_object_positions': 1, 'return_object_info': 1 },
    ]

    t0 = time.perf_counter()
    tjs = 0
    tpd = 0
    n = 0
    with db.DBCon() as dbcon:
        dbcon.echoqueries = True
        dbcon.alwaysexplain = True
        dbcon.alwaysanalyze = True
        for ltcvreq in ltcvlist:
            for which in [ None, 'patch', 'detections', 'forced' ]:
                for extra in extras:
                    kwargs = extra.copy()
                    kwargs['objids'] = ltcvreq[1]
                    if ltcvreq[0] is not None:
                        kwargs['processing_version'] = ltcvreq[0]
                    if which is not None:
                        kwargs['which'] = which

                    n += 1
                    tj0 = time.perf_counter()
                    jsres = ltcv.many_object_ltcvs( return_format='json', dbcon=dbcon, **kwargs )
                    dbcon.rollback()
                    tjs += time.perf_counter() - tj0

                    n += 1
                    tp0 = time.perf_counter()
                    pdres = ltcv.many_object_ltcvs( return_format='pandas', dbcon=dbcon, **kwargs )
                    dbcon.rollback()
                    tpd += time.perf_counter() - tp0

                    del kwargs['objids']
                    if which is None:
                        kwargs['which'] = 'patch'
                    if ltcvreq[0] is not None:
                        del kwargs['processing_version']

                    # Verify the "json" (really, list/dict) return is right
                    check_ltcv( ltcvreq[4], ltcvreq[2], ltcvreq[3], jsres, **kwargs )

                    # Make sure pandas return is consistent
                    if isinstance( pdres, tuple ):
                        jsltcvs = jsres[0]
                        pdltcvs = pdres[0]
                        jsobjinfo = jsres[1]
                        pdobjinfo = pdres[1]
                    else:
                        jsltcvs = jsres
                        pdltcvs = pdres
                        jsobjinfo = None
                        pdobjinfo = None

                    compare_pandas_to_json( pdltcvs, jsltcvs, pdobjinfo, jsobjinfo )

        FDBLogger.info( f"{n} calls, {time.perf_counter()-t0:.2f}s; t_js={tjs:.2f}, t_pd={tpd:.2f}, "
                        f"t_query={dbcon.timings.tot_query_time:.2f}, t_commit={dbcon.timings.tot_commit_time:.2f}, "
                        f"t_fetch={dbcon.timings.tot_fetch_time:.2f}" )


def test_object_search( set_of_lightcurves, objstats_realtime_view, check_search_vs_expected ):

    tests = [ { 'pv': 'pvc_pv2',
                'band': None,
                'roots': [0, 1],
                'conditions': { 'firstdet_mjd_min': 59999, 'firstdet_mjd_max': 60030 }
               },
              { 'pv': 'pvc_pv2',
                'band': None,
                'roots': [1, 2, 3],
                'conditions': { 'lastdet_mjd_min': 60059, 'lastdet_mjd_max': 60081 }
               },
              { 'pv': 'pvc_pv2',
                'band': None,
                'roots': [2, 3],
                'conditions': { 'lastdet_mjd_min': 60059, 'lastdet_mjd_max': 60081,
                                'firstdet_mjd_min': 60039 }
               },
              { 'pv': 'pvc_pv2',
                'band': None,
                'roots': [1, 2],
                'conditions': { 'maxdet_mjd_min': 60034, 'maxdet_mjd_max': 60051 }
               },
              { 'pv': 'pvc_pv2',
                'band': None,
                'roots': [ 1, 2 ],
                'conditions': { 'maxdet_flux_min': np.pow( 10,  (31.4 - 23.1) / 2.5 ) }
               },
              { 'pv': 'pvc_pv2',
                'band': None,
                'roots': [ 1 ],
                'conditions': { 'nsn10_min': 4 }
               },
              { 'pv': 'pvc_pv2',
                'band': None,
                'roots': [ 1, 2 ],
                'conditions': { 'nsn5_min': 2 }
               },
              { 'pv': 'pvc_pv2',
                'band': None,
                'roots': [ 2 ],
                'conditions': { 'nsn5_min': 2, 'nsn5_max': 7 }
               },
              { 'pv': 'pvc_pv2',
                'band': None,
                'roots': [ 1 ],
                'conditions': { 'ndets23_min': 2 }
               },
              { 'pv': 'pvc_pv2',
                'band': None,
                'roots': [ 1, 2 ],
                'conditions': { 'ndets24_min': 5 }
               },
              { 'pv': 'pvc_pv2',
                'band': None,
                'roots': [0, 2, 3 ],
                'conditions': { 'ndets22_max': 0 }
               },
              { 'pv': 'pvc_pv2',
                'band': None,
                'roots': [ 2 ],
                'conditions': { 'ndets24_min': 2, 'ndets22_max': 0  }
               },
              { 'pv': 'pvc_pv2',
                'band': None,
                'roots': [ 1, 2 ],
                'conditions': { 'lastdet_mjd_minus_firstdet_mjd_min': 35 }
               },
              { 'pv': 'pvc_pv2',
                'band': None,
                'roots': [ 0 ],
                'conditions': { 'lastdet_mjd_minus_firstdet_mjd_min': 25,
                                'lastdet_mjd_minus_firstdet_mjd_max': 35 }
               },
              # Probably test other things too.... like bands....
             ]

    made_procvers = { 'realtime' }

    try:
        for test in tests:
            if test['pv'] not in made_procvers:
                with pytest.raises( RuntimeError, match="Can't do object search, materialized view.*doesn't exist" ):
                    results = ltcv.object_search( test['pv'], **(test['conditions']) )

                ltcv.create_object_stats_materialized_view( test['pv'] )
                made_procvers.add( test['pv'] )

            results = ltcv.object_search( test['pv'], **(test['conditions']) )
            check_search_vs_expected( test['pv'], test['roots'], test['band'], results )

    finally:
        with db.DBCon() as con:
            for procver in made_procvers:
                if procver == 'realtime':
                    continue

                con.execute_nofetch( sql.SQL( "DROP MATERIALIZED VIEW {view}" )
                                     .format( view=sql.Identifier( f'objstatscomb_{procver}' ) ) )
                con.execute_nofetch( sql.SQL( "DROP MATERIALIZED VIEW {view}" )
                                     .format( view=sql.Identifier( f'objstats_{procver}' ) ) )

            con.commit()


def test_get_hot_ltcvs( set_of_lightcurves, lightcurve_checker ):
    # ...not sure how to test this without mjd_now since it uses the current time,
    #    and that will be different based on when this is run

    check_ltcv = lightcurve_checker

    ltcvinfo = [
        { 'kwargs': { 'mjd_now': 60056., 'detected_since_mjd': 60035. },
          'passprocver': 'pvc_pv2',
          'testprocver': 'pv2',
          'exproot': [1, 2, 3],
          'expobj': [201, 2011, 202, 203]
         },
        { 'kwargs': { 'mjd_now': 60046., 'detected_since_mjd': 60035., },
          'passprocver': 'pvc_pv2',
          'testprocver': 'pv2',
          'exproot': [1, 2],
          'expobj': [201, 2011, 202],
         },
        { 'kwargs': { 'mjd_now': 60021., 'detected_in_last_days': 2 },
          'passprocver': 'pvc_pv2',
          'testprocver': 'pv2',
          'exproot': [0, 1],
          'expobj': [200, 201, 2011]
         },
        { 'kwargs': { 'mjd_now': 60041., 'detected_in_last_days': 2 },
          'passprocver': 'pvc_pv2',
          'testprocver': 'pv2',
          'exproot': [1, 2],
          'expobj': [201, 2011, 202]
         },
        # detected in last days defaults to 30
        { 'kwargs': { 'mjd_now': 60085. },
          'passprocver': 'pvc_pv2',
          'testprocver': 'pv2',
          'exproot': [1, 2, 3],
          'expobj': [201, 2011, 202, 203]
         },
        { 'kwargs': { 'mjd_now': 60095. },
          'passprocver': 'pvc_pv2',
          'testprocver': 'pv2',
          'exproot': [2],
          'expobj': [202]
         },
        { 'kwargs': { 'mjd_now': 60061. },
          'passprocver': 'realtime',
          'testprocver': 'realtime',
          'exproot': [1, 2],
          'expobj': [1, 2]
         },
    ]

    extras = [
        {},
        { 'include_object_positions': 1 },
        { 'include_object_positions': 0 },
        { 'include_source_positions': 1 },
        { 'include_object_positions': 1, 'include_source_positions': 1 },
        { 'include_base_procver': 1 },
        { 'include_base_procver': 1, 'include_object_positions': 1 },
        { 'use_weighted_source_positions': 1, 'include_object_positions': 1, 'include_base_procver': 1 },
        { 'always_use_weighted_source_positions': 1, 'include_object_positions': 1, 'include_base_procver': 1 },
        { 'always_use_weighted_source_positions': 1, 'include_object_positions': 1 },
    ]

    n = 0
    t0 = time.perf_counter()
    tjs = 0
    tpd = 0
    for lc in ltcvinfo:
        for source_patch in [ True, False, None ]:
            for extra in extras:
                kwargs = extra.copy()
                kwargs.update( lc['kwargs'] )

                if lc['passprocver'] is not None:
                    kwargs['processing_version'] = lc['passprocver']
                if source_patch is not None:
                    kwargs['source_patch'] = source_patch

                n += 1
                tj0 = time.perf_counter()
                jsres = ltcv.get_hot_ltcvs( return_format='json', **kwargs )
                tjs += time.perf_counter() - tj0

                n += 1
                tp0 = time.perf_counter()
                pdres = ltcv.get_hot_ltcvs( return_format='pandas', **kwargs )
                tpd += time.perf_counter() - tp0

                for yank in [ 'processing_version', 'source_patch', 'detected_since_mjd', 'detected_in_last_days' ]:
                    if yank in kwargs:
                        del kwargs[yank]
                kwargs['which'] = 'patch' if source_patch in ( True, None ) else 'forced'
                if 'include_object_positions' not in kwargs:
                    # get_hot_ltcvs has a different default from many_object_ltcvs
                    kwargs['include_object_positions'] = True
                check_ltcv( lc['testprocver'], lc['exproot'], lc['expobj'], jsres,
                            return_object_info=True, **kwargs )

                compare_pandas_to_json( pdres[0], jsres[0], pdres[1], jsres[1] )

    FDBLogger.info( f"{n} calls in {time.perf_counter()-t0:.2f} sec; js time={tjs:.2f}, pd time={tpd:.2f}" )
