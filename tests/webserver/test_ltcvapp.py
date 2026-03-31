import pytest
import time
import itertools

import numpy as np

import db
from util import FDBLogger


def _check_ltcv_res( procver, expected_roots, expected_diaobjectids, res, single=False,
                     mjd_now=None, bands=None, which='patch', return_object_info=False, include_object_positions=False,
                     include_base_procver=False, include_source_ids=False, include_source_positions=False,
                     use_weighted_source_positions=False, always_use_weighted_source_positions=False,
                     procver_collection=None, set_of_lightcurves=None,
                     expect_all_roots=True ):
    # This has a lot of redundancy with test_ltcv.py::compare_ltcv_to_expected
    bpvs, pvs, pvinfo = procver_collection
    roots = set_of_lightcurves
    assert roots is not None

    which = 'patch' if which is None else which
    pvrow = [ p for p in pvinfo if ( ( p['procver'].description == procver ) or
                                     ( procver in pvs and p['procver'].description == pvs[procver].description ) or
                                     ( isinstance(procver, db.ProcessingVersion) and procver.id==p['procver'].id ) ) ]
    assert len(pvrow) == 1
    pvrow = pvrow[0]

    expected_root_ids = [ roots[i]['root'].id for i in expected_roots ]
    use_weighted_source_positions = use_weighted_source_positions or always_use_weighted_source_positions

    if return_object_info:
        assert isinstance( res, dict )
        if single:
            assert set( res.keys() ) == { 'ltcv', 'objinfo' }
            assert isinstance( res['ltcv'], dict )
            assert isinstance( res['objinfo'], dict )
            ltcvs = [ res['ltcv'] ]
        else:
            assert set( res.keys() ) == { 'ltcvs', 'objinfo' }
            assert isinstance( res['ltcvs'], list )
            assert isinstance( res['objinfo'], dict )
            ltcvs = res['ltcvs']
        infos = res['objinfo']
    else:
        if single:
            assert isinstance( res, dict )
            ltcvs = [ res ]
        else:
            assert isinstance( res, list )
            ltcvs = res
        infos = None

    expected_keys = [ 'rootid', 'mjd', 'diasourceid', 'source_diaobjectid',
                      'visit', 'band', 'flux', 'fluxerr', 'isdet' ]
    if which != 'detections':
        expected_keys.extend( [ 'diaforcedsourceid', 'forced_diaobjectid' ] )
    if which == 'patch':
        expected_keys.append( 'ispatch' )
    if include_base_procver:
        expected_keys.append( 'base_procver_s' )
        if which != 'detections':
            expected_keys.append( 'base_procver_f' )
    if include_source_positions:
        expected_keys.extend( [ 'det_ra', 'det_dec', 'det_raerr', 'det_decerr', 'det_ra_dec_cov' ] )

    assert isinstance( ltcvs, list )
    assert all( set( lc.keys() ) == set( expected_keys ) for lc in ltcvs )

    expected_keys.remove( 'rootid' )

    if expect_all_roots:
        assert len( ltcvs ) == len( expected_roots )
        assert set( lc['rootid'] for lc in ltcvs ) == set( str(r) for r in expected_root_ids )
    else:
        assert len( set( lc['rootid'] for lc in ltcvs ) ) == len( lc['rootid'] for lc in ltcvs )
        assert set( lc['rootid'] for lc in ltcvs ).issubset( set( str(r) for r in expected_root_ids ) )


    datacache = {}
    for rootid in expected_root_ids:
        rdex = None
        for i, root in enumerate( roots ):
            if root['root'].id == rootid:
                rdex = i
                break
        if rdex is None:
            raise ValueError( "Failed to find root object {rootid}" )

        dex = [ lc['rootid'] for lc in ltcvs ].index( str(rootid) )
        thisltcv = ltcvs[dex]

        assert all( len(thisltcv[k]) == len(thisltcv['mjd']) for k in expected_keys )

        # OK, here's the deal.  For each rootid, there are multiple base processing versions that have
        #   data.  We need to extract the highest priority for each.  To do this, rewrangle the
        #   set_of_lightcurves data so that its indexed by base_procver, in descending priority order.

        if mjd_now is not None:
            if bands is not None:
                conds = lambda s: s.midpointmjdtai <= mjd_now and s.band in bands
            else:
                conds = lambda s: s.midpointmjdtai <= mjd_now
        elif bands is not None:
            conds = lambda s: s.band in bands
        else:
            conds = lambda s: True

        src_bpvkeys = [ p[2] for p in pvrow['diasource'] ]
        frc_bpvkeys = [ p[2] for p in pvrow['diaforcedsource'] ]
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

            for bpvtuple in pvrow['diasource']:
                bpv, _prio, bpvkey = bpvtuple
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

            for bpvtuple in pvrow['diaforcedsource']:
                bpv, _prio, bpvkey = bpvtuple
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

        if which == 'detections':
            # If which is detections, throw out only forced sources
            data = [ d for d in data if d['src'] is not None ]
        elif which == 'forced':
            # If which is forced, through out detections for which we don't have forced
            data = [ d for d in data if d['frc'] is not None ]

        # Save this as it may be used again below
        datacache[rootid] = data

        # OK, now we have wrangled things to the point where they can be compared

        assert len( data ) == len( thisltcv['mjd'] )
        if which == 'detections':
            assert all( flux == pytest.approx( d['src'].psfflux, rel=1e-6 )
                        for flux, d in zip( thisltcv['flux'], data ) )
            assert all( flux == pytest.approx( d['src'].psffluxerr, rel=1e-6 )
                        for flux, d in zip( thisltcv['fluxerr'], data ) )
            assert all( bool(i) for i in thisltcv['isdet'] )
            assert all( v == d['src'].visit for v, d in zip( thisltcv['visit'], data ) )
            assert all( b == d['src'].band for b, d in zip( thisltcv['band'], data ) )
            assert all( o == d['src'].diaobjectid for o, d in zip( thisltcv['source_diaobjectid'], data ) )
        elif which =='forced':
            assert all( flux == pytest.approx( d['frc'].psfflux, rel=1e-6 )
                        for flux, d in zip( thisltcv['flux'], data ) )
            assert all( flux == pytest.approx( d['frc'].psffluxerr, rel=1e-6 )
                        for flux, d in zip( thisltcv['fluxerr'], data ) )
            assert all( bool(i) == ( d['src'] is not None ) for i, d in zip( thisltcv['isdet'], data ) )
            assert all( v == d['frc'].visit for v, d in zip( thisltcv['visit'], data ) )
            assert all( b == d['frc'].band for b, d in zip( thisltcv['band'], data ) )
            assert all( o == ( d['src'].diaobjectid if d['src'] is not None else None )
                        for o, d in zip( thisltcv['source_diaobjectid'], data ) )
            assert all( o == d['frc'].diaobjectid for o, d in zip( thisltcv['forced_diaobjectid'], data ) )
        else:
            assert all( flux == pytest.approx( ( d['frc'].psfflux if d['frc'] is not None
                                                 else d['src'].psfflux ),
                                               rel=1e-6 )
                        for flux, d in zip( thisltcv['flux'], data ) )
            assert all( flux == pytest.approx( ( d['frc'].psffluxerr if d['frc'] is not None
                                                 else d['src'].psffluxerr ),
                                               rel=1e-6 )
                        for flux, d in zip( thisltcv['fluxerr'], data ) )
            assert all( bool(i) == ( d['src'] is not None ) for i, d in zip( thisltcv['isdet'], data ) )
            assert all( bool(i) == ( d['frc'] is None ) for i, d in zip( thisltcv['ispatch'], data ) )
            assert all( v == ( d['frc'].visit if d['frc'] is not None else d['src'].visit )
                        for v, d in zip( thisltcv['visit'], data ) )
            assert all( b == ( d['frc'].band if d['frc'] is not None else d['src'].band )
                        for b, d in zip( thisltcv['band'], data ) )
            assert all( o == ( d['src'].diaobjectid if d['src'] is not None else None )
                        for o, d in zip( thisltcv['source_diaobjectid'], data ) )
            assert all( o == ( d['frc'].diaobjectid if d['frc'] is not None else None )
                        for o, d in zip( thisltcv['forced_diaobjectid'], data ) )

        if include_base_procver:
            assert all( v == ( d['src_bpv'].description if d['src_bpv'] is not None else None )
                        for v, d in zip( thisltcv['base_procver_s'], data ) )
            if which != 'detections':
                assert all( v == ( d['frc_bpv'].description if d['frc_bpv'] is not None else None )
                            for v, d in zip( thisltcv['base_procver_f'], data ) )

        if include_source_positions:
            assert all( v == ( pytest.approx( d['src'].ra, rel=1e-12 ) if d['src'] is not None else None )
                        for v, d in zip( thisltcv['det_ra'], data ) )
            assert all( v == ( pytest.approx( d['src'].dec, rel=1e-12 ) if d['src'] is not None else None )
                        for v, d in zip( thisltcv['det_dec'], data ) )
            assert all( v == ( pytest.approx( d['src'].raerr, rel=1e-6 ) if d['src'] is not None else None )
                        for v, d in zip( thisltcv['det_raerr'], data ) )
            assert all( v == ( pytest.approx( d['src'].decerr, rel=1e-6 ) if d['src'] is not None else None )
                        for v, d in zip( thisltcv['det_decerr'], data ) )
            assert all( v== ( pytest.approx( d['src'].ra_dec_cov, abs=0.0001/3600. ) if d['src'] is not None else None )
                        for v, d in zip( thisltcv['det_ra_dec_cov'], data ) )

    if infos is not None:

        expected_obj_keys = { 'diaobjectid', 'rootid' }
        if include_base_procver:
            expected_obj_keys.add( 'obj_base_procver' )
        if include_object_positions:
            expected_obj_keys = expected_obj_keys.union( { 'ra', 'dec', 'raerr', 'decerr', 'ra_dec_cov' } )
            if include_base_procver:
                expected_obj_keys.add( 'pos_base_procver' )

        assert set( infos.keys() ) == expected_obj_keys

        assert set( infos['rootid'] ) == set( str(e) for e in expected_root_ids )
        assert len( infos['diaobjectid'] ) == len( expected_diaobjectids )
        assert set( infos['diaobjectid'] ) == set( expected_diaobjectids )

        for r in expected_roots:

            # These next two are a bit gratuitous (since they aren't
            #   used again), but useful for pdb debugging.
            dex = [ lc['rootid'] for lc in ltcvs ].index( str(roots[r]['root'].id) )
            thisltcv = ltcvs[dex]

            for diaobjectid in expected_diaobjectids:
                if diaobjectid not in roots[r]['obj'].keys():
                    continue
                diaobject = roots[r]['obj'][diaobjectid]
                dex = infos['diaobjectid'].index( diaobjectid )
                assert infos['rootid'][dex] == str( diaobject.rootid )
                if include_base_procver:
                    # TODO : make sure the *right* processing version was returned!
                    # ...actually... the fixtures only have objects in a single base processing
                    # version for each processing version, so that wouldn't be an interesting test right now.
                    bpv = [ b[0] for b in pvrow['diaobject'] if b[0].description==infos['obj_base_procver'][dex] ]
                    assert len(bpv) == 1
                    bpv = bpv[0]
                    assert diaobject.base_procver_id == bpv.id

                if include_object_positions:
                    pos = None

                    if always_use_weighted_source_positions:
                        pos = None
                        if include_base_procver and return_object_info:
                            assert infos['pos_base_procver'][dex] is None

                    else:
                        # There are multiple positions, so make sure we got the highest priority one that exists
                        #
                        # OMG this is becoming such an ugly hack of how I store data, this is what
                        #   happens when you just need to get stuff done fast and can't go back
                        #   and refactor.
                        pos = None
                        posbpv = None
                        for proposedposbpv, prio, bpvkey in pvrow['diaobject_position']:
                            if (diaobjectid, bpvkey) in roots[r]['pos'].keys():
                                pos = roots[r]['pos'][ (diaobjectid, bpvkey) ]
                                posbpv = proposedposbpv
                                break

                        if include_base_procver:
                            if ( pos is None ) or always_use_weighted_source_positions:
                                assert infos['pos_base_procver'][dex] is None
                            else:
                                assert infos['pos_base_procver'][dex] == posbpv.description

                        if pos is not None:
                            assert infos['ra'][dex] == pytest.approx( pos.ra, rel=1e-12 )
                            assert infos['dec'][dex] == pytest.approx( pos.dec, rel=1e-12 )
                            assert infos['raerr'][dex] == pytest.approx( pos.raerr, rel=1e-6 )
                            assert infos['decerr'][dex] == pytest.approx( pos.decerr, rel=1e-6 )
                            # Fixture didn't put any correlations in but for random variancer
                            assert infos['ra_dec_cov'][dex] == pytest.approx( pos.ra_dec_cov, abs=0.0001/3600. )

                    if use_weighted_source_positions:
                        justsrcs = [ d for d in datacache[roots[r]['root'].id] if d['src'] is not None ]
                        srcra = np.array( [ j['src'].ra for j in justsrcs ] )
                        srcdec = np.array( [ j['src'].dec for j in justsrcs ] )
                        if which == 'detections':
                            sn = np.array( [ j['src'].psfflux / j['src'].psffluxerr for j in justsrcs ] )
                        else:
                            # ... this may be wholly gratuitous because I don't think
                            #     the fixtures set a different forced flux from the source flux
                            # However, this is the equivalent logic server-side.
                            sn = np.array( [ ( j['frc'].psfflux / j['frc'].psffluxerr )
                                             if j['frc'] is not None
                                             else ( j['src'].psfflux / j['src'].psffluxerr )
                                             for j in justsrcs ] )
                        w = np.where( sn > 3 )[0]
                        srcra = srcra[w]
                        srcdec = srcdec[w]
                        weight = sn[w] ** 2
                        meanra = ( srcra * weight ).sum() / ( weight.sum() )
                        meandec = ( srcdec * weight ).sum() / ( weight.sum() )
                        raerr = np.sqrt( ( weight * ( srcra - meanra )**2 ).sum() / weight.sum() )
                        decerr = np.sqrt( ( weight * ( srcdec - meandec )**2 ).sum() / weight.sum() )
                        ra_dec_cov = ( weight * ( srcra - meanra ) * ( srcdec - meandec ) ).sum() / weight.sum()

                        if ( pos is None ) or always_use_weighted_source_positions:
                            if pos is not None:
                                # In this case, the position should *not* match the diaobject_position value too closely
                                assert not infos['ra'][dex] != pytest.approx( pos.ra, abs=0.01/3600. )
                                assert not infos['dec'][dex] != pytest.approx( pos.dec, abs=0.01/3600. )
                            # But should be good within numerical precision to the calculated positions (modulo
                            # order of operations and floating roundoff)
                            # ....*if* there were enough sources to calculate a position from!
                            if len(srcra) > 0:
                                # THIS NEEDS TO BE FIXED
                                # The fact that it's happening only in realtime / forced really
                                #   suggests to me that it's not a string float roundoff thing
                                #   and that there's some weird bug in my code somewhere.
                                # (And, honestly, it might be in the test fixture....)
                                # if ( procver == 'realtime' ) and ( which == 'forced' ):
                                if False:
                                    assert infos['ra'][dex] == pytest.approx( meanra, rel=3e-7 )
                                    assert infos['dec'][dex] == pytest.approx( meandec, rel=3e-7 )
                                    assert infos['raerr'][dex] == pytest.approx( raerr, rel=3e-3 )
                                    assert infos['decerr'][dex] == pytest.approx( decerr, rel=3e-3 )
                                    # Fixture didn't put any correlations in
                                    assert infos['ra_dec_cov'][dex] == pytest.approx( ra_dec_cov, abs=0.0001/3600. )
                                else:
                                    assert infos['ra'][dex] == pytest.approx( meanra, rel=1e-12 )
                                    assert infos['dec'][dex] == pytest.approx( meandec, rel=1e-12 )
                                    assert infos['raerr'][dex] == pytest.approx( raerr, rel=1e-6 )
                                    assert infos['decerr'][dex] == pytest.approx( decerr, rel=1e-6 )
                                    # Fixture didn't put any correlations in
                                    assert infos['ra_dec_cov'][dex] == pytest.approx( ra_dec_cov, abs=0.0001/3600. )
                            else:
                                assert all( infos[i][dex] is None
                                            for i in ( 'ra', 'dec', 'raerr', 'decerr', 'ra_dec_cov' ) )


def test_getmanyltcvs( test_user, fastdb_client, set_of_lightcurves, procver_collection ):
    roots = set_of_lightcurves
    pvc = procver_collection


    ltcvlist = [
        # Object 1 is not in pv1, so ony expect object 0 back
        ( 'pvc_pv1', [ str(roots[i]['root'].id) for i in [0, 1] ], [0], [100], 'pv1' ),
        # pvc_pv2 should be the default
        ( None, [ str(roots[i]['root'].id) for i in [0, 2] ], [0, 2], [200, 202], 'pv2' ),
        # If we ask for diaobjects that are in the wrong processing version, we still get
        #   back the corresponding ones from the sources in this processing version
        ( 'pvc_pv2', [0, 2], [ 0, 2], [200, 202], 'pv2' ),
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
    first = True
    for ltcvreq in ltcvlist:
        for which in [ None, 'patch', 'detections', 'forced' ]:
            for extra in extras:
                if ltcvreq[0] is None:
                    url = '/ltcv/getmanyltcvs'
                else:
                    url = f'/ltcv/getmanyltcvs/{ltcvreq[0]}'

                if first:
                    first = False
                    # Just check the missing objids call once
                    for foo in [ extra, [], 'kitten' ]:
                        with pytest.raises( RuntimeError, match=( "^Error response from server, status 422: "
                                                                  "Must pass POST data as a json dict with at least "
                                                                  "objids as a key" ) ):
                            n += 1
                            fastdb_client.post( url, json=foo )

                    # Just check the bad paramter call once
                    with pytest.raises( RuntimeError, match=( "^Error response from server, status 422: "
                                                              "Unknown data parameters: {'foo'}" ) ):
                        n += 1
                        fastdb_client.post( url, json={'objids': ltcvreq[1], 'foo': 'bar'} )

                kwargs = extra.copy()
                kwargs['objids'] = ltcvreq[1]
                if which is not None:
                    kwargs['which'] = which
                n += 1
                res = fastdb_client.post( url, json=kwargs )
                # ****
                if ( ( extra.get('mjd_now', None) == 60061 ) and ( ltcvreq[0] == 'realtime' ) and
                     ( which in [ 'patch', 'forced' ] ) ):
                    import pdb; pdb.set_trace()
                # ****
                if which is None:
                    kwargs['which'] = 'patch'
                del kwargs['objids']
                _check_ltcv_res( ltcvreq[4], ltcvreq[2], ltcvreq[3], res,
                                 set_of_lightcurves=roots, procver_collection=pvc,
                                 **kwargs )
                pass


    FDBLogger.info( f"{n} requests in {time.perf_counter()-t0:.2f} sec." )



def test_getltcv( test_user, fastdb_client, set_of_lightcurves, procver_collection ):
    roots = set_of_lightcurves
    pvc = procver_collection

    # Each element of the list is:
    #  * processing version
    #  * object to ask for
    #  * expected root ids returned
    #  * expected diaobjects returned

    ltcvlist = [ ( None, 202, [2], [202], 'pv2' ),
                 ( None, roots[2]['root'].id, [2], [202], 'pv2' ),
                 ( 'pvc_pv1', 100, [0], [100] ),
                 ( 'pvc_pv1', roots[0]['root'].id, [0], [100] ),
                 ( 'pvc_pv1', 1, None, None ),
                 ( 'pvc_pv2', roots[1]['root'].id, [1], [201, 2011] ),
                 ( 'pvc_pv2', 1, [1], [201, 2011] ),
                 ( 'pvc_pv2', 201, [1], [201, 2011] ),
                ]

    extras = [ {},
               { 'mjd_now': 60040. },
               { 'include_source_positions': 1 },
               { 'include_base_procver': 1 },
               { 'return_object_info': 1, 'include_base_procver': 1 },
               { 'include_source_positions': 1, 'include_base_procver': 1 },
               { 'include_source_positions': 1, 'include_base_procver': 1, 'return_object_info': 1 },
               { 'include_source_positions': 1, 'include_base_procver': 1, 'return_object_info': 1,
                 'use_weighted_source_positions': 0, 'always_use_weighted_source_positions': 1 },
               { 'use_weighted_source_positions': 1, 'include_base_procver': 1,
                 'return_object_info': 1, 'include_object_positions': 1, 'manual_check': 1, },
               { 'always_use_weighted_source_positions': 1, 'include_base_procver': 1,
                 'return_object_info': 1 , 'include_object_positions': 1, 'manual_check': 2 }
              ]


    n = 0
    t0 = time.perf_counter()
    first = True
    firstforreq = set()
    for ltcvreq in ltcvlist:
        for which in [ 'detections', 'forced', 'patch', None ]:
            for extra in extras:
                # So we can mung it
                extra = extra.copy()

                manual = None
                if 'manual_check' in extra:
                    manual = extra['manual_check']
                    del extra['manual_check']
                if which is not None:
                    extra['which'] = which

                if ltcvreq[0] is None:
                    url = f'/ltcv/getltcv/{ltcvreq[1]}'
                    pv = ltcvreq[4]
                else:
                    url = f'/ltcv/getltcv/{ltcvreq[0]}/{ltcvreq[1]}'
                    pv = ltcvreq[0]

                if first:
                    first = False
                    # ...we don't really need to test this 320 times...
                    with pytest.raises( RuntimeError, match=( "^Error response from server, status 422: "
                                                              "Unknown data parameters: {'foo'}" ) ):
                        n += 1
                        fastdb_client.post( url, json={'foo': 'bar'} )

                if ltcvreq[2] is None:
                    # ...and we don't have to test this with every set of options...
                    if ltcvreq not in firstforreq:
                        firstforreq.add( ltcvreq )
                        with pytest.raises( RuntimeError, match=( f"^Error response from server, status 422: "
                                                                  f"Could not find lightcurve for {ltcvreq[1]} "
                                                                  f"in processing version {ltcvreq[0]}" ) ):
                            n += 1
                            res = fastdb_client.post( url, json=extra )
                else:
                    n += 1
                    res = fastdb_client.post( url, json=extra )

                    if ( manual == 1 ) and ( pv == 'pvc_pv2' ):
                        for dex in range( len(res['objinfo']['diaobjectid']) ):
                            if res['objinfo']['diaobjectid'][dex] == 201:
                                # use_weighted_source_position won't be necessary for
                                #   object 201 in pv 2
                                assert res['objinfo']['pos_base_procver'][dex] is not None
                            elif res['objinfo']['diaobjectid'][dex] == 2011:
                                # But, object 2011 has no positions stored
                                assert res['objinfo']['pos_base_procver'][dex] is None
                    elif manual == 2:
                        assert len( res['objinfo']['diaobjectid'] ) == len( ltcvreq[3] )
                        # In this case, every object should have a weighted source position
                        assert all( pbv is None for pbv in res['objinfo']['pos_base_procver'] )

                    _check_ltcv_res( pv, ltcvreq[2], ltcvreq[3], res, single=True, **extra,
                                     set_of_lightcurves=roots, procver_collection=pvc )

    FDBLogger.info( f"Sent {n} requests in {time.perf_counter()-t0:.2f} sec." )



# TODO : test getrandomltcv ; that might require the ability to pass a random seed for a reproducible test.
@pytest.mark.skip( reason="This test needs to be written" )
def test_getrandomltcv( test_user, fastdb_client, procver_collection, set_of_lightcurves ):
    assert False


def test_gethottransients( test_user, fastdb_client, procver_collection, set_of_lightcurves ):
    roots = set_of_lightcurves
    pvc = procver_collection

    ltcvinfo = [ { 'kwargs': { 'mjd_now': 60056., 'detected_since_mjd': 60035. },
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
                 { 'kwargs': { 'mjd_now': 60061. },
                   'passprocver': None,
                   'testprocver': 'realtime',
                   'exproot': [1, 2],
                   'expobj': [1, 2]
                  }
                ]

    extras = [ {},
               { 'include_object_positions': 1 },
               { 'include_object_positions': 0 },
               { 'include_source_positions': 1 },
               { 'include_object_positions': 1, 'include_source_positions': 1 },
               { 'include_base_procver': 1 },
               { 'include_base_procver': 1, 'include_object_positions': 1 },
               { 'use_weighted_source_positions': 1, 'include_object_positions': 1 },
               { 'always_use_weighted_source_positions': 1, 'include_object_positions': 1, 'include_base_procver': 1 },
               { 'always_use_weighted_source_positions': 1, 'include_object_positions': 1 },
              ]

    for lc in ltcvinfo:
        for source_patch in [ True, False, None ]:
            for extra in extras:

                if lc['passprocver'] is None:
                    url = '/ltcv/gethottransients'
                else:
                    url = f'/ltcv/gethottransients/{lc["passprocver"]}'

                kwargs = extra.copy()
                kwargs.update( lc['kwargs'] )
                if source_patch is not None:
                    kwargs['source_patch'] = source_patch

                res = fastdb_client.post( url, json=kwargs )

                for yank in [ 'source_patch', 'detected_since_mjd', 'detected_in_last_days' ]:
                    if yank in kwargs:
                        del kwargs[yank]
                kwargs['which'] = 'patch' if source_patch in ( True, None ) else 'forced'
                if 'include_object_positions' not in kwargs:
                    # get_hot_ltcvs has a different default from many_object_ltcvs
                    kwargs['include_object_positions'] = True
                _check_ltcv_res( lc['testprocver'], lc['exproot'], lc['expobj'], res,
                                 set_of_lightcurves=roots, procver_collection=pvc,
                                 return_object_info=True, **kwargs )


    # # This tests gets the same information as ../test_ltcv.py, only via
    # # the webap.  ../test_ltcv.py::test_get_hot_ltcvs makes sure that
    # # the direct call to ltcv.get_hot_ltcvs returns the right stuff.
    # # (Or, at least, it should.)  This test makes sure that what you get
    # # from the webap matches what you get from a direct call.

    # def _compare_direct_to_webap( df, objdf, res ):
    #     assert ( df.index.get_level_values('diaobjectid').unique()
    #              == np.array( [ r['diaobjectid'] for r in res ] ) ).all()
    #     assert ( objdf.index.get_level_values('diaobjectid').unique()
    #              == np.array( [ r['diaobjectid'] for r in res ] ) ).all()
    #     for objrow in res:
    #         objid = objrow['diaobjectid']
    #         subdf = df.loc[ objid ]
    #         subobjdf = objdf.loc[ objid ]
    #         assert objrow['rootid'] == str( subobjdf.rootid )
    #         assert objrow['ra'] == subobjdf.ra
    #         assert objrow['dec'] == subobjdf.dec
    #         assert objrow['zp'] == 31.4
    #         assert len(subdf) == len( objrow['photometry']['mjd'] )
    #         assert ( subdf.index.values == np.array( objrow['photometry']['mjd'] ) ).all()
    #         assert ( subdf.band == np.array( objrow['photometry']['band'] ) ).all()
    #         assert ( subdf.visit == np.array( objrow['photometry']['visit'] ) ).all()
    #         assert ( subdf.flux == np.array( objrow['photometry']['flux'] ) ) .all()
    #         assert ( subdf.fluxerr == np.array( objrow['photometry']['fluxerr'] ) ).all()
    #         assert ( subdf.isdet == np.array( objrow['photometry']['isdet'] ) ).all()
    #         if 'ispatch' in subdf.columns:
    #             assert ( subdf.ispatch == np.array( objrow['photometry']['ispatch'] ) ).all()

    # df, objdf, _ = ltcv.get_hot_ltcvs( 'pvc_pv3', detected_since_mjd=60035, mjd_now=60056 )
    # res = fastdb_client.post( '/ltcv/gethottransients', json={ 'processing_version': 'pvc_pv3',
    #                                                            'detected_since_mjd': 60035,
    #                                                            'mjd_now': 60056 } )
    # _compare_direct_to_webap( df, objdf, res )

    # df, objdf, _ = ltcv.get_hot_ltcvs( 'pvc_pv3', detected_since_mjd=60035, mjd_now=60046 )
    # res = fastdb_client.post( '/ltcv/gethottransients', json={ 'processing_version': 'pvc_pv3',
    #                                                            'detected_since_mjd': 60035,
    #                                                            'mjd_now': 60046 } )
    # _compare_direct_to_webap( df, objdf, res )

    # df, objdf, _ = ltcv.get_hot_ltcvs( 'pvc_pv3', detected_in_last_days=2, mjd_now=60021 )
    # res = fastdb_client.post( '/ltcv/gethottransients', json={ 'processing_version': 'pvc_pv3',
    #                                                            'detected_in_last_days': 2,
    #                                                            'mjd_now': 60021 } )
    # _compare_direct_to_webap( df, objdf, res )

    # df, objdf, _ = ltcv.get_hot_ltcvs( 'pvc_pv3', detected_in_last_days=2, mjd_now=60041 )
    # res = fastdb_client.post( '/ltcv/gethottransients', json={ 'processing_version': 'pvc_pv3',
    #                                                            'detected_in_last_days': 2,
    #                                                            'mjd_now': 60041 } )
    # _compare_direct_to_webap( df, objdf, res )

    # # detected_in_last_days defaults to 30
    # df, objdf, _ = ltcv.get_hot_ltcvs( 'pvc_pv3', mjd_now=60085 )
    # res = fastdb_client.post( '/ltcv/gethottransients', json={ 'processing_version': 'pvc_pv3',
    #                                                            'mjd_now': 60085 } )
    # _compare_direct_to_webap( df, objdf, res )
    # df, objdf, _ = ltcv.get_hot_ltcvs( 'pvc_pv3', mjd_now=60095 )
    # res = fastdb_client.post( '/ltcv/gethottransients', json={ 'processing_version': 'pvc_pv3',
    #                                                            'mjd_now': 60095 } )
    # _compare_direct_to_webap( df, objdf, res )

    # # Test source patch.  Gotta use pvc_pv1 for this.

    # df, objdf, _ = ltcv.get_hot_ltcvs( 'pvc_pv1', mjd_now=60031 )
    # res = fastdb_client.post( '/ltcv/gethottransients', json={ 'processing_version': 'pvc_pv1',
    #                                                            'mjd_now': 60031 } )
    # _compare_direct_to_webap( df, objdf, res )

    # df, objdf, _ = ltcv.get_hot_ltcvs( 'pvc_pv1', mjd_now=60031, source_patch=True )
    # res = fastdb_client.post( '/ltcv/gethottransients', json={ 'processing_version': 'pvc_pv1',
    #                                                            'mjd_now': 60031,
    #                                                            'source_patch': True } )
    # _compare_direct_to_webap( df, objdf, res )
