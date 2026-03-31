import pytest
import itertools

import numpy as np

import db
import ltcv


def _check_ltcv_res( procver, expected_roots, expected_diaobjectids, res,
                     mjdnow=None, bands=None, which='patch', return_object_info=False,
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
        ltcvs = res['ltcvs']
        infos = res['objinfo']
        if not include_base_procver:
            raise RuntimeError( '_check_ltcv_res is broken when include_base_procver is False '
                                'and return_object_info is True' )
    else:
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

        if mjdnow is not None:
            if bands is not None:
                conds = lambda s: s.midpointmjdtai <= mjdnow and s.band in bands
            else:
                conds = lambda s: s.midpointmjdtai <= mjdnow
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

        assert set( infos['rootid'] ) == set( str(e) for e in expected_root_ids )
        # We know that there's only one diaobject per processing version, so we don't have
        #   to go through all the pain we'll go through with sources below
        assert len( infos['diaobjectid'] ) == len( expected_diaobjectids )
        assert set( infos['diaobjectid'] ) == set( expected_diaobjectids )

        for r in expected_roots:
            for diaobjectid in expected_diaobjectids:
                if diaobjectid not in roots[r]['obj'].keys():
                    continue
                diaobject = roots[r]['obj'][diaobjectid]
                dex = infos['diaobjectid'].index( diaobjectid )
                assert infos['rootid'][dex] == str( diaobject.rootid )
                bpv = [ b[0] for b in pvrow['diaobject'] if b[0].description==infos['obj_base_procver'][dex] ]
                assert len(bpv) == 1
                bpv = bpv[0]
                assert diaobject.base_procver_id == bpv.id

                # There *are* multiple positions, so make sure we got the highest priority one that exists
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

                if always_use_weighted_source_positions:
                    assert infos['pos_base_procver'][dex] is None

                else:
                    if pos is None:
                        assert infos['pos_base_procver'][dex] is None
                    else:
                        if use_weighted_source_positions:
                            assert infos['pos_base_procver'][dex] in ( posbpv.description, None )
                        else:
                            assert infos['pos_base_procver'][dex] == posbpv.description

                        if infos['pos_base_procver'] is not None:
                            assert infos['ra'][dex] == pytest.approx( pos.ra, rel=1e-12 )
                            assert infos['dec'][dex] == pytest.approx( pos.dec, rel=1e-12 )
                            assert infos['raerr'][dex] == pytest.approx( pos.raerr, rel=1e-6 )
                            assert infos['decerr'][dex] == pytest.approx( pos.decerr, rel=1e-6 )
                            # Fixture didn't put any correlations in but for random variancer
                            assert infos['ra_dec_cov'][dex] == pytest.approx( pos.ra_dec_cov, abs=0.0001/3600. )

                if ( use_weighted_source_positions ) and ( infos['pos_base_procver'][dex] ) is None:
                    justsrcs = [ d['src'] for d in datacache[r] if d['src'] is not None ]
                    srcra = np.array( j.ra for j in justsrcs )
                    srcdec = np.array( j.dec for j in justsrcs )
                    sn = np.array( j.psfflux / j.psffluxerr for j in justsrcs )
                    w = np.where( sn > 3 )[0]
                    srcra = srcra[w]
                    srcdec = srcdec[w]
                    weight = sn[w] ** 2
                    meanra = ( srcra * weight ).sum() / ( weight.sum() )
                    meandec = ( srcdec * weight.sum() ) / ( weight.sum() )
                    raerr = np.sqrt( ( weight * ( srcra - meanra )**2 ).sum() / weight.sum() )
                    decerr = np.sqrt( ( weight * ( srcdec - meandec )**2 ).sum() / weight.sum() )
                    ra_dec_cov = ( weight * ( srcra - meanra ) * ( srcdec - meandec ) ).sum() / weight.sum()

                    # In this case, the position should *not* match the diaobject_position value too closely
                    assert not infos['ra'][dex] != pytest.approx( pos.ra, abs=0.01/3600. )
                    assert not infos['dec'][dex] != pytest.approx( pos.dec, abs=0.01/3600. )

                    # But should be good within numerical precision to the calculated positions (modulo
                    # order of operations and floating roundoff)
                    assert infos['ra'][dex] == pytest.approx( meanra, rel=1e-12 )
                    assert infos['dec'][dex] == pytest.approx( meandec, rel=1e-12 )
                    assert infos['raerr'][dex] == pytest.approx( raerr, rel=1e-12 )
                    assert infos['decerr'][dex] == pytest.approx( decerr, rel=1e-12 )
                    # Fixture didn't put any correlations in
                    assert infos['ra_dec_cov'][dex] == pytest.approx( ra_dec_cov, abs=0.0001/3600. )


def test_getmanyltcvs( test_user, fastdb_client, set_of_lightcurves, procver_collection ):
    roots = set_of_lightcurves
    pvc = procver_collection

    # Object 1 is not in pv1, so ony expect object 0 back
    for which in [ 'detections', 'forced', 'patch', None ]:
        postdata = { "objids": [ str(roots[i]['root'].id) for i in [0, 1] ] }
        if which is not None:
            postdata['which'] = which
        res = fastdb_client.post( '/ltcv/getmanyltcvs/pvc_pv1', json=postdata )
        _check_ltcv_res( 'pv1', [0], [100], res, which=which,
                         set_of_lightcurves=roots, procver_collection=pvc )

    # pvc_pv2 should be the default
    for which in [ 'detections', 'forced', 'patch', None ]:
        for objset in [ [ str(roots[i]['root'].id) for i in [0, 2] ], [ 200, 202 ] ]:
            postdata = { 'objids': objset }
            if which is not None:
                postdata['which'] = which
            for suffix in [ '/pvc_pv2', '' ]:
                res = fastdb_client.post( f'/ltcv/getmanyltcvs{suffix}', json=postdata )
                _check_ltcv_res( 'pv2', [0, 2], [200, 202], res, which=which,
                                 set_of_lightcurves=roots, procver_collection=pvc )


    # If we ask for diaobjectids that are in the wrong processing version, we still get
    #   back the corresponding ones from this processing version
    for which in [ 'detections', 'forced', 'patch', None ]:
        postdata = { 'objids': [0, 2] }
        if which is not None:
            postdata['which'] = which
        res = fastdb_client.post( '/ltcv/getmanyltcvs/pvc_pv2', json=postdata )
        _check_ltcv_res( 'pv2', [0, 2], [200, 202], res, which=which,
                         set_of_lightcurves=roots, procver_collection=pvc )


    # Test mjd_now
    for which in [ 'detections', 'forced', 'patch', None ]:
        for objset in [ [ str(roots[i]['root'].id) for i in [0, 2] ], [ 200, 202 ] ]:
            postdata = { 'objids': objset, 'mjd_now': 60041. }
            if which is not None:
                postdata['which'] = which
            res = fastdb_client.post( '/ltcv/getmanyltcvs/pvc_pv2', json=postdata )
            _check_ltcv_res( 'pv2', [0, 2], [200, 202], res, which=which, mjdnow=60041.,
                             set_of_lightcurves=roots, procver_collection=pvc )


    # Make sure bands words
    for which in [ 'detections', 'forced', 'patch', None ]:
        for objset in [ [ str(roots[i]['root'].id) for i in [0, 2] ], [ 200, 202 ] ]:
            for bands in [ 'r', [ 'r' ] ]:
                postdata = { 'objids': objset, 'bands': bands }
                if which is not None:
                    postdata['which'] = which
                res = fastdb_client.post( '/ltcv/getmanyltcvs/pvc_pv2', json=postdata )
                _check_ltcv_res( 'pv2', [0, 2], [200, 202], res, which=which, bands=['r'],
                                 set_of_lightcurves=roots, procver_collection=pvc )

    # Test include source positions
    for which in [ 'detections', 'forced', 'patch', None ]:
        for objset in [ [ str(roots[i]['root'].id) for i in [0, 1] ], [ 200, 201 ] ]:
            postdata = { 'objids': objset, 'include_source_positions': 1 }
            if which is not None:
                postdata['which'] = which
            res = fastdb_client.post( '/ltcv/getmanyltcvs/pvc_pv2', json=postdata )
            _check_ltcv_res( 'pv2', [0, 1], [200, 201, 2011], res, which=which, include_source_positions=True,
                             set_of_lightcurves=roots, procver_collection=pvc )

    # Test returning object info
    for which in [ 'detections', 'forced', 'patch', None ]:
        for objset in [ [ str(roots[i]['root'].id) for i in [0, 1] ], [ 200, 201 ] ]:
            postdata = { 'objids': objset,
                         'include_source_positions': 1,
                         'return_object_info': 1,
                         'include_base_procver': 1,
                         'include_object_positions': 1,
                        }
            if which is not None:
                postdata['which'] = which
            res = fastdb_client.post( '/ltcv/getmanyltcvs/pvc_pv2', json=postdata )
            _check_ltcv_res( 'pv2', [0, 1], [200, 201, 2011], res, which=which,
                             include_source_positions=True, return_object_info=True, include_base_procver=True,
                             set_of_lightcurves=roots, procver_collection=pvc )



def test_getltcv( test_user, fastdb_client, set_of_lightcurves, procver_collection ):
    roots = set_of_lightcurves
    bpvs, _pvs, _pvinfo = procver_collection

    def _check_ltcv( res, rootdex, objdex, which='patch', bpv_key='unknown', obj_bpv_key=None,
                     include_base_procver=False, include_source_ids=False, include_source_positions=False,
                     obj_base_procver=None, pos_base_procver=None ):
        obj_bpv_key = bpv_key if obj_bpv_key is None else obj_bpv_key
        bpv_key = [ bpv_key ] if not isinstance( bpv_key, list ) else bpv_key
        obj_bpv_key = [ obj_bpv_key ] if not isinstance( obj_bpv_key, list ) else obj_bpv_key
        expectedkeys = [ 'rootid', 'diaobjectid', 'ra', 'dec', 'raerr', 'decerr', 'ra_dec_cov', 'ltcv', 'rootid' ]
        expectedkeys_ltcv = [ 'mjd', 'band', 'flux', 'fluxerr', 'isdet' ]
        if which == 'patch':
            expectedkeys_ltcv.append( 'ispatch' )
        if include_base_procver:
            expectedkeys.extend( [ 'obj_base_procver_id', 'pos_base_procver_id' ] )
            expectedkeys_ltcv.extend( [ 'base_procver_s', 'base_procver_f' ] )
        if include_source_ids:
            expectedkeys_ltcv.extend( [ 'diaobjectid', 'diasourceid' ] )
            if which != 'detections':
                expectedkeys_ltcv.append( 'diaforcedsourceid' )
        if include_source_positions:
            expectedkeys_ltcv.extend( [ 'det_ra', 'det_dec', 'det_raerr', 'det_decerr', 'det_ra_dec_cov' ] )

        assert set( res.keys() ) == set( expectedkeys )
        assert set( res['ltcv'].keys() ) == set( expectedkeys_ltcv )

        assert res['rootid'] == str( roots[rootdex]['root'].id )
        assert all( o == roots[rootdex]['objs'][objdex]['obj'].diaobjectid for o in res['diaobjectid'] )
        for r, d in zip( res['ra'], res['dec'] ):
            assert any( r == pytest.approx( roots[rootdex]['objs'][objdex]['pos'][b][-1].ra, abs=1e-5 )
                        for b in obj_bpv_key )
            assert any( d == pytest.approx( roots[rootdex]['objs'][objdex]['pos'][b][-1].dec, abs=1e-5 )
                        for b in obj_bpv_key )
        if which == 'detections':
            assert all( r == 1 for r in res['ltcv']['isdet'] )
        if include_base_procver:
            for rb in res['obj_base_procver_id']:
                assert any( rb == str(bpvs[o].id) for o in obj_bpv_key )
            for rb in res['pos_base_procver_id']:
                for opv in obj_bpv_key:
                    valid_bpvkeys = [ bpvs[k].id for k in bpvs.keys() if k[0:len(opv)+19]==f'{opv}_diaobject_position' ]
                    assert any( rb == str(v) for v in valid_bpvkeys )

        forced = {}
        sources = {}
        # We're assuming that the bpv keys passed will be in increasing priority order
        for bk in bpv_key:
            forced.update( { f.visit: f for f in roots[rootdex]['objs'][objdex]['frc'][bk] } )
            sources.update( { s.visit: s for s in roots[rootdex]['objs'][objdex]['src'][bk] } )
        forced = list( forced.values() )
        sources = list( sources.values() )
        forced.sort( key=lambda f: f.midpointmjdtai )
        sources.sort( key=lambda s: s.midpointmjdtai )
        srci = 0
        frci = 0
        for i in range( len(res['ltcv']['mjd'] ) ):
            # Should have forced photometry where ispatch is not 1.  (ispatch will only be 1 for last n points)
            if ( ( 'which' == 'forced' ) or
                 ( ( 'ispatch' in res['ltcv'] ) and not ( res['ltcv']['ispatch'][i] ) ) ):
                assert res['ltcv']['mjd'][i] == pytest.approx( forced[frci].midpointmjdtai, abs=1./3600./24. )
                assert res['ltcv']['band'][i] == forced[frci].band
                assert res['ltcv']['flux'][i] == pytest.approx( forced[frci].psfflux, rel=1e-6 )
                assert res['ltcv']['fluxerr'][i] == pytest.approx( forced[frci].psffluxerr, rel=1e-6 )
                frci += 1
            # If 'isdet' is true, should correspond to a source
            if res['ltcv']['isdet'][i]:
                assert res['ltcv']['mjd'][i] == pytest.approx( sources[srci].midpointmjdtai, abs=1./3600./24. )
                assert res['ltcv']['band'][i] == sources[srci].band
                assert res['ltcv']['flux'][i] == pytest.approx( sources[srci].psfflux, rel=1e-6 )
                assert res['ltcv']['fluxerr'][i] == pytest.approx( sources[srci].psffluxerr, rel=1e-6 )
                srci += 1

    res = fastdb_client.post( f'/ltcv/getltcv/pvc_pv2/{roots[3]["root"].id}' )
    _check_ltcv( res, 3, 1, bpv_key='bpv2a' )

    # The base processing version of the *object* is going to be bpv2,
    #   even though we're supposed to be pulling photometry from default
    #   (which is an alias for pv3).  (TODO: fix the fixture so that the
    #   processing versions are different for different sources so we
    #   can test that!  That probably means editing other tests too when
    #   the fixture changes....)
    with pytest.raises( RuntimeError, match=( "Error response from server, status 422: rootids from "
                                              "many_object_ltcvs and get_object_infos don't match; you probably "
                                              "have the wrong object_procver." ) ):
        res = fastdb_client.post( '/ltcv/getltcv/203' )

    with pytest.raises( RuntimeError, match=( "Error response from server, status 422: rootids from "
                                              "many_object_ltcvs and get_object_infos don't match; you probably "
                                              "have the wrong object_procver." ) ):
        res = fastdb_client.post( '/ltcv/getltcv/pvc_pv3/203' )

    res = fastdb_client.post( '/ltcv/getltcv/pvc_pv3/203', json={ 'object_procver': 'pvc_pv2' } )
    _check_ltcv( res, 3, 1, bpv_key='bpv3', obj_bpv_key='bpv2a' )

    # Objects aren't defined in 'default', which is probably bad, damn, I really need to
    #   rethink these fixtures, which will be PAINFUL.
    with pytest.raises( RuntimeError, match=( "Error response from server, status 422: rootids from "
                                              "many_object_ltcvs and get_object_infos don't match; you probably "
                                              "have the wrong object_procver." ) ):
        res = fastdb_client.post( f'/ltcv/getltcv/{roots[3]["root"].id}' )

    with pytest.raises( RuntimeError, match=( "Error response from server, status 422: rootids from "
                                              "many_object_ltcvs and get_object_infos don't match; you probably "
                                              "have the wrong object_procver." ) ):
        res = fastdb_client.post( f'/ltcv/getltcv/pvc_pv3/{roots[3]["root"].id}' )

    res = fastdb_client.post( f'/ltcv/getltcv/pvc_pv3/{roots[3]["root"].id}',
                              json={ 'object_procver': 'pvc_pv2' } )
    _check_ltcv( res, 3, 1, bpv_key='bpv3', obj_bpv_key='bpv2a' )

    res = fastdb_client.post( '/ltcv/getltcv/realtime/0' )
    _check_ltcv( res, 0, 0, bpv_key='realtime' )

    # The object *is* defined in realtime, so the fallback defaults server side should work here
    res = fastdb_client.post( f'/ltcv/getltcv/realtime/{roots[0]["root"].id}' )
    _check_ltcv( res, 0, 0, bpv_key='realtime' )

    # This is an example where there's actually a mix of base processing versions on
    #   the returned forced photometry.
    res = fastdb_client.post( '/ltcv/getltcv/pvc_pv1/100' )
    _check_ltcv( res, 0, 2, bpv_key=['bpv1a', 'bpv1'], obj_bpv_key='bpv1a' )


# TODO : test getrandomltcv ; that might require the ability to pass a random seed for a reproducible test.
@pytest.mark.skip( reason="This test needs to be written" )
def test_getrandomltcv( test_user, fastdb_client, procver_collection, set_of_lightcurves ):
    assert False


def test_gethottransients( test_user, fastdb_client, procver_collection, set_of_lightcurves ):
    # This tests gets the same information as ../test_ltcv.py, only via
    # the webap.  ../test_ltcv.py::test_get_hot_ltcvs makes sure that
    # the direct call to ltcv.get_hot_ltcvs returns the right stuff.
    # (Or, at least, it should.)  This test makes sure that what you get
    # from the webap matches what you get from a direct call.

    def _compare_direct_to_webap( df, objdf, res ):
        assert ( df.index.get_level_values('diaobjectid').unique()
                 == np.array( [ r['diaobjectid'] for r in res ] ) ).all()
        assert ( objdf.index.get_level_values('diaobjectid').unique()
                 == np.array( [ r['diaobjectid'] for r in res ] ) ).all()
        for objrow in res:
            objid = objrow['diaobjectid']
            subdf = df.loc[ objid ]
            subobjdf = objdf.loc[ objid ]
            assert objrow['rootid'] == str( subobjdf.rootid )
            assert objrow['ra'] == subobjdf.ra
            assert objrow['dec'] == subobjdf.dec
            assert objrow['zp'] == 31.4
            assert len(subdf) == len( objrow['photometry']['mjd'] )
            assert ( subdf.index.values == np.array( objrow['photometry']['mjd'] ) ).all()
            assert ( subdf.band == np.array( objrow['photometry']['band'] ) ).all()
            assert ( subdf.visit == np.array( objrow['photometry']['visit'] ) ).all()
            assert ( subdf.flux == np.array( objrow['photometry']['flux'] ) ) .all()
            assert ( subdf.fluxerr == np.array( objrow['photometry']['fluxerr'] ) ).all()
            assert ( subdf.isdet == np.array( objrow['photometry']['isdet'] ) ).all()
            if 'ispatch' in subdf.columns:
                assert ( subdf.ispatch == np.array( objrow['photometry']['ispatch'] ) ).all()

    df, objdf, _ = ltcv.get_hot_ltcvs( 'pvc_pv3', detected_since_mjd=60035, mjd_now=60056 )
    res = fastdb_client.post( '/ltcv/gethottransients', json={ 'processing_version': 'pvc_pv3',
                                                               'detected_since_mjd': 60035,
                                                               'mjd_now': 60056 } )
    _compare_direct_to_webap( df, objdf, res )

    df, objdf, _ = ltcv.get_hot_ltcvs( 'pvc_pv3', detected_since_mjd=60035, mjd_now=60046 )
    res = fastdb_client.post( '/ltcv/gethottransients', json={ 'processing_version': 'pvc_pv3',
                                                               'detected_since_mjd': 60035,
                                                               'mjd_now': 60046 } )
    _compare_direct_to_webap( df, objdf, res )

    df, objdf, _ = ltcv.get_hot_ltcvs( 'pvc_pv3', detected_in_last_days=2, mjd_now=60021 )
    res = fastdb_client.post( '/ltcv/gethottransients', json={ 'processing_version': 'pvc_pv3',
                                                               'detected_in_last_days': 2,
                                                               'mjd_now': 60021 } )
    _compare_direct_to_webap( df, objdf, res )

    df, objdf, _ = ltcv.get_hot_ltcvs( 'pvc_pv3', detected_in_last_days=2, mjd_now=60041 )
    res = fastdb_client.post( '/ltcv/gethottransients', json={ 'processing_version': 'pvc_pv3',
                                                               'detected_in_last_days': 2,
                                                               'mjd_now': 60041 } )
    _compare_direct_to_webap( df, objdf, res )

    # detected_in_last_days defaults to 30
    df, objdf, _ = ltcv.get_hot_ltcvs( 'pvc_pv3', mjd_now=60085 )
    res = fastdb_client.post( '/ltcv/gethottransients', json={ 'processing_version': 'pvc_pv3',
                                                               'mjd_now': 60085 } )
    _compare_direct_to_webap( df, objdf, res )
    df, objdf, _ = ltcv.get_hot_ltcvs( 'pvc_pv3', mjd_now=60095 )
    res = fastdb_client.post( '/ltcv/gethottransients', json={ 'processing_version': 'pvc_pv3',
                                                               'mjd_now': 60095 } )
    _compare_direct_to_webap( df, objdf, res )

    # Test source patch.  Gotta use pvc_pv1 for this.

    df, objdf, _ = ltcv.get_hot_ltcvs( 'pvc_pv1', mjd_now=60031 )
    res = fastdb_client.post( '/ltcv/gethottransients', json={ 'processing_version': 'pvc_pv1',
                                                               'mjd_now': 60031 } )
    _compare_direct_to_webap( df, objdf, res )

    df, objdf, _ = ltcv.get_hot_ltcvs( 'pvc_pv1', mjd_now=60031, source_patch=True )
    res = fastdb_client.post( '/ltcv/gethottransients', json={ 'processing_version': 'pvc_pv1',
                                                               'mjd_now': 60031,
                                                               'source_patch': True } )
    _compare_direct_to_webap( df, objdf, res )
