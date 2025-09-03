import datetime
import pytest

import astropy.time

import db
from spectrum import what_spectra_are_wanted, get_spectrum_info


_dt_of_mjd = lambda mjd : astropy.time.Time( mjd, format='mjd' ).to_datetime( timezone=datetime.UTC )


@pytest.fixture( scope="module" )
def wanted_spectra( set_of_lightcurves, test_user ):
    roots = set_of_lightcurves

    wanteds_list = [ { 'wantspec_id': f'{roots[0]["root"].id} ; req1',
                       'root_diaobject_id': roots[0]['root'].id,
                       'wanttime': _dt_of_mjd(60010.),
                       'user_id': test_user.id,
                       'requester': 'req1',
                       'priority': 1
                      },
                     { 'wantspec_id': f'{roots[0]["root"].id} ; req2',
                       'root_diaobject_id': roots[0]['root'].id,
                       'wanttime': _dt_of_mjd(60015.),
                       'user_id': test_user.id,
                       'requester': 'req2',
                       'priority': 5
                      },
                     { 'wantspec_id': f'{roots[1]["root"].id} ; req1',
                       'root_diaobject_id': roots[1]["root"].id,
                       'wanttime': _dt_of_mjd(60025.),
                       'user_id': test_user.id,
                       'requester': 'req1',
                       'priority': 2
                      },
                     { 'wantspec_id': f'{roots[2]["root"].id} ; req1',
                       'root_diaobject_id': roots[2]["root"].id,
                       'wanttime': _dt_of_mjd(60050.),
                       'user_id': test_user.id,
                       'requester': 'req1',
                       'priority': 3
                      }
                    ]
    try:
        with db.DBCon() as con:
            wanteds = [ db.WantedSpectra( **kwargs ) for kwargs in wanteds_list ]
            for w in wanteds:
                w.insert( dbcon=con, nocommit=True, refresh=False )
            con.commit()

        yield wanteds

    finally:
        with db.DBCon() as con:
            con.execute_nofetch( "DELETE FROM wantedspectra WHERE wantspec_id=ANY(%(them)s)",
                                 { 'them': [ w['wantspec_id'] for w in wanteds_list ] } )
            con.commit()


@pytest.fixture( scope="module" )
def planned_spectra( set_of_lightcurves ):
    roots = set_of_lightcurves

    planneds_list = [ { 'root_diaobject_id': roots[1]['root'].id,
                        'facility': 'test facility',
                        'plantime': _dt_of_mjd(60030.),
                       },
                      { 'root_diaobject_id': roots[2]['root'].id,
                        'facility': 'test facility',
                        'plantime': _dt_of_mjd(60055.),
                       } ]

    try:
        with db.DBCon() as con:
            planneds = [ db.PlannedSpectra( **kwargs ) for kwargs in planneds_list ]
            for p in planneds:
                p.insert( dbcon=con, nocommit=True, refresh=False )
            con.commit()

        yield planneds

    finally:
        with db.DBCon() as con:
            con.execute_nofetch( "DELETE FROM plannedspectra WHERE root_diaobject_id=ANY(%(them)s)",
                                 { 'them': [ roots[1]['root'].id, roots[2]['root'].id ] } )
            con.commit()


@pytest.fixture( scope="module" )
def reported_spectra( set_of_lightcurves ):
    roots = set_of_lightcurves

    specs_list = [ { 'root_diaobject_id': roots[1]['root'].id,
                     'facility': 'test facility',
                     'mjd': 60031.,
                     'z': 0.25,
                     'classid': 42
                    } ]

    try:
        with db.DBCon() as con:
            specs = [ db.SpectrumInfo( **kwargs ) for kwargs in specs_list ]
            for s in specs:
                s.insert( dbcon=con, nocommit=True, refresh=False )
            con.commit()

        yield specs

    finally:
        with db.DBCon() as con:
            con.execute_nofetch( "DELETE FROM spectruminfo WHERE root_diaobject_id=ANY(%(them)s)",
                                 { 'them': [ roots[1]['root'].id ] } )
            con.commit()


# This one is not a module fixture
@pytest.fixture
def more_reported_spectra( set_of_lightcurves, reported_spectra ):
    roots = set_of_lightcurves

    specs_list = [ { 'root_diaobject_id': roots[0]['root'].id,
                     'facility': 'another test facility',
                     'mjd': 60020.,
                     'z': 0.1,
                     'classid': 42
                    },
                   { 'root_diaobject_id': roots[0]['root'].id,
                     'facility': 'test facility',
                     'mjd': 60025.,
                     'z': 0.11,
                     'classid': 13
                    },
                   { 'root_diaobject_id': roots[2]['root'].id,
                     'facility': 'test facility',
                     'mjd': 60050.,
                     'z': 0.33,
                     'classid': 666
                    } ]
    try:
        with db.DBCon() as con:
            specs = [ db.SpectrumInfo( **kwargs ) for kwargs in specs_list ]
            for s in specs:
                s.insert( dbcon=con, nocommit=True, refresh=False )
            con.commit()

        yield specs

    finally:
        with db.DBCon() as con:
            con.execute_nofetch( "DELETE FROM spectruminfo WHERE root_diaobject_id=ANY(%(them)s)",
                                 { 'them': [ roots[0]['root'].id, roots[2]['root'].id ] } )
            con.commit()


def test_what_spectra_are_wanted( wanted_spectra, planned_spectra, reported_spectra, set_of_lightcurves ):
    roots = set_of_lightcurves

    # FIRST TEST:
    # Try to get everything that's wanted
    df = what_spectra_are_wanted( 'realtime', mjdnow=60080. )
    df.insert( 0, 'id',[ f"{str(i)} ; {r}" for i, r in zip( df.root_diaobject_id.values, df.requester.values ) ] )
    assert ( set( df.id.values ) == set( str(w.wantspec_id) for w in wanted_spectra ) )
    assert all( df[ df.id==w.wantspec_id ].root_diaobject_id.values[0] == w.root_diaobject_id for w in wanted_spectra )
    assert all( df[ df.id==w.wantspec_id ].requester.values[0] == w.requester for w in wanted_spectra )
    assert all( df[ df.id==w.wantspec_id ].priority.values[0] == w.priority for w in wanted_spectra )

    # The first two should have a last detection of 60030 and a last forced of 60050, because they're object 0
    subdf = df[ df.root_diaobject_id==roots[0]['root'].id ]
    assert len( subdf ) == 2
    assert ( subdf.frced_mjd == 60050 ).all()
    assert ( subdf.src_mjd == 60030 ).all()
    # TODO : check the magnitude and based, compared to what's in set_of_lightcurves

    # The others should have a last detection of 60060 and a last forced of 60055: that's where realtime ends
    subdf = df[ df.root_diaobject_id!=roots[0]['root'].id ]
    assert len( subdf ) == 2
    assert ( subdf.frced_mjd == 60055 ).all()
    assert ( subdf.src_mjd == 60060 ).all()
    # TODO : check the magnitude and based, compared to what's in set_of_lightcurves

    # SECOND TEST
    # Only get things that are wanted since mjd 60020
    df = what_spectra_are_wanted( 'realtime', mjdnow=60080, wantsince=_dt_of_mjd(60020) )
    df.insert( 0, 'id',[ f"{str(i)} ; {r}" for i, r in zip( df.root_diaobject_id.values, df.requester.values ) ] )
    expectedids = [ w.wantspec_id for w in wanted_spectra if w.wanttime >= _dt_of_mjd(60020) ]
    assert len( df.id ) == len( expectedids )
    assert set( df.id ) == set( expectedids )

    # THIRD TEST
    # Only get things that req1 asked for
    df = what_spectra_are_wanted( 'realtime', mjdnow=60080, requester='req1' )
    df.insert( 0, 'id',[ f"{str(i)} ; {r}" for i, r in zip( df.root_diaobject_id.values, df.requester.values ) ] )
    expectedids = [ w.wantspec_id for w in wanted_spectra if w.requester == 'req1' ]
    assert len( df.id ) == len( expectedids )
    assert set( df.id ) == set( expectedids )
    assert ( df.requester.values == 'req1' ).all()

    # FOURTH TEST
    # Combine the previous two tests.  This will just get 2 results
    df = what_spectra_are_wanted( 'realtime', mjdnow=60080, requester='req1', wantsince=_dt_of_mjd(60020) )
    df.insert( 0, 'id',[ f"{str(i)} ; {r}" for i, r in zip( df.root_diaobject_id.values, df.requester.values ) ] )
    expectedids = [ w.wantspec_id for w in wanted_spectra if w.requester == 'req1' and w.wanttime >= _dt_of_mjd(60020) ]
    assert len( expectedids ) == 2
    assert len( df.id ) == len( expectedids )
    assert set( df.id ) == set( expectedids )
    assert ( df.requester.values == 'req1' ).all()

    # FIFTH TEST
    # Nothing claimed since 60050.  This should through out one, as roots[2] is claimed for 60055
    df  = what_spectra_are_wanted( 'realtime', mjdnow=60080, notclaimsince=_dt_of_mjd(60050) )
    df.insert( 0, 'id',[ f"{str(i)} ; {r}" for i, r in zip( df.root_diaobject_id.values, df.requester.values ) ] )
    expectedids = [ w.wantspec_id for w in wanted_spectra if w.root_diaobject_id != roots[2]['root'].id ]
    assert len( expectedids ) == 3
    assert len( df.id ) == len( expectedids )
    assert set( df.id ) == set( expectedids )

    # SIXTH TEST
    # No spectrum since mjd 60030.  This will throw out roots[1],w hich has a spectrum on mjd 60031.
    df = what_spectra_are_wanted( 'realtime', mjdnow=60080, nospecsince=60030 )
    df.insert( 0, 'id',[ f"{str(i)} ; {r}" for i, r in zip( df.root_diaobject_id.values, df.requester.values ) ] )
    expectedids = [ w.wantspec_id for w in wanted_spectra if w.root_diaobject_id != roots[1]['root'].id ]
    assert len( expectedids ) == 3
    assert len( df.id ) == len( expectedids )
    assert set( df.id ) == set( expectedids )

    # SEVENTH TEST
    # Detected since 60040.  This should through out the two root[0] wanteds.
    df = what_spectra_are_wanted( 'realtime', mjdnow=60080, detsince=60040 )
    df.insert( 0, 'id',[ f"{str(i)} ; {r}" for i, r in zip( df.root_diaobject_id.values, df.requester.values ) ] )
    expectedids = [ w.wantspec_id for w in wanted_spectra if w.root_diaobject_id != roots[0]['root'].id ]
    assert len( expectedids ) == 2
    assert len( df.id ) == len( expectedids )
    assert set( df.id ) == set( expectedids )

    # EIGHTH TEST
    # lim_mag 24.8 will keep only roots[2], as it's the only one that's at least that bright
    #   still at mjd 60060
    df = what_spectra_are_wanted( 'realtime', mjdnow=60080, lim_mag=24.8 )
    df.insert( 0, 'id',[ f"{str(i)} ; {r}" for i, r in zip( df.root_diaobject_id.values, df.requester.values ) ] )
    expectedids = [ w.wantspec_id for w in wanted_spectra if w.root_diaobject_id == roots[2]['root'].id ]
    assert len( expectedids ) == 1
    assert len( df.id ) == len( expectedids )
    assert set( df.id ) == set( expectedids )

    # NINTH TEST
    # However, if we do lim_mag 24.5 in the i-band, it will keep both roots[1] and roots[2]
    df = what_spectra_are_wanted( 'realtime', mjdnow=60080, lim_mag=24.8, lim_mag_band='i' )
    df.insert( 0, 'id',[ f"{str(i)} ; {r}" for i, r in zip( df.root_diaobject_id.values, df.requester.values ) ] )
    expectedids = [ w.wantspec_id for w in wanted_spectra
                    if w.root_diaobject_id in ( roots[2]['root'].id, roots[1]['root'].id ) ]
    assert len( expectedids ) == 2
    assert len( df.id ) == len( expectedids )
    assert set( df.id ) == set( expectedids )

    # TENTH TEST
    # If we say r band, that's back to the results of the eight test
    df = what_spectra_are_wanted( 'realtime', mjdnow=60080, lim_mag=24.8, lim_mag_band='r' )
    df.insert( 0, 'id',[ f"{str(i)} ; {r}" for i, r in zip( df.root_diaobject_id.values, df.requester.values ) ] )
    expectedids = [ w.wantspec_id for w in wanted_spectra if w.root_diaobject_id == roots[2]['root'].id ]
    assert len( expectedids ) == 1
    assert len( df.id ) == len( expectedids )
    assert set( df.id ) == set( expectedids )

    # ...should probably do all kinds of tests combining the criteria, huh.


def test_get_spectrum_info( set_of_lightcurves, reported_spectra, more_reported_spectra ):
    roots = set_of_lightcurves

    df = get_spectrum_info( rootids=roots[0]['root'].id )
    assert len(df) == 2
    assert set( df.root_diaobject_id ) == { roots[0]['root'].id }
    assert set( df.facility ) == { 'test facility', 'another test facility' }
    assert df[ df.facility == 'test facility' ].mjd.values[0] == 60025
    assert df[ df.facility == 'test facility' ].z.values[0] == 0.11
    assert df[ df.facility == 'test facility' ].classid.values[0] == 13
    assert df[ df.facility == 'another test facility' ].mjd.values[0] == 60020
    assert df[ df.facility == 'another test facility' ].z.values[0] == 0.1
    assert df[ df.facility == 'another test facility' ].classid.values[0] == 42

    df = get_spectrum_info( facility='test facility' )
    assert len(df) == 3
    assert ( df.facility == 'test facility' ).all()
    df = get_spectrum_info( facility='another test facility' )
    assert len(df) == 1
    assert ( df.facility == 'another test facility' ).all()

    df = get_spectrum_info( mjd_min=60022. )
    assert len(df) == 3
    assert set( df.mjd ) == { 60025., 60031., 60050. }
    df = get_spectrum_info( mjd_max=60028. )
    assert len(df) == 2
    assert set( df.mjd ) == { 60020., 60025. }
    df = get_spectrum_info( mjd_min=60022., mjd_max=60028. )
    assert len(df) == 1
    assert set( df.mjd ) == { 60025. }

    df = get_spectrum_info( classid=42 )
    assert len(df) == 2
    assert set( df.root_diaobject_id ) == { roots[0]['root'].id, roots[1]['root'].id }
    assert df[ df.root_diaobject_id==roots[0]['root'].id ].mjd.values[0] == 60020.
    assert df[ df.root_diaobject_id==roots[0]['root'].id ].z.values[0] == 0.1
    assert df[ df.root_diaobject_id==roots[0]['root'].id ].facility.values[0] == 'another test facility'
    assert df[ df.root_diaobject_id==roots[1]['root'].id ].mjd.values[0] == 60031.
    assert df[ df.root_diaobject_id==roots[1]['root'].id ].z.values[0] == 0.25
    assert df[ df.root_diaobject_id==roots[1]['root'].id ].facility.values[0] == 'test facility'

    df = get_spectrum_info( z_min=0.15 )
    assert len(df) == 2
    assert set( df.root_diaobject_id ) == { roots[1]['root'].id, roots[2]['root'].id }
    df = get_spectrum_info( z_max=0.30 )
    assert len(df) == 3
    assert set( df.root_diaobject_id ) == { roots[0]['root'].id, roots[1]['root'].id }
    df = get_spectrum_info( z_min=0.15, z_max=0.30 )
    assert len(df) == 1
    assert df.root_diaobject_id.values[0] == roots[1]['root'].id
