import datetime
import pytest
import astropy.time
import db


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
