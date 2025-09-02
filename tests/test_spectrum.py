import datetime
import pytest

import astropy.time

import db
import util
from spectrum import what_spectra_are_wanted

@pytest.fixture( scope="module" )
def wanted_spectra( set_of_lightcurves, test_user ):
    roots = set_of_lightcurves

    mjd60010 = astropy.time.Time( 60010., format='mjd' ).to_datetime( timezone=datetime.UTC )
    mjd60015 = astropy.time.Time( 60015., format='mjd' ).to_datetime( timezone=datetime.UTC )
    mjd60025 = astropy.time.Time( 60025., format='mjd' ).to_datetime( timezone=datetime.UTC )
    mjd60050 = astropy.time.Time( 60050., format='mjd' ).to_datetime( timezone=datetime.UTC )
    
    wanteds_list = [ { 'wantspec_id': f'{roots[0]["root"].id} ; req1',
                       'root_diaobject_id': roots[0]['root'].id,
                       'wanttime': mjd60010,
                       'user_id': test_user.id,
                       'requester': 'req1',
                       'priority': 1
                      },
                     { 'wantspec_id': f'{roots[0]["root"].id} ; req2',
                       'root_diaobject_id': roots[0]['root'].id,
                       'wanttime': mjd60015,
                       'user_id': test_user.id,
                       'requester': 'req2',
                       'priority': 5
                      },
                     { 'wantspec_id': f'{roots[1]["root"].id} ; req1',
                       'root_diaobject_id': roots[1]["root"].id,
                       'wanttime': mjd60025,
                       'user_id': test_user.id,
                       'requester': 'req1',
                       'priority': 2
                      },
                     { 'wantspec_id': f'{roots[2]["root"].id} ; req1',
                       'root_diaobject_id': roots[2]["root"].id,
                       'wanttime': mjd60050,
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

    mjd60030 = astropy.time.Time( 60030., format='mjd' ).to_datetime( timezone=datetime.UTC )
    mjd60055 = astropy.time.Time( 60055., format='mjd' ).to_datetime( timezone=datetime.UTC )


    planneds_list = [ { 'root_diaobject_id': roots[1]['root'].id,
                        'facility': 'test facility',
                        'plantime': mjd60030
                       },
                      { 'root_diaobject_id': roots[2]['root'].id,
                        'facility': 'test facility',
                        'plantime': mjd60055
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
                     'mjd': 60031,
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


def test_what_spectra_are_wanted( wanted_spectra, planned_spectra, reported_spectra ):
    df = what_spectra_are_wanted( 'pvc_pv3', mjdnow=60080. )
    import pdb; pdb.set_trace()
        
              
    
    
                 
                 
