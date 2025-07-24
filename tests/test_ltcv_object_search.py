import pytest

import db
import ltcv


def check_df_contents( df, procverid, statbands=None ):
    with db.DB() as con:
        cursor = con.cursor()

        for row in df.itertuples():
            q = ( "SELECT psfflux, psffluxerr, midpointmjdtai, band "
                  "FROM diasource "
                  "WHERE diaobjectid=%(o)s AND processing_version=%(pv)s " )
            if statbands is not None:
                q += "AND band=ANY(%(bands)s) "
            q += "ORDER BY psfflux DESC LIMIT 1"
            cursor.execute( q, { 'o': row.diaobjectid, 'pv': procverid, 'bands': statbands } )
            dbrow = cursor.fetchone()
            assert dbrow[0] == pytest.approx( row.maxdetflux, rel=1e-5 )
            assert dbrow[1] == pytest.approx( row.maxdetfluxerr, rel=1e-5 )
            assert dbrow[2] == pytest.approx( row.maxdetmjd, abs=1e-5 )
            assert dbrow[3] == row.maxdetband

            q = ( "SELECT psfflux, psffluxerr, midpointmjdtai, band "
                  "FROM diasource "
                  "WHERE diaobjectid=%(o)s AND processing_version=%(pv)s " )
            if statbands is not None:
                q += "AND band=ANY(%(bands)s) "
            q += "ORDER BY midpointmjdtai DESC LIMIT 1"
            cursor.execute( q, { 'o': row.diaobjectid, 'pv': procverid, 'bands': statbands } )
            dbrow = cursor.fetchone()
            assert dbrow[0] == pytest.approx( row.lastdetflux, rel=1e-5 )
            assert dbrow[1] == pytest.approx( row.lastdetfluxerr, rel=1e-5 )
            assert dbrow[2] == pytest.approx( row.lastdetmjd, abs=1e-5 )
            assert dbrow[3] == row.lastdetband

            q = ( "SELECT psfflux, psffluxerr, midpointmjdtai, band "
                  "FROM diasource "
                  "WHERE diaobjectid=%(o)s AND processing_version=%(pv)s " )
            if statbands is not None:
                q += "AND band=ANY(%(bands)s) "
            q += "ORDER BY midpointmjdtai LIMIT 1"
            cursor.execute( q, { 'o': row.diaobjectid, 'pv': procverid, 'bands': statbands } )
            dbrow = cursor.fetchone()
            assert dbrow[0] == pytest.approx( row.firstdetflux, rel=1e-5 )
            assert dbrow[1] == pytest.approx( row.firstdetfluxerr, rel=1e-5 )
            assert dbrow[2] == pytest.approx( row.firstdetmjd, abs=1e-5 )
            assert dbrow[3] == row.firstdetband

            q = ( "SELECT psfflux, psffluxerr, midpointmjdtai, band "
                  "FROM diaforcedsource "
                  "WHERE diaobjectid=%(o)s AND processing_version=%(pv)s " )
            if statbands is not None:
                q += "AND band=ANY(%(bands)s) "
            q += "ORDER BY midpointmjdtai DESC LIMIT 1"
            cursor.execute( q, { 'o': row.diaobjectid, 'pv': procverid, 'bands': statbands } )
            dbrow = cursor.fetchone()
            assert dbrow[0] == pytest.approx( row.lastforcedflux, rel=1e-5 )
            assert dbrow[1] == pytest.approx( row.lastforcedfluxerr, rel=1e-5 )
            assert dbrow[2] == pytest.approx( row.lastforcedmjd, abs=1e-5 )
            assert dbrow[3] == row.lastforcedband


# The test_user fixture is in this next test not becasue it's needed for
#   the test, but because this is a convenient test for loading up a
#   database for use developing the web ap.  In the tests subdirectory,
#   run:
#      pytest -v --trace test_ltcv_object_search.py::test_object_search
#   and wait about a minute for the fixtures to finish.  When you get the (Pdb) prompt,
#   you're at the beginning of this test.  Let that shell just sit there, and go play
#   with the web ap.

# This is separated out from test_ltcv.py since it uses a different fixture... at least for now
def test_object_search( procver, test_user, snana_fits_maintables_loaded_module ):
    with pytest.raises( ValueError, match="Unknown search keywords: {'foo'}" ):
        ltcv.object_search( procver.description, foo=5 )

    with pytest.raises( ValueError, match='Unknown return format foo' ):
        ltcv.object_search( procver.description, return_format='foo' )

    # Do an absurdly large radial query to see if we get more than one
    jsonresults = ltcv.object_search( procver.description, return_format='json',
                                      ra=185.45, dec=-34.95, radius=5.3*3600. )
    assert set( jsonresults.keys() ) == { 'diaobjectid', 'ra', 'dec', 'numdet', 'numdetinwindow',
                                          'firstdetmjd', 'firstdetband', 'firstdetflux', 'firstdetfluxerr',
                                          'lastdetmjd', 'lastdetband', 'lastdetflux', 'lastdetfluxerr',
                                          'maxdetmjd', 'maxdetband', 'maxdetflux', 'maxdetfluxerr',
                                          'lastforcedmjd', 'lastforcedband', 'lastforcedflux', 'lastforcedfluxerr' }
    assert set( jsonresults['diaobjectid']) == { 1340712, 1822149, 2015822 }

    # Also get the pandas response, make sure it's the same as json
    results = ltcv.object_search( procver.description, return_format='pandas',
                                  ra=185.45, dec=-34.95, radius=5.3*3600. )
    assert len(results) == 3
    assert set( results.columns ) == set( jsonresults.keys() )
    for row in results.itertuples():
        dex = jsonresults['diaobjectid'].index( row.diaobjectid )
        for col in results.columns:
            assert jsonresults[col][dex] == getattr( row, col )

    check_df_contents( results, procver.id, None )

    # Now do a search including only r-band
    resultsr = ltcv.object_search( procver.description, return_format='pandas',
                                   ra=185.45, dec=-34.95, radius=5.3*3600.,
                                   statbands='r' )
    assert len(resultsr) == 3
    assert all( r.maxdetband == 'r' for r in resultsr.itertuples() )
    assert all( r.lastdetband == 'r' for r in resultsr.itertuples() )
    assert all( r.lastforcedband == 'r' for r in resultsr.itertuples() )
    check_df_contents( resultsr, procver.id, ['r'] )

    # Now try r- and g-band
    resultsrg = ltcv.object_search( procver.description, return_format='pandas',
                                    ra=185.45, dec=-34.95, radius=5.3*3600.,
                                    statbands=[ 'r', 'g' ] )
    assert len(resultsrg) == 3
    # Because we searched more bands, at least one of the lightcurves should have more detections
    bigger = False
    for row in resultsrg.itertuples():
        bigger = bigger or ( resultsr[resultsr.diaobjectid==row.diaobjectid].numdet.values[0] < row.numdet )
    assert bigger
    assert all( r.maxdetband in ('r', 'g') for r in resultsrg.itertuples() )
    assert all( r.lastdetband in ('r', 'g') for r in resultsrg.itertuples() )
    assert all( r.lastforcedband in  ('r', 'g') for r in resultsrg.itertuples() )
    check_df_contents( resultsrg, procver.id, ['r', 'g'] )
