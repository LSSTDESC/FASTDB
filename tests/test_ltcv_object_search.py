import pytest

import numpy as np

import db
import ltcv


def check_df_contents( df, procverid, statbands=None ):
    """Used in the test ltcv object test, to verify that the first, last, max fields match the databse.

    Won't work with anything that uses numdetinwindow, because the
    database searches here that match what's in the dataframe don't take the
    window into account, but ltcv.py::object_search first does a filter
    for only things captured within the window.

    """

    with db.DB() as con:
        cursor = con.cursor()

        assert all( df.lastdetmjd >= df.firstdetmjd )
        assert all( df.lastforcedmjd >= df.lastdetmjd )

        if statbands is not None:
            assert all( i in statbands for i in df.firstdetband )
            assert all( i in statbands for i in df.lastdetband )
            assert all( i in statbands for i in df.maxdetband )
            assert all( i in statbands for i in df.lastforcedband )

        for row in df.itertuples():
            q = ( "SELECT psfflux, psffluxerr, midpointmjdtai, band "
                  "FROM diasource "
                  "WHERE diaobjectid=%(o)s AND processing_version=%(pv)s " )
            if statbands is not None:
                q += "AND band=ANY(%(bands)s) "
            q += "ORDER BY psfflux DESC LIMIT 1"
            cursor.execute( q, { 'o': row.diaobjectid, 'pv': procverid, 'bands': statbands } )
            dbrow = cursor.fetchone()
            assert dbrow[2] == pytest.approx( row.maxdetmjd, abs=1e-5 )
            assert dbrow[3] == row.maxdetband
            assert dbrow[0] == pytest.approx( row.maxdetflux, rel=1e-5 )
            assert dbrow[1] == pytest.approx( row.maxdetfluxerr, rel=1e-5 )

            q = ( "SELECT psfflux, psffluxerr, midpointmjdtai, band "
                  "FROM diasource "
                  "WHERE diaobjectid=%(o)s AND processing_version=%(pv)s " )
            if statbands is not None:
                q += "AND band=ANY(%(bands)s) "
            q += "ORDER BY midpointmjdtai DESC LIMIT 1"
            cursor.execute( q, { 'o': row.diaobjectid, 'pv': procverid, 'bands': statbands } )
            dbrow = cursor.fetchone()
            assert dbrow[3] == row.lastdetband
            assert dbrow[2] == pytest.approx( row.lastdetmjd, abs=1e-5 )
            assert dbrow[0] == pytest.approx( row.lastdetflux, rel=1e-5 )
            assert dbrow[1] == pytest.approx( row.lastdetfluxerr, rel=1e-5 )

            q = ( "SELECT psfflux, psffluxerr, midpointmjdtai, band "
                  "FROM diasource "
                  "WHERE diaobjectid=%(o)s AND processing_version=%(pv)s " )
            if statbands is not None:
                q += "AND band=ANY(%(bands)s) "
            q += "ORDER BY midpointmjdtai LIMIT 1"
            cursor.execute( q, { 'o': row.diaobjectid, 'pv': procverid, 'bands': statbands } )
            dbrow = cursor.fetchone()
            assert dbrow[3] == row.firstdetband
            assert dbrow[2] == pytest.approx( row.firstdetmjd, abs=1e-5 )
            assert dbrow[0] == pytest.approx( row.firstdetflux, rel=1e-5 )
            assert dbrow[1] == pytest.approx( row.firstdetfluxerr, rel=1e-5 )

            q = ( "SELECT psfflux, psffluxerr, midpointmjdtai, band "
                  "FROM diaforcedsource "
                  "WHERE diaobjectid=%(o)s AND processing_version=%(pv)s " )
            if statbands is not None:
                q += "AND band=ANY(%(bands)s) "
            q += "ORDER BY midpointmjdtai DESC LIMIT 1"
            cursor.execute( q, { 'o': row.diaobjectid, 'pv': procverid, 'bands': statbands } )
            dbrow = cursor.fetchone()
            assert dbrow[3] == row.lastforcedband
            assert dbrow[2] == pytest.approx( row.lastforcedmjd, abs=1e-5 )
            assert dbrow[0] == pytest.approx( row.lastforcedflux, rel=1e-5 )
            assert dbrow[1] == pytest.approx( row.lastforcedfluxerr, rel=1e-5 )


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
    """This test tests lots of the keywords, but doesn't test every conceivable combination because n² is big."""

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


    # FIRST/MAX/LAST FLUX MJD TESTS

    results = ltcv.object_search( procver.description, return_format='pandas',
                                  mint_firstdetection=60400, maxt_firstdetection=60700 )
    assert all( results.firstdetmjd >= 60400 )
    assert all( results.firstdetmjd <= 60700 )
    assert any( results.lastdetmjd > 60700 )
    assert all( results.lastdetmjd >= results.firstdetmjd )
    check_df_contents( results, procver.id )

    results = ltcv.object_search( procver.description, return_format='pandas',
                                  mint_firstdetection=60400, maxt_firstdetection=60700,
                                  statbands=['g','r'] )
    assert len(results) == 64
    assert all( results.firstdetmjd >= 60400 )
    assert all( results.firstdetmjd <= 60700 )
    assert any( results.lastdetmjd > 60700 )
    check_df_contents( results, procver.id, ['g', 'r'] )

    results = ltcv.object_search( procver.description, return_format='pandas',
                                  mint_firstdetection=60400, maxt_firstdetection=60700,
                                  minmag_firstdetection=22, maxmag_firstdetection=24 )
    assert len(results) == 57
    assert all( results.firstdetmjd >= 60400 )
    assert all( results.firstdetmjd <= 60700 )
    assert all( results.firstdetflux >= 10**( (24-31.4) / -2.5 ) )
    assert all( results.firstdetflux <= 10**( (22-31.4) / -2.5 ) )
    assert any( results.lastdetmjd > 60700 )
    check_df_contents( results, procver.id )

    results = ltcv.object_search( procver.description, return_format='pandas',
                                  mint_firstdetection=60400, maxt_firstdetection=60700,
                                  minmag_firstdetection=22, maxmag_firstdetection=24,
                                  statbands=['g', 'r'] )
    assert len(results) == 36
    assert all( results.firstdetmjd >= 60400 )
    assert all( results.firstdetmjd <= 60700 )
    assert all( results.firstdetflux >= 10**( (24-31.4) / -2.5 ) )
    assert all( results.firstdetflux <= 10**( (22-31.4) / -2.5 ) )
    assert any( results.lastdetmjd > 60700 )
    check_df_contents( results, procver.id, ['g', 'r'] )


    results = ltcv.object_search( procver.description, return_format='pandas',
                                  mint_lastdetection=60400, maxt_lastdetection=60700 )
    assert len(results) == 90
    assert all( results.lastdetmjd >= 60400 )
    assert all( results.lastdetmjd <= 60700 )
    assert any( results.firstdetmjd < 60400 )
    check_df_contents( results, procver.id )

    results = ltcv.object_search( procver.description, return_format='pandas',
                                  mint_lastdetection=60400, maxt_lastdetection=60700,
                                  statbands=['g', 'r'] )
    assert len(results) == 66
    assert all( results.lastdetmjd >= 60400 )
    assert all( results.lastdetmjd <= 60700 )
    assert any( results.firstdetmjd < 60400 )
    check_df_contents( results, procver.id, ['g', 'r'] )

    results = ltcv.object_search( procver.description, return_format='pandas',
                                  mint_lastdetection=60400, maxt_lastdetection=60700,
                                  minmag_lastdetection=22, maxmag_lastdetection=24 )
    assert len(results) == 67
    assert all( results.lastdetmjd >= 60400 )
    assert all( results.lastdetmjd <= 60700 )
    assert all( results.lastdetflux >= 10**( (24-31.4) / -2.5 ) )
    assert all( results.lastdetflux <= 10**( (22-31.4) / -2.5 ) )
    assert any( results.firstdetmjd < 60400 )
    check_df_contents( results, procver.id )

    results = ltcv.object_search( procver.description, return_format='pandas',
                                  mint_lastdetection=60400, maxt_lastdetection=60700,
                                  minmag_lastdetection=22, maxmag_lastdetection=24,
                                  statbands=['g', 'r'] )
    assert len(results) == 43
    assert all( results.lastdetmjd >= 60400 )
    assert all( results.lastdetmjd <= 60700 )
    assert all( results.lastdetflux >= 10**( (24-31.4) / -2.5 ) )
    assert all( results.lastdetflux <= 10**( (22-31.4) / -2.5 ) )
    assert any( results.firstdetmjd < 60400 )
    check_df_contents( results, procver.id, ['g', 'r'] )


    results = ltcv.object_search( procver.description, return_format='pandas',
                                  mint_maxdetection=60400, maxt_maxdetection=60700 )
    assert len(results) == 88
    assert all( results.maxdetmjd >= 60400 )
    assert all( results.maxdetmjd <= 60700 )
    assert any( results.firstdetmjd < 60400 )
    assert any( results.lastdetmjd > 60700 )
    check_df_contents( results, procver.id )

    results = ltcv.object_search( procver.description, return_format='pandas',
                                  mint_maxdetection=60400, maxt_maxdetection=60700,
                                  statbands=['g', 'r'] )
    assert len(results) == 64
    assert all( results.maxdetmjd >= 60400 )
    assert all( results.maxdetmjd <= 60700 )
    assert any( results.firstdetmjd < 60400 )
    # assert any( results.lastdetmjd > 60700 )   #  Just didn't happen...
    check_df_contents( results, procver.id, ['g', 'r'] )


    results = ltcv.object_search( procver.description, return_format='pandas',
                                  mint_maxdetection=60400, maxt_maxdetection=60700,
                                  minmag_maxdetection=22, maxmag_lastdetection=24 )
    assert len(results) == 57
    assert all( results.maxdetmjd >= 60400 )
    assert all( results.maxdetmjd <= 60700 )
    assert any( results.firstdetmjd < 60400 )
    assert any( results.lastdetmjd > 60700 )
    assert all( results.maxdetflux >= 10**( (24-31.4) / -2.5 ) )
    assert all( results.maxdetflux <= 10**( (22-31.4) / -2.5 ) )
    check_df_contents( results, procver.id )

    results = ltcv.object_search( procver.description, return_format='pandas',
                                  mint_maxdetection=60400, maxt_maxdetection=60700,
                                  minmag_maxdetection=22, maxmag_lastdetection=24,
                                  statbands=['g', 'r'] )
    assert len(results) == 39
    assert all( results.maxdetmjd >= 60400 )
    assert all( results.maxdetmjd <= 60700 )
    assert any( results.firstdetmjd < 60400 )
    # assert any( results.lastdetmjd > 60700 )   #  Just didn't happen...
    assert all( results.maxdetflux >= 10**( (24-31.4) / -2.5 ) )
    assert all( results.maxdetflux <= 10**( (22-31.4) / -2.5 ) )
    check_df_contents( results, procver.id, ['g', 'r'] )


    # Number of detections
    lotsofresults = ltcv.object_search( procver.description, return_format='pandas', min_numdetections=1 )
    for n in [ 5, 10, 25, 50 ]:
        results = ltcv.object_search( procver.description, return_format='pandas', min_numdetections=n )
        expectedn = ( lotsofresults.numdet >= n ).sum()
        assert len(results) == expectedn
        check_df_contents( results, procver.id )

    manyresults = ltcv.object_search( procver.description, return_format='pandas', min_numdetections=1,
                                      statbands=['g','r'] )
    assert len(manyresults) < len(lotsofresults)
    for n in [ 5, 10, 25, 50 ]:
        results = ltcv.object_search( procver.description, return_format='pandas', min_numdetections=n,
                                      statbands=['g', 'r'] )
        expectedn = ( manyresults.numdet >= n ).sum()
        assert len(results) == expectedn
        check_df_contents( results, procver.id, ['g', 'r'] )


    # A numdetection mix just for the sake of it
    results = ltcv.object_search( procver.description, return_format='pandas',
                                  mint_maxdetection=60400, maxt_maxdetection=60700,
                                  minmag_maxdetection=22, maxmag_lastdetection=24,
                                  min_numdetections=5 )
    assert len(results) < 57      # This was checked above but without min_numdetections, so should have less here
    assert len(results) == 18
    assert all( results.maxdetmjd >= 60400 )
    assert all( results.maxdetmjd <= 60700 )
    assert any( results.firstdetmjd < 60400 )
    # assert any( results.lastdetmjd > 60700 )   # Didn't happen
    assert all( results.maxdetflux >= 10**( (24-31.4) / -2.5 ) )
    assert all( results.maxdetflux <= 10**( (22-31.4) / -2.5 ) )
    check_df_contents( results, procver.id )


    # Check minimum and maximum last mag
    # Start by redoing an old search
    oldresults = ltcv.object_search( procver.description, return_format='pandas',
                                     mint_maxdetection=60400, maxt_maxdetection=60700 )
    kept = oldresults[ oldresults.lastforcedflux > 0 ]
    kept = kept[ ( -2.5*np.log10( kept.lastforcedflux ) + 31.4 <= 24 ) &
                 ( -2.5*np.log10( kept.lastforcedflux ) + 31.4 >= 23 ) ]
    results = ltcv.object_search( procver.description, return_format='pandas',
                                  mint_maxdetection=60400, maxt_maxdetection=60700,
                                  min_lastmag=23, max_lastmag=24 )
    assert len(results) == len(kept)
    assert all( results.lastforcedflux >= 10**( (24-31.4) / -2.5 ) )
    assert all( results.lastforcedflux <= 10**( (23-31.4) / -2.5 ) )
    assert all( results.maxdetmjd >= 60400 )
    assert all( results.maxdetmjd <= 60700 )
    assert any( results.firstdetmjd < 60400 )
    # assert any( results.lastdetmjd > 60700 )   # Didn't happen
    check_df_contents( results, procver.id )


    # Test searching inside a window
    # This is what I got the first time I ran it.  TODO, poke into the
    #   database to make sure it got the right results?
    results = ltcv.object_search( procver.description, return_format='pandas',
                                  window_t0=60500, window_t1=60600, min_window_numdetections=5 )
    assert all( results.numdetinwindow >= 5 )
    assert len(results) == 8
