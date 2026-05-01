# EDITING OF THIS MODULE IS IN PROGRESS, IT'S CURRENTLY BROKEN

import sys
import io
import datetime
import pytz
import logging

import numpy as np
import pandas
import astropy.time
from psycopg import sql

import db
import util
import ltcv

# Want this to be False except when
#  doing deep-in-the-weeds debugging
_show_way_too_much_debug_info = False


def what_spectra_are_wanted( procver='realtime', position_procver=None,
                             wantsince=None, requester=None, notclaimsince=None,
                             nospecsince=None, detsince=None, lim_mag=None, lim_mag_band=None,
                             is_host=None, mjdnow=None, logger=None ):
    """Find out what spectra have been requested.

    In addition to the explicit filters below, there are some implicit filters:

    * If there are no detections in processing version procver, that
      wanted spectrum will be thrown out.  This can easily happen.
      Suppose there are detections in the 'realtime' procver, and
      suppose that that's what was used by the spectrum recommendation
      engine to choose what is wanted.  If you then pass procver 'dr1',
      but a given object with wanted spectra has no 'dr1' photometry,
      that object will be filtered out.

    * If you set a lim_mag_band, even without setting a lim_mag, it will
      implicitly filter out anything that doesn't have a detection at
      all in lim_mag_band.

    Parmeters
    ---------
      procver : str, default 'realtime'
        The processing version or alias to look at photometry for.  Used
        to filter the diasource and diaforcedsource tables.  If you get
        no photometry back, it means that there is no photometry for
        that processing version in the database.

        To filter object tables, it will normally get the object that is
        associated with the photometry of this processing version.  If
        there isn't any photometry of this processing version, will look
        for the object of this processing version.  If there's no object
        either, then that's an error.

      wantsince : datetime or None
        If not None, only get spectra that have been requested since this time.

      requester : str or None
        If given, only return wanted spectra tagged with this requester.

      notclaimsince : datetime or None
        If not None, only get spectra that have not been claimed
        (i.e. declared as planned) since this time.

      nospecsince : float or None
        If not None, this should be an mjd.  Will not return objects
        that have spectrum info taken since this mjd.

      detsince : float or None
        If not None, this should be an mjd.  Will only return
        objects that have been *detected* (i.e. have a diasource)
        since this mjd in the specified procver.

      lim_mag : float or None
        If not None, only return objects whose most recent observation
        (either source or forced source) is ≤ this limiting magnitude in
        the specified procver.

      lim_mag_band : str or None
        If None, then lim_mag will look at the most recent observation
        in any band.  Give a band here for lim_mag to only consider
        observations in that band.  Should be u, g, r, i, z, or Y.

      mjdnow : float or None
        For testing purposes: pretend that the current mjd is this value
        when pulling photometry and querying the wantedspectra table.
        If None, will use the current system time.  NOTE: this has
        implications for simulations; if your simulations have times in
        the future, they will be thrown out if you don't specify a
        mjdnow in the future!

      is_host : bool, default None
        Set this to True if you only want wantedspectra of transients,
        set it to False if you only want wantedspectra of hosts.  By
        default, return both

      logger : logging.Logger object or None
        Will use util.logger if None is passed

    Returns
    -------
      pandas.DataFrame with columns:
         root_diaobject_id
         diabojectid [ WARNING -- these aren't unique, so this is just a "random" one ]
         requester
         priority
         ra -- ra given by the requester
         dec -- dec given by the requester
         diaobj_meanra  -- weghted average of detection positions of transient.  DO NOT USE FOR HOST
         diaobj_meandec -- weighted average of detection positions of transient.  DO NOT USE FOR HOST.
         is_hsot -- True if the requester claimed this was a host position, False if a transient position
         src_mjd -- mjd of latest detection
         src_band -- band of latest detection
         src_mag -- magnitude (AB) of latest detection
         frced_mjd -- mjd of last forced-photometry point
         frced_band -- band of last forced-photometry point
         frced_mag -- magnitude (AB) of last forced-photometry point

    """

    logger = logger if logger is not None else util.logger

    now = datetime.datetime.now( tz=datetime.UTC )
    if mjdnow is not None:
        now = datetime.datetime.utcfromtimestamp( astropy.time.Time( mjdnow, format='mjd', scale='tai' ).unix_tai )
        now = pytz.utc.localize( now )

    with db.DBCon() as con:
        procver = util.procver_id( procver, dbcon=con )

        # Create a temporary table with things that are wanted but that have not been claimed.
        #
        # ROB THINK : the distinct on stuff.  What should / will happen if the same
        #   requester requests the same spectrum more than once?  Maybe a unique
        #   constraint in wantedspectra?

        con.execute_nofetch( "CREATE TEMP TABLE tmp_wanted( rootid UUID, is_host boolean, "
                             "ra double precision, dec double precision, requester text, priority int, "
                             "wanttime timestamp with time zone )",
                             explain=False, analyze=False )
        q = ( f"INSERT INTO tmp_wanted (\n"
              f"  SELECT DISTINCT ON(root_diaobject_id, requester, is_host)\n"
              f"    root_diaobject_id, is_host, ra, dec, requester, priority, wanttime\n"
              f"  FROM (\n"
              f"    SELECT w.root_diaobject_id, w.is_host, w.ra, w.dec, w.requester, w.priority, w.wanttime\n"
              f"           {',r.plannedspec_id' if notclaimsince is not None else ''}\n"
              f"    FROM wantedspectra w\n" )
        if notclaimsince is not None:
            q += ( "    LEFT JOIN plannedspectra r\n"
                   "      ON r.root_diaobject_id=w.root_diaobject_id AND r.is_host=w.is_host\n"
                   "        AND r.plantime>%(reqtime)s\n"
                   "  ) subq\n"
                   "  WHERE plannedspec_id IS NULL\n"
                  )
            whereand = "AND"
        else:
            q += "  ) subq\n"
            whereand = "WHERE"
        q += f"  {whereand} subq.wanttime<=%(now)s\n"
        if wantsince is not None:
            q += "    AND subq.wanttime>=%(wanttime)s\n"
        if requester is not None:
            q += "    AND requester=%(requester)s\n"
        if is_host is not None:
            q += "    AND is_host=%(is_host)s\n"
        q += "  ORDER BY root_diaobject_id, requester, is_host, wanttime DESC )"
        subdict =  { 'wanttime': wantsince, 'reqtime': notclaimsince, 'now': now, 'requester': requester,
                     'is_host': is_host }
        con.execute_nofetch( q, subdict )

        rows, _cols = con.execute( "SELECT COUNT(rootid) FROM tmp_wanted" )
        if rows[0][0] == 0:
            logger.debug( "Empty table tmp_wanted" )
            return pandas.DataFrame( [], columns=[ 'root_diaobject_id', 'requester', 'priority', 'wanttime',
                                                   'diaobjectid', 'is_host', 'ra', 'dec',
                                                   'src_mjd', 'src_band', 'src_mag',
                                                   'frced_mjd', 'frced_band', 'frced_mag' ] )
        else:
            logger.debug( f"{rows[0][0]} rows in tmp_wanted" )
        if _show_way_too_much_debug_info:
            rows, _cols = con.execute( "SELECT * FROM tmp_wanted" )
            sio = io.StringIO()
            sio.write( "Contents of tmp_wanted:\n" )
            sio.write( f"{'UUID':36s} {'requester':16s} priority\n" )
            sio.write( "------------------------------------ ---------------- --------\n" )
            for row in rows:
                sio.write( f"{str(row[0]):36s} {row[1]:16s} {row[2]:2d}\n" )
            logger.debug( sio.getvalue() )

        # Filter that table by throwing out things that have a spectruminfo whose mjd is greater than
        #   obstime.
        if nospecsince is None:
            con.execute_nofetch( "ALTER TABLE tmp_wanted RENAME TO tmp_wanted_no_spec", explain=False, analyze=False )
        else:
            con.execute_nofetch( "CREATE TEMP TABLE tmp_wanted_no_spec(\n"
                                 "  rootid UUID, is_host boolean, ra double precision,\n"
                                 "  dec double precision, requester text, priority int,\n"
                                "   wanttime timestamp with time zone)\n",
                                 explain=False, analyze=False )
            q = ( "/*+ IndexScan(s idx_spectruminfo_root_diaobject_id) */"
                  "INSERT INTO tmp_wanted_no_spec (\n"
                  "  SELECT DISTINCT ON(rootid,requester,is_host)\n"
                  "    rootid, is_host, ra, dec, requester, priority, wanttime\n"
                  "  FROM (\n"
                  "    SELECT t.rootid, t.is_host, t.ra, t.dec, t.requester,\n"
                  "           t.priority, s.specinfo_id, t.wanttime\n"
                  "    FROM tmp_wanted t\n"
                  "    LEFT JOIN spectruminfo s\n"
                  "      ON s.root_diaobject_id=t.rootid AND s.is_host=t.is_host\n"
                  "        AND s.mjd>=%(obstime)s AND s.mjd<=%(now)s\n"
                  "  ) subq\n"
                  "  WHERE specinfo_id IS NULL\n"
                  "  ORDER BY rootid, requester, is_host )\n" )
            con.execute_nofetch( q, { 'obstime': nospecsince, 'now': mjdnow } )

        row, _cols = con.execute( "SELECT COUNT(rootid) FROM tmp_wanted_no_spec" )
        if row[0][0] == 0:
            logger.debug( "Empty table tmp_wanted_no_spec" )
            return pandas.DataFrame( [], columns=[ 'root_diaobject_id', 'requester', 'priority', 'wanttime',
                                                   'diaobjectid', 'is_host', 'ra', 'dec',
                                                   'src_mjd', 'src_band', 'src_mag',
                                                   'frced_mjd', 'frced_band', 'frced_mag' ] )
        else:
            logger.debug( f"{row[0][0]} rows in tmp_wanted_no_spec" )
        if _show_way_too_much_debug_info:
            rows, _cols = con.execute( "SELECT * FROM tmp_wanted_no_spec" )
            sio = io.StringIO()
            sio.write( "Contents of tmp_wanted2:\n" )
            sio.write( "------------------------------------ ---------------- --------\n" )
            sio.write( f"{'UUID':36s} {'requester':16s} priority\n" )
            for row in rows:
                sio.write( f"{str(row[0]):36s} {row[1]:16s} {row[2]:2d}\n" )
            logger.debug( sio.getvalue() )

        # Pull down everything into a pandas dataframe
        rows, cols = con.execute( "SELECT * FROM tmp_wanted_no_spec" )
        df = util.laboriously_construct_pandas( rows, columns=cols, doublecols=['ra', 'dec'],
                                                int16cols=['priority'], ignore_missing_cols=True )
        df.set_index( 'rootid', inplace=True )

        # OK, this is a little profligate.  We could definitely pull less from postgres by doing
        #   the "find latest detection" in SQL (and indeed I used to do it that way).  Not clear
        #   if that would be faster, but this is simpler to code!

        srcltcvs, objinfo = ltcv.many_object_ltcvs( processing_version=procver, which='detections',
                                                    objids_table='tmp_wanted_no_spec', return_format='pandas',
                                                    return_object_info=True, include_object_positions=True,
                                                    always_use_weighted_source_positions=True,
                                                    mjd_now=mjdnow, dbcon=con )
        frcltcvs = ltcv.many_object_ltcvs( processing_version=procver, which='forced',
                                           objids_table='tmp_wanted_no_spec', return_format='pandas',
                                           return_object_info=False, mjd_now=mjdnow, dbcon=con )

    srcltcvs.reset_index( inplace=True )
    frcltcvs.reset_index( inplace=True )

    # Remove unwanted columns
    yanks = [ i for i in srcltcvs.columns if i not in [ 'rootid', 'mjd', 'band', 'flux' ] ]
    srcltcvs.drop( yanks, axis='columns', inplace=True )
    yanks = [ i for i in frcltcvs.columns if i not in [ 'rootid', 'mjd', 'band', 'flux' ] ]
    frcltcvs.drop( yanks, axis='columns', inplace=True )

    # Extract latest row for each object in srcltcvs and frcltcvs
    srcltcvs = srcltcvs.loc[ srcltcvs.groupby(["rootid", "band"])["mjd"].idxmax() ]
    frcltcvs = frcltcvs.loc[ frcltcvs.groupby(["rootid", "band"])["mjd"].idxmax() ]

    # Magnitudes
    for photdf in [ srcltcvs, frcltcvs ]:
        photdf['mag'] = 99.
        photdf.loc[ photdf['flux'] > 0, 'mag' ] = (
            -2.5 * np.log10( photdf.loc[ photdf['flux'] > 0, 'flux' ] ) + 31.4 )
        photdf['mag'] = 99.
        photdf.loc[ photdf['flux'] > 0, 'mag' ] = (
            -2.5 * np.log10( photdf.loc[ photdf['flux'] > 0, 'flux' ] ) + 31.4 )
        photdf.drop( [ 'flux' ] , axis='columns', inplace=True )
    srcltcvs.rename( { 'mjd': 'src_mjd', 'band': 'src_band', 'mag': 'src_mag' }, axis='columns', inplace=True )
    frcltcvs.rename( { 'mjd': 'frced_mjd', 'band': 'frced_band', 'mag': 'frced_mag' }, axis='columns', inplace=True )

    # Filter by limiting magnitude if necessary
    if lim_mag is not None:
        if lim_mag_band is not None:
            # We should only have (at most) one magntiude for each band
            lim_srcltcvs = srcltcvs.loc[ srcltcvs.src_band == lim_mag_band, [ "rootid", "src_mjd", "src_mag" ] ]
            lim_frcltcvs = frcltcvs.loc[ frcltcvs.frced_band == lim_mag_band,[ "rootid", "frced_mjd", "frced_mag" ] ]
        else:
            lim_srcltcvs = srcltcvs.loc[ srcltcvs.groupby(["rootid"])["src_mjd"].idxmax(),
                                        [ "rootid", "src_mjd", "src_mag" ] ]
            lim_frcltcvs = frcltcvs.loc[ frcltcvs.groupby(["rootid"])["frced_mjd"].idxmax(),
                                        [ "rootid", "frced_mjd", "frced_mag" ] ]

        lim_srcltcvs.set_index( 'rootid', inplace=True )
        lim_frcltcvs.set_index( 'rootid', inplace=True )
        lim_ltcvs = lim_srcltcvs.join( lim_frcltcvs, how='outer' )
        lim_ltcvs.loc[ : , 'mag_for_cut' ] = lim_ltcvs.src_mag
        # WORRY : isnull(), NaN, etc.
        forcednewer = ( ( lim_ltcvs.mag_for_cut.isnull() & ( ~lim_ltcvs.frced_mag.isnull() ) ) |
                        ( ( ( ~lim_ltcvs.mag_for_cut.isnull() ) & ( ~lim_ltcvs.frced_mag.isnull() ) )
                          & ( lim_ltcvs.frced_mjd > lim_ltcvs.src_mjd ) ) )
        lim_ltcvs.loc[ forcednewer, 'mag_for_cut' ] = lim_ltcvs.loc[ forcednewer, 'frced_mag' ]
        lim_ltcvs = lim_ltcvs.loc[ :, [ 'mag_for_cut' ] ]
        lim_ltcvs = lim_ltcvs[ lim_ltcvs.mag_for_cut <= lim_mag ]

        # This will remove anything from df that doesn't have a rootid in lim_ltcvs
        df = df.join( lim_ltcvs, how='inner' )
        df.drop( ['mag_for_cut'], axis='columns', inplace=True )

    # Keep only the latest lightcurve point independet of band
    srcltcvs = srcltcvs.loc[ srcltcvs.groupby(["rootid"])["src_mjd"].idxmax() ]
    frcltcvs = frcltcvs.loc[ frcltcvs.groupby(["rootid"])["frced_mjd"].idxmax() ]

    # If necessary, throw out things that do not have a detection since detsince
    if detsince is not None:
        srcltcvs = srcltcvs[ srcltcvs.src_mjd >= detsince ]

    # Throw out stuff we don't want from objinfo
    objinfo.reset_index( inplace=True )
    yanks = [ i for i in objinfo.columns if i not in [ 'rootid', 'diaobjectid', 'ra', 'dec' ] ]
    objinfo.drop( yanks, axis='columns', inplace=True )
    objinfo = objinfo.groupby( 'rootid' ).agg( 'first' )
    objinfo.rename( { 'ra': 'diaobj_meanra', 'dec': 'diaobj_meandec' }, axis='columns', inplace=True )

    # Join to latest mags.  We *assume* there are detections, otherwise nobody would want a spectrum.
    # Also, we wouldn't have heard about the object in the first place.
    srcltcvs.set_index( 'rootid', inplace=True )
    df = df.join( srcltcvs, how='inner' )
    df.rename( { 'mjd': 'src_mjd', 'flux': 'src_flux', 'band': 'src_band' }, axis='columns', inplace=True )
    frcltcvs.set_index( 'rootid', inplace=True )
    df = df.join( frcltcvs, how='left' )
    df.rename( { 'mjd': 'frced_mjd', 'flux': 'frced_flux', 'band': 'frced_band' }, axis='columns', inplace=True )

    # Join to obinfo to get ra/dec
    df = df.join( objinfo, how='left' )

    # Return
    df.reset_index( inplace=True )
    df.rename( { 'rootid': 'root_diaobject_id' }, axis='columns', inplace=True )
    return df


def get_spectrum_info( logger=None, **kwargs ):
    if logger is None:
        logger = logging.getLogger( __name__ )
        logger.propagate = False
        if not logger.hasHandlers():
            logout = logging.StreamHandler( sys.stderr )
            logger.addHandler( logout )
            formatter = logging.Formatter( '[%(asctime)s - what_spectra_are_wanted - %(levelname)s] - %(message)s',
                                           datefmt='%Y-%m-%d %H:%M:%S' )
            logout.setFormatter( formatter )
            logger.setLevel( logging.INFO )

    with db.DBCon() as con:
        q = sql.SQL( "SELECT * FROM spectruminfo " )

        # Backwards compatibility
        if 'since' in kwargs:
            kwargs['inserted_at_min'] = kwargs['since']
            del kwargs['since']
        if 'root_diaobject_ids' in kwargs:
            kwargs['root_diaobject_id'] = kwargs['root_diaobject_ids']
            del kwargs['root_diaobject_ids']

        searchspec = {
            'root_diaobject_id':  { 'mult': True,  'substr': False, 'minmax': False },
            'facility':           { 'mult': True,  'substr': True,  'minmax': True },
            'mjd':                { 'mult': False, 'substr': False, 'minmax': True },
            'z':                  { 'mult': False, 'substr': False, 'minmax': True },
            'class_description':  { 'mult': True,  'substr': True,  'minmax': False },
            'classid':            { 'mult': True,  'substr': False, 'minmax': True },
            'is_host':            { 'mult': False, 'substr': False, 'minmax': False },
            'inserted_at':        { 'mult': False, 'substr': False, 'minmax': True }
        }

        whereq, subdict, leftovers, _where = db.construct_pgsql_where_clause( searchspec, **kwargs )
        if len(leftovers) != 0:
            raise ValueError( "Unknown arguments: {leftovers}" )

        q += whereq

        rows, cols = con.execute( q, subdict )
        df = pandas.DataFrame( rows, columns=cols )

    return df
