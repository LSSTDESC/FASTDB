import sys
import io
import datetime
import pytz
import logging
import collections

import psycopg
import pandas
import astropy.time

import db

# Want this to be False except when
#  doing deep-in-the-weeds debugging
_show_way_too_much_debug_info = False


def what_spectra_are_wanted( procver=None, wantsince=None, requester=None, notclaimsince=None,
                             nospecsince=None, detsince=None, lim_mag=None, lim_mag_band=None,
                             mjdnow=None, logger=None ):
    """Find out what spectra have been requested

    Parmeters
    ---------
      procver : str or None
        The processing version or alias to look at photometry for.  If
        not given, will glom together a mishmash of all processing
        versions.  (This is not as bad as it sounds.  For most use cases
        of this function, you're looking for recent real-time
        photometry, so there will only be the most recent realtime data.
        TODO: think about a "realtime" processing version alias.)

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
        since this mjd.

      lim_mag : float or None
        If not None, only return objects whose most recent observation
        (either source or forced source) is ≤ this limiting magnitude.

      lim_mag_band : str or None
        If None, then lim_mag will look at the most recent observation
        in any band.  Give a band here for lim_mag to only consider
        observations in that band.  Should be u, g, r, i, z, or Y.

      mjdnow : float or None
        For testing purposes: pretend that the current mjd is this value
        when pulling photometry.

      logger : logging.Logger object or None
        Will make a default if one is not given

    Returns
    -------
      pandas dataframe TODO DOCUMENT


    """

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

    now = datetime.datetime.now( tz=datetime.UTC )
    if mjdnow is not None:
        now = datetime.datetime.utcfromtimestamp( astropy.time.Time( mjdnow, format='mjd', scale='tai' ).unix_tai )
        now = pytz.utc.localize( now )

    with db.DB() as con:
        cursor = con.cursor()

        # If a processing version was given, turn it into a number
        if procver is not None:
            # TODO : validity date range?
            cursor.execute( "SELECT id FROM processing_version WHERE description=%(procver)s",
                            { 'procver': procver } )
            rows = cursor.fetchall()
            if len(rows) == 0:
                cursor.execute( "SELECT id FROM processing_version_alias WHERE description=%(procver)s",
                                { 'procver': procver } )
                rows = cursor.fetchall()
            if len(rows) == 0:
                return f"Error, unknown processing version {procver}", 500
            procver = rows[0][0]

        # Create a temporary table things that are wanted but that have not been claimed.
        #
        # ROB THINK : the distinct on stuff.  What should / will happen if the same
        #   requester requests the same spectrum more than once?  Maybe a unique
        #   constraint in wantedspectra?

        cursor.execute( "CREATE TEMP TABLE tmp_wanted( root_diaobject_id UUID, requester text, priority int )" )
        q = ( f"INSERT INTO tmp_wanted ( "
              f"  SELECT DISTINCT ON(root_diaobject_id,requester,priority) root_diaobject_id, requester, priority "
              f"  FROM ( "
              f"    SELECT w.root_diaobject_id, w.requester, w.priority, w.wanttime "
              f"           {',r.plannedspec_id' if notclaimsince is not None else ''} "
              f"    FROM wantedspectra w " )
        if notclaimsince is not None:
            q += ( "    LEFT JOIN plannedspectra r "
                   "      ON r.root_diaobject_id=w.root_diaobject_id AND r.created_at>%(reqtime)s "
                   "  ) subq "
                   "  WHERE plannedspec_id IS NULL "
                  )
            whereand = "AND"
        else:
            q += "  ) subq "
            whereand = "WHERE"
        q += f"  {whereand} subq.wanttime<=%(now)s "
        if wantsince is not None:
            q += "    AND subq.wanttime>=%(wanttime)s "
        if requester is not None:
            q += "    AND requester=%(requester)s "
        q += "  GROUP BY root_diaobject_id,requester,priority )"
        subdict =  { 'wanttime': wantsince, 'reqtime': notclaimsince, 'now': now, 'requester': requester }
        tmpcur = psycopg.ClientCursor( con )
        logger.debug( f"Sending query: {tmpcur.mogrify(q,subdict)}" )
        cursor.execute( q, subdict )

        cursor.execute( "SELECT COUNT(root_diaobject_id) FROM tmp_wanted" )
        row = cursor.fetchall()
        if row[0][0] == 0:
            logger.debug( "Empty table tmp_wanted" )
            return { 'status': 'ok', 'wantedspectra': [] }
        else:
            logger.debug( f"{row[0][0]} rows in tmp_wanted" )
        if _show_way_too_much_debug_info:
            cursor.execute( "SELECT * FROM tmp_wanted" )
            sio = io.StringIO()
            sio.write( "Contents of tmp_wanted:\n" )
            sio.write( f"{'UUID':36s} {'requester':16s} priority\n" )
            sio.write( "------------------------------------ ---------------- --------\n" )
            for row in cursor.fetchall():
                sio.write( f"{str(row[0]):36s} {row[1]:16s} {row[2]:2d}\n" )
            logger.debug( sio.getvalue() )

        # Filter that table by throwing out things that have a spectruminfo whose mjd is greater than
        #   obstime.
        if nospecsince is None:
            cursor.execute( "ALTER TABLE tmp_wanted RENAME TO tmp_wanted2" )
        else:
            cursor.execute( "CREATE TEMP TABLE tmp_wanted2( root_diaobject_id UUID, "
                            "                               requester text, priority int ) " )
            q = ( "INSERT INTO tmp_wanted2 ( "
                  "  SELECT DISTINCT ON(root_diaobject_id,requester,priority) root_diaobject_id, requester, "
                  "                                                           priority "
                  "  FROM ( "
                  "    SELECT t.root_diaobject_id, t.requester, t.priority, s.specinfo_id "
                  "    FROM tmp_wanted t "
                  "    LEFT JOIN spectruminfo s "
                  "      ON s.root_diaobject_id=t.root_diaobject_id AND s.mjd>=%(obstime)s AND s.mjd<=%(now)s "
                  "  ) subq "
                  "  WHERE specinfo_id IS NULL "
                  "  GROUP BY root_diaobject_id, requester, priority )" )
            cursor.execute( q, { 'obstime': nospecsince, 'now': mjdnow } )

        cursor.execute( "SELECT COUNT(root_diaobject_id) FROM tmp_wanted2" )
        row = cursor.fetchall()
        if row[0][0] == 0:
            logger.debug( "Empty table tmp_wanted2" )
            return { 'status': 'ok', 'wantedspectra': [] }
        else:
            logger.debug( f"{row[0][0]} rows in tmp_wanted2" )
        if _show_way_too_much_debug_info:
            cursor.execute( "SELECT * FROM tmp_wanted2" )
            sio = io.StringIO()
            sio.write( "Contents of tmp_wanted2:\n" )
            sio.write( "------------------------------------ ---------------- --------\n" )
            sio.write( f"{'UUID':36s} {'requester':16s} priority\n" )
            for row in cursor.fetchall():
                sio.write( f"{str(row[0]):36s} {row[1]:16s} {row[2]:2d}\n" )
            logger.debug( sio.getvalue() )

        # Filter that table by throwing out things that do not have a detection since detsince
        if detsince is None:
            cursor.execute( "ALTER TABLE tmp_wanted2 RENAME TO tmp_wanted3" )
        else:
            cursor.execute( "CREATE TEMP TABLE tmp_wanted3( root_diaobject_id UUID, requester text, "
                            "                               priority int ) " )
            q = ( "INSERT INTO tmp_wanted3 ( "
                  "  SELECT DISTINCT ON(t.root_diaobject_id,requester,priority) "
                  "    t.root_diaobject_id, requester, priority "
                  "  FROM tmp_wanted2 t "
                  "  INNER JOIN diaobject_root_map dorm ON t.root_diaobject_id=dorm.rootid "
                  "  INNER JOIN diasource s ON ( dorm.diaobjectid=s.diaobjectid AND "
                  "                              dorm.processing_version=s.diaobject_procver " )
            if procver is not None:
                q += "                           AND s.processing_version=%(procver) "
            q += ( "                                                                )"
                   " WHERE s.midpointmjdtai>=%(detsince)s AND s.midpointmjdtai<=%(now)s"
                   " ORDER BY root_diaobject_id,requester,priority )" )
            cursor.execute( q, { 'detsince': detsince, 'procver': procver, 'now': mjdnow } )

        cursor.execute( "SELECT COUNT(root_diaobject_id) FROM tmp_wanted3" )
        row = cursor.fetchall()
        if row[0][0] == 0:
            logger.debug( "Empty table tmp_wanted3" )
            return { 'status': 'ok', 'wantedspectra': [] }
        else:
            logger.debug( f"{row[0][0]} rows in tmp_wanted3\n" )
        if _show_way_too_much_debug_info:
            cursor.execute( "SELECT * FROM tmp_wanted3" )
            sio = io.StringIO()
            sio.write( "Contents of tmp_wanted3:\n" )
            sio.write( f"{'UUID':36s} {'requester':16s} priority\n" )
            sio.write( "------------------------------------ ---------------- --------\n" )
            for row in cursor.fetchall():
                sio.write( f"{str(row[0]):36s} {row[1]:16s} {row[2]:2d}\n" )
            logger.debug( sio.getvalue() )


        # Get the latest *detection* (source) for the objects
        cursor.execute( "CREATE TEMP TABLE tmp_latest_detection( root_diaobject_id UUID, "
                        "                                        mjd double precision, "
                        "                                        band text, mag real ) " )
        q = ( "INSERT INTO tmp_latest_detection ( "
              "  SELECT root_diaobject_id, mjd, band, mag "
              "  FROM ( "
              "    SELECT DISTINCT ON (t.root_diaobject_id) t.root_diaobject_id,"
              "           s.band AS band, s.midpointmjdtai AS mjd, "
              "           CASE WHEN s.psfflux>0 THEN -2.5*LOG(s.psfflux)+31.4 ELSE 99 END AS mag "
              "    FROM tmp_wanted3 t "
              "    INNER JOIN diaobject_root_map r ON t.root_diaobject_id=r.rootid "
              "    INNER JOIN diasource s ON r.diaobjectid=s.diaobjectid "
              "                           AND r.processing_version=s.diaobject_procver "
              "    WHERE s.midpointmjdtai<=%(now)s " )
        if procver is not None:
            q += "    AND s.processing_version=%(procver)s "
        if lim_mag_band is not None:
            q += "    AND s.band=%(band)s "
        q += "    ORDER BY t.root_diaobject_id,mjd DESC ) subq ) "
        cursor.execute( q, { 'procver': procver, 'band': lim_mag_band, 'now': mjdnow } )

        cursor.execute( "SELECT COUNT(*) FROM tmp_latest_detection" )
        logger.debug( f"{cursor.fetchone()[0]} rows in tmp_latest_detection" )
        if _show_way_too_much_debug_info:
            cursor.execute( "SELECT root_diaobject_id,mjd,band,mag FROM tmp_latest_detection" )
            sio = io.StringIO()
            sio.write( "Contents of tmp_latest_detection:\n" )
            sio.write( f"{'UUID':36s} {'mjd':8s} {'band':6s} {'mag':6s}\n" )
            sio.write( "------------------------------------ -------- ------ ------\n" )
            for row in cursor.fetchall():
                sio.write( f"{str(row[0]):36s} {row[1]:8.2f} {row[2]:6s} {row[3]:6.2f}\n" )
            logger.debug( sio.getvalue() )

        # Get the latest forced source for the objects
        cursor.execute( "CREATE TEMP TABLE tmp_latest_forced( root_diaobject_id UUID, "
                        "                                     mjd double precision, "
                        "                                     band text, mag real ) " )
        q = ( "INSERT INTO tmp_latest_forced ( "
              "  SELECT root_diaobject_id, mjd, band, mag "
              "  FROM ( "
              "    SELECT DISTINCT ON (t.root_diaobject_id) t.root_diaobject_id,"
              "           f.band AS band, f.midpointmjdtai AS mjd, "
              "           CASE WHEN f.psfflux>0 THEN -2.5*LOG(f.psfflux)+31.4 ELSE 99 END AS mag "
              "    FROM tmp_wanted3 t "
              "    INNER JOIN diaobject_root_map r ON t.root_diaobject_id=r.rootid "
              "    INNER JOIN diaforcedsource f ON r.diaobjectid=f.diaobjectid "
              "                                 AND r.processing_version=f.diaobject_procver "
              "    WHERE f.midpointmjdtai<=%(now)s " )
        if procver is not None:
            q += "      AND f.processing_version=%(procver)s "
        if lim_mag_band is not None:
            q += "     AND f.band=%(band)s "
        q += "    ORDER BY t.root_diaobject_id,mjd DESC ) AS subq ) "
        cursor.execute( q, { 'procver': procver, 'band': lim_mag_band, 'now': mjdnow } )

        cursor.execute( "SELECT COUNT(*) FROM tmp_latest_forced" )
        logger.debug( f"{cursor.fetchone()[0]} rows in tmp_latest_forced" )
        if _show_way_too_much_debug_info:
            cursor.execute( "SELECT root_diaobject_id,mjd,band,mag FROM tmp_latest_forced" )
            sio = io.StringIO()
            sio.write( "Contents of tmp_latest_forced:\n" )
            sio.write( f"{'UUID':36s} {'mjd':8s} {'band':6s} {'mag':6s}\n" )
            sio.write( "------------------------------------ -------- ------ ------\n" )
            for row in cursor.fetchall():
                sio.write( f"{str(row[0]):36s} {row[1]:8.2f} {row[2]:6s} {row[3]:6.2f}\n" )
            logger.debug( sio.getvalue() )

        # Get object info.  Notice that if a processing version wasn't requested,
        #   you get a semi-random one....
        cursor.execute( "CREATE TEMP TABLE tmp_object_info( root_diaobject_id UUID, requester text, "
                        "                                   priority smallint, diaobjectid bigint, "
                        "                                   processing_version int, "
                        "                                   ra double precision, dec double precision )" )
        q = ( "INSERT INTO tmp_object_info ( "
              "  SELECT DISTINCT ON (t.root_diaobject_id) t.root_diaobject_id, t.requester, "
              "                                           t.priority, o.diaobjectid, o.processing_version, "
              "                                           o.ra, o.dec "
              "  FROM tmp_wanted3 t "
              "  INNER JOIN diaobject_root_map dorm ON dorm.rootid=t.root_diaobject_id "
              "  INNER JOIN diaobject o ON dorm.diaobjectid=o.diaobjectid "
              "                         AND dorm.processing_version=o.processing_version " )
        if procver is not None:
            q += "  WHERE o.processing_version=%(procver)s "
        q += ")"
        cursor.execute( q, { 'procver': procver } )

        cursor.execute( "SELECT COUNT(*)_ FROM tmp_object_info" )
        logger.debug( f"{cursor.fetchone()[0]} rows in tmp_object_info" )
        if _show_way_too_much_debug_info:
            cursor.execute( "SELECT root_diaobject_id,requester,priority,diaobjectid,processing_version,ra,dec "
                            "FROM tmp_object_info" )
            sio = io.StringIO()
            sio.write( "Contents of tmp_object_info:\n" )
            sio.write( f"{'UUID':36s} {'requester':16s} {'prio':4s} {'diaobjectid':12s} {'pver':4s} "
                       f"{'ra':8s} {'dec':8s}\n" )
            sio.write( "------------------------------------ ---------------- ---- ------------ ---- "
                       "-------- --------\n" )
            for row in cursor.fetchall():
                sio.write( f"{str(row[0]):36s} {row[1]:16s} {row[2]:4d} {row[3]:12d} {row[4]:4d} "
                           f"{row[5]:8.4f} {row[6]:8.4f}\n" )
            logger.debug( sio.getvalue() )

        # Join all the things and pull
        q = ( "SELECT t.root_diaobject_id, t.requester, t.priority, o.ra, o.dec, "
              "       s.mjd AS src_mjd, s.band AS src_band, s.mag AS src_mag, "
              "       f.mjd AS frced_mjd, f.band AS frced_band, f.mag AS frced_mag "
              "FROM tmp_wanted3 t "
              "INNER JOIN tmp_object_info o ON t.root_diaobject_id=o.root_diaobject_id "
              "LEFT JOIN tmp_latest_detection s ON t.root_diaobject_id=s.root_diaobject_id "
              "LEFT JOIN tmp_latest_forced f ON t.root_diaobject_id=f.root_diaobject_id" )
        cursor.execute( q )
        columns = [ c.name for c in cursor.description ]
        df = pandas.DataFrame( cursor.fetchall(), columns=columns )

    # Filter by limiting magnitude if necessary
    if lim_mag is not None:
        df['forcednewer'] = ( ( ( ~df['src_mjd'].isnull() ) & ( ~df['frced_mjd'].isnull() )
                                  & ( df['frced_mjd']>=df['src_mjd'] ) )
                              |
                              ( ( df['src_mjd'].isnull() ) & ( ~df['frced_mjd'].isnull() ) ) )
        if _show_way_too_much_debug_info:
            widthbu = pandas.options.display.width
            maxcolbu = pandas.options.display.max_columns
            pandas.options.display.width = 4096
            pandas.options.display.max_columns = None
            debugdf = df.loc[ :, ['root_diaobject_id','src_mjd','src_band','src_mag',
                                  'frced_mjd','frced_band','frced_mag','forcednewer'] ]
            logger.debug( f"df:\n{debugdf}" )
            pandas.options.display.width = widthbu
            pandas.options.display.max_columns = maxcolbu
        df = df[ ( df['forcednewer'] & ( df['frced_mag'] <= lim_mag ) )
                 |
                 ( (~df['forcednewer']) & ( df['src_mag'] <= lim_mag ) ) ]

    return df

    # Build the return structure
    retarr = []
    for row in df.itertuples():
        retarr.append( { 'oid': row.root_diaobject_id,
                         'ra': float( row.ra ),
                         'dec': float( row.dec ),
                         'req': row.requester,
                         'prio': int( row.priority ),
                         'latest_source_band': row.src_band,
                         'latest_source_mjd': row.src_mjd,
                         'latest_source_mag': row.src_mag,
                         'latest_forced_band': row.frced_band,
                         'latest_forced_mjd': row.frced_mjd,
                         'latest_forced_mag': row.frced_mag } )

    return { 'status': 'ok', 'wantedspectra': retarr }


def get_spectrum_info( rootids=None, facility=None, mjd_min=None, mjd_max=None, classid=None,
                       z_min=None, z_max=None, since=None, logger=None ):
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

    with db.DB() as con:
        cursor = con.cursor()
        where = "WHERE"
        q = "SELECT * FROM spectruminfo "
        subdict = {}

        if rootids is not None:
            if ( isinstance( rootids, collections.abc.Sequence ) and not ( isinstance( rootids, str ) ) ):
                q += f"{where} root_diaobject_id=ANY(%(ids)s) "
                subdict['ids'] = [ str(i) for i in rootids ]
            else:
                q += f"{where} root_diaobject_id=%(id)s "
                subdict['id'] = str(rootids)
            where = "AND"

        if facility is not None:
            q += f"{where} facility=%(fac)s "
            subdict['fac'] = facility
            where = "AND"

        if mjd_min is not None:
            q += f"{where} mjd>=%(mjdmin)s "
            subdict['mjdmin'] = mjd_min
            where = "AND"

        if mjd_max is not None:
            q += f"{where} mjd<=%(mjdmax)s "
            subdict['mjdmax'] = mjd_max
            where = "AND"

        if classid is not None:
            q += f"{where} classid=%(class)s "
            subdict['class'] = classid
            where = "AND"

        if z_min is not None:
            q += f"{where} z>=%(zmin)s "
            subdict['zmin'] = z_min
            where = "AND"

        if z_max is not None:
            q += f"{where} z<=%(zmax)s "
            subdict['zmax'] = z_max
            where = "AND"

        if since is not None:
            q += f"{where} inserted_at>=%(since)s "
            subdict['since'] = since
            where = "AND"

        tmpcur = psycopg.ClientCursor( con )
        logger.debug( f"Sending query: {tmpcur.mogrify(q,subdict)}" )

        cursor.execute( q, subdict )
        columns = [ col.name for col in cursor.description ]
        df = pandas.DataFrame( cursor.fetchall(), columns=columns )

    return df
