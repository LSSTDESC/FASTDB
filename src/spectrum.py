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
import util

# Want this to be False except when
#  doing deep-in-the-weeds debugging
_show_way_too_much_debug_info = False


def what_spectra_are_wanted( procver='default', wantsince=None, requester=None, notclaimsince=None,
                             nospecsince=None, detsince=None, lim_mag=None, lim_mag_band=None,
                             mjdnow=None, logger=None ):
    """Find out what spectra have been requested

    Parmeters
    ---------
      procver : str, default 'default'
        The processing version or alias to look at photometry for.  Used
        to filter the diasource, diaforcedsource, and diaobject tables.
        Note!  You may not want to use the default here; we will
        probably have a 'realtime' processing version, or 'alerts', or
        some such, and you probably want to search that.

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
        Will use util.logger if None is passed

    Returns
    -------
      pandas dataframe TODO DOCUMENT

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

        con.execute_nofetch( "CREATE TEMP TABLE tmp_wanted( root_diaobject_id UUID, requester text, priority int )" )
        q = ( f"INSERT INTO tmp_wanted (\n"
              f"  SELECT DISTINCT ON(root_diaobject_id,requester,priority) root_diaobject_id, requester, priority\n"
              f"  FROM (\n"
              f"    SELECT w.root_diaobject_id, w.requester, w.priority, w.wanttime\n"
              f"           {',r.plannedspec_id' if notclaimsince is not None else ''}\n"
              f"    FROM wantedspectra w\n" )
        if notclaimsince is not None:
            q += ( "    LEFT JOIN plannedspectra r\n"
                   "      ON r.root_diaobject_id=w.root_diaobject_id AND r.created_at>%(reqtime)s\n"
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
        q += "  GROUP BY root_diaobject_id,requester,priority )"
        subdict =  { 'wanttime': wantsince, 'reqtime': notclaimsince, 'now': now, 'requester': requester }
        con.execute_nofetch( q, subdict )

        rows, _cols = con.execute( "SELECT COUNT(root_diaobject_id) FROM tmp_wanted" )
        if rows[0][0] == 0:
            logger.debug( "Empty table tmp_wanted" )
            return { 'status': 'ok', 'wantedspectra': [] }
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
            con.execute_nofetch( "ALTER TABLE tmp_wanted RENAME TO tmp_wanted2" )
        else:
            con.execute_nofetch( "CREATE TEMP TABLE tmp_wanted2( root_diaobject_id UUID,\n"
                                 "                               requester text, priority int )\n" )
            q = ( "/*+ IndexScan(s idx_spectruminfo_root_diaobject_id) */"
                  "INSERT INTO tmp_wanted2 (\n"
                  "  SELECT DISTINCT ON(root_diaobject_id,requester,priority) root_diaobject_id, requester,\n"
                  "                                                           priority\n"
                  "  FROM (\n"
                  "    SELECT t.root_diaobject_id, t.requester, t.priority, s.specinfo_id\n"
                  "    FROM tmp_wanted t\n"
                  "    LEFT JOIN spectruminfo s\n"
                  "      ON s.root_diaobject_id=t.root_diaobject_id AND s.mjd>=%(obstime)s AND s.mjd<=%(now)s\n"
                  "  ) subq\n"
                  "  WHERE specinfo_id IS NULL\n"
                  "  GROUP BY root_diaobject_id, requester, priority )" )
            con.execute_nofetch( q, { 'obstime': nospecsince, 'now': mjdnow } )

        row, _cols = con.execute( "SELECT COUNT(root_diaobject_id) FROM tmp_wanted2" )
        if row[0][0] == 0:
            logger.debug( "Empty table tmp_wanted2" )
            return { 'status': 'ok', 'wantedspectra': [] }
        else:
            logger.debug( f"{row[0][0]} rows in tmp_wanted2" )
        if _show_way_too_much_debug_info:
            rows, _cols = con.execute( "SELECT * FROM tmp_wanted2" )
            sio = io.StringIO()
            sio.write( "Contents of tmp_wanted2:\n" )
            sio.write( "------------------------------------ ---------------- --------\n" )
            sio.write( f"{'UUID':36s} {'requester':16s} priority\n" )
            for row in rows:
                sio.write( f"{str(row[0]):36s} {row[1]:16s} {row[2]:2d}\n" )
            logger.debug( sio.getvalue() )

        # Filter that table by throwing out things that do not have a detection since detsince
        if detsince is None:
            con.execute_nofetch( "ALTER TABLE tmp_wanted2 RENAME TO tmp_wanted3" )
        else:
            con.execute_nofetch( "CREATE TEMP TABLE tmp_wanted3( root_diaobject_id UUID, requester text, "
                                 "                               priority int )\n" )
            q = ( "/*+ IndexScan(src idx_diasource_diaobjectid) */\n"
                  "INSERT INTO tmp_wanted3 (\n"
                  "  SELECT DISTINCT ON(t.root_diaobject_id,requester,priority)\n"
                  "    t.root_diaobject_id, requester, priority\n"
                  "  FROM tmp_wanted2 t\n"
                  "  INNER JOIN diaobject o ON t.root_diaobject_id=o.rootid\n"
                  "  INNER JOIN (\n"
                  "    SELECT DISTINCT ON(src.diaobjectid,src.visit) src.diaobjectid\n"
                  "    FROM diasource src\n"
                  "    INNER JOIN base_procver_of_procver pv ON src.base_procver_id=pv.base_procver_id\n"
                  "      AND pv.procver_id=%(procver)s\n"
                  "    WHERE src.diaobjectid=o.diaobjectid\n"
                  "      AND s.midpointmjdtai>=%(detsince)s AND s.midpointmjdtai<=%(now)s\n"
                  "    ORDER BY src.diaobjectid,src.visit,pv.priority DESC\n"
                  "  ) s ON o.diaobjectid=s.diaobjectid\n"
                  "  ORDER BY root_diaobject_id,requester,priority\n"
                  ")" )
            con.execute_nofetch( q, { 'detsince': detsince, 'procver': procver, 'now': mjdnow } )

        row, _cols = con.execute( "SELECT COUNT(root_diaobject_id) FROM tmp_wanted3" )
        if row[0][0] == 0:
            logger.debug( "Empty table tmp_wanted3" )
            return { 'status': 'ok', 'wantedspectra': [] }
        else:
            logger.debug( f"{row[0][0]} rows in tmp_wanted3\n" )
        if _show_way_too_much_debug_info:
            rows, _cols = con.execute( "SELECT * FROM tmp_wanted3" )
            sio = io.StringIO()
            sio.write( "Contents of tmp_wanted3:\n" )
            sio.write( f"{'UUID':36s} {'requester':16s} priority\n" )
            sio.write( "------------------------------------ ---------------- --------\n" )
            for row in rows:
                sio.write( f"{str(row[0]):36s} {row[1]:16s} {row[2]:2d}\n" )
            logger.debug( sio.getvalue() )


        # Get the latest *detection* (source) for the objects
        con.execute_nofetch( "CREATE TEMP TABLE tmp_latest_detection( root_diaobject_id UUID,\n"
                             "                                        mjd double precision,\n"
                             "                                        band text, mag real )" )
        q = ( "/*+ IndexScan(src idx_diasource_diaobjectid) */\n"
              "INSERT INTO tmp_latest_detection (\n"
              "  SELECT root_diaobject_id, diaobjectid, mjd, band, mag\n"
              "  FROM (\n"
              "    SELECT DISTINCT ON (t.root_diaobject_id) t.root_diaobject_id, s.diaobjectid,\n"
              "           s.band AS band, s.midpointmjdtai AS mjd,\n"
              "           CASE WHEN s.psfflux>0 THEN -2.5*LOG(s.psfflux)+31.4 ELSE 99 END AS mag\n"
              "    FROM tmp_wanted3 t\n"
              "    INNER JOIN diaobject o ON t.root_diaobject_id=o.rootid\n"
              "    INNER JOIN (\n"
              "      SELECT DISTINCT ON (src.diaobjectid,src.visit) src.diaobjectid,src.midpointmjdtai,\n"
              "                                                     src.psfflux,src.band\n"
              "      FROM diasource src\n"
              "      INNER JOIN base_procver_of_procver pv ON src.base_procver_id=pv.base_procver_id\n"
              "        AND pv.procver_id=%(procver)s\n"
              "      WHERE src.midpointmjdtai<=%(now)s\n" )
        if lim_mag_band is not None:
            q += "      AND src.band=%(band)s "
        q += ( "      ORDER BY src.diaobjectid, src.visit, pv.priority DESC\n"
               "    ) s ON o.diaobjectid=s.diaobjectid\n"
               "    ORDER BY t.root_diaobject_id,mjd DESC\n"
               "  ) subq\n"
               ")" )
        con.execute_nofetch( q, { 'procver': procver, 'band': lim_mag_band, 'now': mjdnow } )

        rows, _cols = con.execute( "SELECT COUNT(*) FROM tmp_latest_detection" )
        logger.debug( f"{rows[0][0]} rows in tmp_latest_detection" )
        if _show_way_too_much_debug_info:
            rows, _cols = con.execute( "SELECT root_diaobject_id,mjd,band,mag FROM tmp_latest_detection" )
            sio = io.StringIO()
            sio.write( "Contents of tmp_latest_detection:\n" )
            sio.write( f"{'UUID':36s} {'mjd':8s} {'band':6s} {'mag':6s}\n" )
            sio.write( "------------------------------------ -------- ------ ------\n" )
            for row in rows:
                sio.write( f"{str(row[0]):36s} {row[1]:8.2f} {row[2]:6s} {row[3]:6.2f}\n" )
            logger.debug( sio.getvalue() )

        # Get the latest forced source for the objects
        con.execute_nofetch( "CREATE TEMP TABLE tmp_latest_forced( root_diaobject_id UUID,\n"
                             "                                     mjd double precision,\n"
                             "                                     band text, mag real )\n" )
        q = ( "/*+ IndexScan(frc idx_diaforcedsource_diaobjectid) */\n"
              "INSERT INTO tmp_latest_forced (\n"
              "  SELECT root_diaobject_id, diaobjectid, mjd, band, mag\n"
              "  FROM (\n"
              "    SELECT DISTINCT ON (t.root_diaobject_id) t.root_diaobject_id, f.diaobjectid,\n"
              "           f.band AS band, f.midpointmjdtai AS mjd,\n"
              "           CASE WHEN f.psfflux>0 THEN -2.5*LOG(f.psfflux)+31.4 ELSE 99 END AS mag\n"
              "    FROM tmp_wanted3 t\n"
              "    INNER JOIN diaobject o ON t.root_diaobject_id=o.rootid\n"
              "    INNER JOIN (\n"
              "      SELECT DISTINCT ON (frc.diaobjectid,frc.visit) frc.diaobjectid,frc.midpointmjdtai,\n"
              "                                                     frc.band,frc.psfflux\n"
              "      FROM diaforcedsource frc\n"
              "      INNER JOIN base_procver_of_procver pv ON frc.base_procver_id=pv.base_procver_id\n"
              "        AND pv.procver_id=%(procver)s\n"
              "      WHERE frc.midpointmjdtai<=%(now)s\n" )
        if lim_mag_band is not None:
            q += "        AND frc.band=%(band)s\n"
        q += ( "      ORDER BY frc.diaobjectid, frc.visit, pv.priority DESC\n"
               "    ) f ON o.diaobjectid=f.diaobjectid\n"
               "    ORDER BY t.root_diaobject_id,mjd DESC\n"
               "  ) AS subq\n"
               ")" )
        con.execute_nofetch( q, { 'procver': procver, 'band': lim_mag_band, 'now': mjdnow } )

        rows, _cols = con.execute( "SELECT COUNT(*) FROM tmp_latest_forced" )
        logger.debug( f"{rows[0][0]} rows in tmp_latest_forced" )
        if _show_way_too_much_debug_info:
            rows, _cols = con.execute( "SELECT root_diaobject_id,mjd,band,mag FROM tmp_latest_forced" )
            sio = io.StringIO()
            sio.write( "Contents of tmp_latest_forced:\n" )
            sio.write( f"{'UUID':36s} {'mjd':8s} {'band':6s} {'mag':6s}\n" )
            sio.write( "------------------------------------ -------- ------ ------\n" )
            for row in rows[0]:
                sio.write( f"{str(row[0]):36s} {row[1]:8.2f} {row[2]:6s} {row[3]:6.2f}\n" )
            logger.debug( sio.getvalue() )

        ROB THIS IS BROKEN YOU HAVE TO THINK ABOUT OBJECT PROVENANCE
            
        # Get object info
        con.execute_nofetch( "CREATE TEMP TABLE tmp_object_info( root_diaobject_id UUID, requester text,\n"
                             "                                   priority smallint, diaobjectid bigint,\n"
                             "                                   ra double precision, dec double precision )" )
        q = ( "INSERT INTO tmp_object_info (\n"
              "  SELECT DISTINCT ON (t.root_diaobject_id) t.root_diaobject_id, t.requester,\n"
              "                                           t.priority, o.diaobjectid, o.ra, o.dec\n"
              "  FROM tmp_wanted3 t\n"
              "  INNER JOIN (\n"
              "    SELECT obj.rootid, obj.diaobjectid, obj.ra, obj.dec\n"
              "    FROM diaobject obj\n"
              "    INNER JOIN base_procver_of_procver pv ON obj.base_procver_id=pv.base_procver_id\n"
              "      AND pv.procver_id=%(procver)s\n"
              "   ) o ON o.rootid=t.root_diaobject_id\n"
              ")" )
        con.execute_nofetch( q, { 'procver': procver } )
        # TODO : worry if there are fewer things in this table than tmp_wanted3 ?????
        # (Will happen if objects aren't in processing versions that sources are, which could happen!)
                
        rows, _cols = con.execute( "SELECT COUNT(*)_ FROM tmp_object_info" )
        logger.debug( f"{rows[0][0]} rows in tmp_object_info" )
        if _show_way_too_much_debug_info:
            rows, _cols = con.execute( "SELECT root_diaobject_id,requester,priority,diaobjectid,ra,dec"
                                       "FROM tmp_object_info" )
            sio = io.StringIO()
            sio.write( "Contents of tmp_object_info:\n" )
            sio.write( f"{'UUID':36s} {'requester':16s} {'prio':4s} {'diaobjectid':12s} "
                       f"{'ra':8s} {'dec':8s}\n" )
            sio.write( "------------------------------------ ---------------- ---- ------------ "
                       "-------- --------\n" )
            for row in rows:
                sio.write( f"{str(row[0]):36s} {row[1]:16s} {row[2]:4d} {row[3]:12d} "
                           f"{row[4]:8.4f} {row[5]:8.4f}\n" )
            logger.debug( sio.getvalue() )

        # Join all the things and pull
        q = ( "SELECT t.root_diaobject_id, t.requester, t.priority, o.ra, o.dec, "
              "       s.mjd AS src_mjd, s.band AS src_band, s.mag AS src_mag, "
              "       f.mjd AS frced_mjd, f.band AS frced_band, f.mag AS frced_mag "
              "FROM tmp_wanted3 t "
              "INNER JOIN tmp_object_info o ON t.root_diaobject_id=o.root_diaobject_id "
              "LEFT JOIN tmp_latest_detection s ON t.root_diaobject_id=s.root_diaobject_id "
              "LEFT JOIN tmp_latest_forced f ON t.root_diaobject_id=f.root_diaobject_id" )
        rows, cols = con.execute( q )
        df = pandas.DataFrame( rows, columns=cols )

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

        cursor.execute( q, subdict )
        columns = [ col.name for col in cursor.description ]
        df = pandas.DataFrame( cursor.fetchall(), columns=columns )

    return df
