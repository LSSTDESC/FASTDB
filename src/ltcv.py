__all__ = [ "object_ltcv", "object_search", "get_hot_ltcvs" ]

import datetime
import numbers
import json   # noqa: F401

from psycopg import sql
import pandas
import astropy.time

import db
import util


def get_object_infos( objids=None, objids_table=None, processing_version=None,
                      columns=None, return_format='json', dbcon=None ):
    """Get information from the diaobject table.

    Parameters
    ----------
      objids : list of int or uuid
        Either the diaobejctid or rootid values for the objects to get.
        Either this or objids_table is required.

      objids_table : str, default None
        The name of a table (probably a temporary table) with the
        objects whose info to get in its diaobjectid column.  Cannot
        pass objids if you pass this.  Requires dbcon to be passed (so
        that temporary tables are meaningful).

      processing_version : str, uuid, or None
        If objids is a list of rootids, then this is required.  If
        objids is a list of int, then this is ignored.

      columns : list of str, default None
        If given only include these columns from the diaobject table.
        Otherwise, include all of them.  If 'diaobjectid' is not
        included in this list, it will be prepended to it.  (You can't
        not get diaobjectid back, because it's the index of the returned
        dataframe or the keys of the returned dictionary.)

      return_format : str, default 'json'
        Either 'pandas' or 'json'

      dbcon : db.DBCon or psycopg.Connection, default None
        If given, use this database connection.  If not given,
        then open a connection, closing it when done.

    Returns
    -------
      rval: pandas.DataFrame or dict
        If return_format is 'pandas', the dataframe is indexed by diaobjectid and has
        all columns from the diaobject table.

        If return_format is 'json', then the return is a dictionary whose keys are
        the columns of diaobject, and whose values are lists all of the same length.

    """
    if objids_table is not None:
        if dbcon is None:
            raise ValueError( "objids_table requires dbcon" )
        if objids is not None:
            raise ValueError( "objids_table and objids cannot be used together" )
        if processing_version is not None:
            raise ValueError( "Cannot pass processing_version when passing objids_table" )
    else:
        if not util.isSequence( objids ):
            objids = [ objids ]
        if all( isinstance( o, numbers.Integral ) for o in objids ):
            # Make sure they're int, because if it's something like np.int64, postgres may choke
            objids = [ int(o) for o in objids ]
            if processing_version is not None:
                raise ValueError( "Cannot pass processing_version when passing integer objids" )
            obj_is_root = False
        else:
            try:
                objids = [ util.asUUID(o) for o in objids ]
            except ValueError:
                raise ValueError( "objids must be a list of integers or a list of uuids" )
            obj_is_root = True
            if processing_version is None:
                raise ValueError( "Passing root ids requires a processing_version" )
            pv = db.ProcessingVersion.procver_id( processing_version )
        if len(objids) == 0:
            raise ValueError( "no objids requested" )

    if return_format not in ( 'pandas', 'json' ):
        raise ValueError( f"return_format must be pandas or json, not {return_format}" )

    if columns is None:
        columns = db.DiaObject.all_columns_sql( prefix='o' )
    else:
        if not util.isSequence( columns ):
            columns = [ columns ]
        else:
            columns = list( columns )
        if 'diaobjectid' not in columns:
            columns = columns.copy()
            columns.insert( 0, 'diaobjectid' )
        columns = sql.SQL(',').join( sql.Identifier('o', i) for i in columns )


    with db.DBCon( dbcon ) as dbcon:
        if objids_table is not None:
            q = sql.SQL(
                """/*+ IndexScan(o idx_diaobject_diaobjectid) */
                SELECT {columns}
                FROM {objids_table} t
                INNER JOIN diaobject o ON o.diaobjectid=t.diaobjectid
                """ ).format( columns=columns, objids_table=sql.Identifier(objids_table) )
        else:
            if obj_is_root:
                q = sql.SQL(
                    """
                    SELECT DISTINCT ON(o.diaobjectid) {columns}
                    FROM diaobject o
                    INNER JOIN base_procver_of_procver pv ON o.base_procver_id=pv.base_procver_id
                                                         AND pv.procver_id={pv}
                    WHERE o.rootid=ANY(%(objids)s)
                    ORDER BY o.diaobjectid, pv.priority DESC
                    """
                ).format( columns=columns, pv=pv )
            else:
                q = sql.SQL( "SELECT {columns} FROM diaobject o WHERE diaobjectid=ANY(%(objids)s)"
                            ).format( columns=columns )

        rows, cols = dbcon.execute( q, { 'objids': objids } )

        if return_format == 'pandas':
            return pandas.DataFrame( rows, columns=cols ).set_index( 'diaobjectid' )

        elif return_format == 'json':
            return { c: [ r[i] for r in rows ] for i, c in enumerate( cols ) }

        else:
            raise RuntimeError( "This should never happen" )


def many_object_ltcvs( processing_version='default', objids=None, objids_table=None,
                       bands=None, which='patch', include_base_procver=False,
                       return_format='json', string_keys=False, mjd_now=None, dbcon=None ):
    """Get lightcurves for objects.

    Parameters
    ----------
      processing_version : UUID or str, default 'default'
         The processing version (or alias) to search photometry.

      objids: int, uuid, list of int, or list of uuid
         Objects to search for.  If None, will get ALL OBJECTS; if
         you're doing this, you probably want to use LIMIT (and maybe
         OFFSET).  If ints, will be diaobjectids.  If uuid, will be
         rootids.  If ints, the diaobjectid must be consistent with
         processing_version, or nothing will be found.  If uuids, then
         the diaobjectids found will be the ones that match the
         photometry with the right processing_version.

      objid_table : str, default None
         If not None, then this is the name of a table (probably a
         temporary table) that has the object ids already loaded into it
         in the column "diaobjectid".  Use of this requires dbcon to be
         non None.

      # Offset and limit don't work right, I have to think harder
      # offset: int, default None
      #    Only return lightcurves starting this many in from what's found.
      #    (offset=0 is the same as not passing anything).
      #
      # limit: int, default None
      #    Only return this many objects' lightcurves if given.

      bands: str, list of str or None
         If not None, only include bands in this list.

      which : str, default 'patch'
         forced : get forced photometry (i.e. diaforcedsource)
         detections : get detections (i.e. diasource)
         patch : get forced photometry, but patch in detections where forced photometry is missing

      include_base_procver : bool, default False
         If True, the returned data will have a two columns,
         base_procver_s and base_procver_f, that are the descriptions
         of the base processing versions of the object for sources and
         forced sources respectively (or NaN if not defined).

      return_format : str, default 'json'
         'json' or 'pandas'

      string_keys : bool, default False
         Ignored unless return_format is 'json'.  Normally, with
         return_format 'json', the keys of the returned thing are all
         bigints, as thats what diaobjectids are.  But, if you really
         want to encode this to actual json, that's a problem, because
         actual json specifies that keys have to be strings. (Why do we
         use this format??)  Set string_keys to True, and the diaobjectid
         keys of the returned dictionary will be wrapped in str().

      mjd_now : float, default None
         You almost always want to leave this at None.  It's here for
         testing purposes.  Normally you will get back all data on an
         object that's in the database.  However, if you want to
         pretend it's an earlier time, pass an mjd here, and all data
         later than that mjd will be truncated away.

         Note that this isn't perfect.  For realtime LSST operations,
         on mjd n, you will have detections through mjd n (assuming
         that alerts have gone out fast, brokers have kept up with
         classifications, and we have kept up with injesting broker
         alerts), but only forced photometry through mjd n-1 at the
         latest.  With mjd_now, you get everything in the database
         through mjd n, even forced photometry; it doesn't try to
         simulate forced photometry coming in late.

      dbcon: psycopg.Connection, db.DBCon, or None
         Database connection to use.  If None, will make a new
         connection and close it when done.

    Returns
    -------
      retval: pandas.DataFrame or dict
        If return_format is 'pandas', then you get back a DataFrame with
        indexes (diaobjectid, mjd) and columns (band, flux,
        fluxerr, isdet, [ispatch]).  (ispatch will only be included if
        which is 'patch').

        If return_format is 'json', then you get back a dict of diaobjectid -> dict
        The inner dict has fields:
          { 'visit': list of bigint,
            'mjd': list of float,
            'band': list of str,
            'flux': list of float,
            'fluxerr': list of float
            'isdet': list of int,      (1 for true, 0 for false; javascript JSON chokes on booleans)
            [ 'ispatch': list of int, ] (only included if which is 'patch')
            [ 'base_procver': list of str, ] (only included if include_base_procver is True)
         }

    """

    # Parse objids, set objfield
    if objids_table is not None:
        if dbcon is None:
            raise ValueError( "objids_table requires dbcon" )
        if objids is not None:
            raise ValueError( "objids_table and objids cannot be used together" )
        objfield = 'diaobjectid'
    else:
        objids_table = "tmp_objids"
        if objids is None:
            raise ValueError( "objids is required" )
        if not util.isSequence( objids ):
            objids = [ objids ]
        if all( isinstance( o, numbers.Integral ) for o in objids ):
            # Make sure they're int, because if it's something like np.int64, postgres will choke
            objids = [ int(o) for o in objids ]
            objfield = 'diaobjectid'
        else:
            objfield = 'rootid'
            try:
                objids = [ util.asUUID(o) for o in objids ]
            except ValueError:
                raise ValueError( "objids must be a list of integers or a list of uuids" )
        if len(objids) == 0:
            raise ValueError( "no objids requested" )

    # Parse bands
    if bands is not None:
        if not util.isSequence( bands ):
            bands = [ bands ]

    # Make sure which is something reasonable
    if which not in ( 'detections', 'forced', 'patch' ):
        raise ValueError( f"which must be detections, forced, or patch, not {which}" )

    # Make sure return_format is something reasonable
    if return_format not in ( 'json', 'pandas' ):
        raise ValueError( f"return_format must be json or pandas, not {return_format}" )

    # Make sure mjd_now is floatifiable
    mjd_now = None if mjd_now is None else float( mjd_now )

    with db.DBCon( dbcon ) as dbcon:
        pvid = db.ProcessingVersion.procver_id( processing_version, dbcon=dbcon )

        if objids is not None:
            # For efficiency, we're going to make a first pass and extract just the object ids.
            # If these are root ids, then we can't be sure which diaobjectid will correspond
            # to them, so we will pull the *all* out.  (Think about this.  It's possible we
            # could be doing something with the object's processing version, but consisder
            # all the complicated messy cases.)

            q = sql.SQL( "CREATE TEMP TABLE {objids_table}( diaobjectid bigint )"
                        ).format( objids_table=sql.Identifier( objids_table ) )
            dbcon.execute( q, explain=False )
            if objfield == 'rootid':
                q = sql.SQL(
                    """INSERT INTO {objids_table} (
                         SELECT diaobjectid FROM diaobject
                         WHERE rootid=ANY(%(roots)s)
                       )
                    """
                ).format( objids_table=sql.Identifier(objids_table) )
                dbcon.execute( q, { 'roots': objids } )
            else:
                q = sql.SQL( "COPY {objids_table}(diaobjectid) FROM STDIN"
                            ).format( objids_table=sql.Identifier( objids_table ) )
                with dbcon.cursor.copy( q ) as copier:
                    for objid in objids:
                        copier.write_row( [ objid ] )

        # Extract detections
        dbcon.execute( "CREATE TEMP TABLE tmp_sources( diaobjectid bigint, visit bigint,"
                       "                               mjd double precision, band text, flux real, fluxerr real,"
                       "                               isdet bool, base_procver text )",
                       explain=False )
        q = sql.SQL(
            """/*+ IndexScan(s idx_diasource_diaobjectid) */
               INSERT INTO tmp_sources
               SELECT DISTINCT ON (s.diaobjectid, s.visit)
                 s.diaobjectid, s.visit, s.midpointmjdtai AS mjd,
                 s.band, s.psfflux AS flux, s.psffluxerr AS fluxerr,
                 TRUE as isdet, p.description AS base_procver
               FROM {objids_table} t
               INNER JOIN diasource s ON s.diaobjectid=t.diaobjectid
               INNER JOIN base_procver_of_procver pv ON s.base_procver_id=pv.base_procver_id
                                                    AND pv.procver_id={procver}
               INNER JOIN base_processing_version p ON pv.base_procver_id=p.id
            """
        ).format( procver=pvid, objids_table=sql.Identifier(objids_table) )
        _and = "WHERE"
        if mjd_now is not None:
            q += sql.SQL( f"                   {_and} s.midpointmjdtai<={{t0}}" ).format( t0=mjd_now )
            _and = "  AND"
        if bands is not None:
            q += sql.SQL( f"                   {_and} s.band=ANY(%(bands)s)" )
            _and = "  AND"
        q += sql.SQL(
            """
               ORDER BY s.diaobjectid, s.visit, pv.priority DESC
            """ )
        dbcon.execute( q, { 'bands': bands } )

        if which == 'detections':
            rows, cols = dbcon.execute( "SELECT * FROM tmp_sources" )

        else:
            # Extract forced photometry if necessary
            dbcon.execute( "CREATE TEMP TABLE tmp_forced( diaobjectid bigint, visit bigint,"
                           "                              mjd double precision, band text, flux real, fluxerr real,"
                           "                              isdet bool, base_procver text )",
                           explain=False )
            q = sql.SQL(
                """/*+ IndexScan(s idx_diaforcedsource_diaobjectid) */
                   INSERT INTO tmp_forced
                   SELECT DISTINCT ON (s.diaobjectid, s.visit)
                     s.diaobjectid, s.visit, s.midpointmjdtai AS mjd,
                     s.band, s.psfflux AS flux, s.psffluxerr AS fluxerr,
                     FALSE as isdet, p.description AS base_procver
                   FROM {objids_table} t
                   INNER JOIN diaforcedsource s ON s.diaobjectid=t.diaobjectid
                   INNER JOIN base_procver_of_procver pv ON s.base_procver_id=pv.base_procver_id
                                                        AND pv.procver_id={procver}
                   INNER JOIN base_processing_version p ON pv.base_procver_id=p.id
                """
            ).format( procver=pvid, objids_table=sql.Identifier(objids_table) )
            _and = "WHERE"
            if mjd_now is not None:
                q += sql.SQL( f"                   {_and} s.midpointmjdtai<={{t0}}" ).format( t0=mjd_now )
                _and = "  AND"
            if bands is not None:
                q += sql.SQL( f"                   {_and} s.band=ANY(%(bands)s)" )
                _and = "  AND"
            q += sql.SQL(
                """
                   ORDER BY s.diaobjectid, s.visit, pv.priority DESC
                """ )
            dbcon.execute( q, { 'bands': bands } )

            # Join detections to forced photometry to set the 'isdet' and 'ispatch' flags
            q = sql.SQL(
                """SELECT CASE WHEN f.diaobjectid IS NULL THEN s.diaobjectid ELSE f.diaobjectid END AS diaobjectid,
                          CASE WHEN f.visit IS NULL THEN s.visit ELSE f.visit END AS visit,
                          CASE WHEN f.mjd IS NULL THEN s.mjd ELSE f.mjd END AS mjd,
                          CASE WHEN f.band IS NULL THEN s.band ELSE f.band END AS band,
                          CASE WHEN f.flux IS NULL THEN s.flux ELSE f.flux END AS flux,
                          CASE WHEN f.fluxerr IS NULL THEN s.fluxerr ELSE f.fluxerr END AS fluxerr,
                          CASE WHEN s.mjd IS NULL THEN FALSE ELSE TRUE END AS isdet,
                          CASE WHEN f.mjd IS NULL THEN TRUE ELSE FALSE END as ispatch,
                          CASE WHEN f.base_procver IS NULL THEN s.base_procver ELSE f.base_procver END
                            AS base_procver
                   FROM tmp_forced f
                   FULL OUTER JOIN tmp_sources s ON f.diaobjectid=s.diaobjectid AND s.visit=f.visit
                ORDER BY diaobjectid, mjd
                """ )
            rows, cols = dbcon.execute( q )

    retframe = pandas.DataFrame( rows, columns=cols )

    if not include_base_procver:
        retframe.drop( 'base_procver', axis='columns', inplace=True )

    if which == 'forced':
        if len(retframe) > 0:
            # Pandas annoyance: if retframe has 0 length, this next
            #   statement wipes out the columns.  Grrr.
            retframe = retframe[ ~retframe.ispatch ]
        retframe.drop( 'ispatch', axis='columns', inplace=True )

    if return_format == 'pandas':
        retframe.set_index( ['diaobjectid', 'mjd'], inplace=True )
        return retframe

    elif return_format == 'json':
        retval = {}
        for objid in retframe.diaobjectid.unique():
            subf = retframe[ retframe.diaobjectid==objid  ]
            thisretval = { 'visit': list( subf.visit.values ),
                           'mjd': list( subf.mjd.values ),
                           'band': list( subf.band.values ),
                           'flux': list( subf.flux.values ),
                           'fluxerr': list( subf.fluxerr.values ),
                           'isdet': [ int(i) for i in subf.isdet.values ] }
            if which == 'patch':
                thisretval['ispatch'] = [ int(i) for i in subf.ispatch.values ]
            if include_base_procver:
                thisretval['base_procver'] = list( subf.base_procver )
            k = str(objid) if string_keys else objid
            retval[ k ] = thisretval
        return retval

    else:
        raise RuntimeError( "This should never happen." )


def object_ltcv( processing_version='default', diaobjectid=None,
                 bands=None, which='patch', include_base_procver=False, return_format='json',
                 mjd_now=None, dbcon=None ):
    """Get the lightcurve for an object.

    Parameters
    ----------
       processing_version : UUID or str, default 'default'
          The processing verson (or alias) to search photometry.

       diaobjectid : int or UUID; required
          The object id -- will be either a diaobjectid or a
          root_diaobjectid based on the type passed.  If an int,
          it's a diaobjectid, and it needs to be consistent with
          what's in processing_verson or you'll get nothing back.

       bands : list of str or None
          If not None, only include the bands in this list.

       which : str, default 'patch'
          forced : get forced photometry (i.e. diaforcedsource)
          detections : get detections (i.e. diasource)
          patch : get forced photometry, but patch in detections where forced photometry is missing

       include_base_procver : bool, default False
          If True, the returned data will have a two columns,
          base_procver_s and base_procver_f, that are the descriptions
          of the base processing versions of the object for sources and
          forced sources respectively (or NaN if not defined).

       return_format : str, default 'json'
          'json' or 'pandas'

       mjd_now : float, default None
          You almost always want to leave this at None.  It's here for
          testing purposes.  Normally you will get back all data on an
          object that's in the database.  However, if you want to
          pretend it's an earlier time, pass an mjd here, and all data
          later than that mjd will be truncated away.

          Note that this isn't perfect.  For realtime LSST operations,
          on mjd n, you will have detections through mjd n (assuming
          that alerts have gone out fast, brokers have kept up with
          classifications, and we have kept up with injesting broker
          alerts), but only forced photometry through mjd n-1 at the
          latest.  With mjd_now, you get everything in the database
          through mjd n, even forced photometry; it doesn't try to
          simulate forced photometry coming in late.

       dbcon: psycopg.Connection, db.DBCon, or None
          Database connection to use.  If None, will make a new
          connection and close it when done.

    Returns
    -------
       Either a pandas dataframe, or a json which is a dict of lists.
       Fields:
         mjd : float
         band : str
         flux : float
         fluxerr : float
         isdet : bool (True if this is detected, false otherwise)
         ispatch : bool (True if this is detected but had no forced photometry, false otherwise;
                         this field is only present if which='patch'.)

    """

    rval = many_object_ltcvs( processing_version=processing_version, objids=[ diaobjectid ],
                              bands=bands, which=which, include_base_procver=include_base_procver,
                              return_format=return_format, mjd_now=mjd_now, dbcon=dbcon )
    if len(rval) == 0:
        raise RuntimeError( f"Could not find object for diaobjectid {diaobjectid}" )

    if return_format == 'pandas':
        rval.reset_index( inplace=True )
        rval.drop( 'diaobjectid', axis='columns', inplace=True )
        return rval

    elif return_format == 'json':
        rval = rval[ list(rval.keys())[0] ]
        return rval


def debug_count_temp_table( con, table ):
    res = con.execute( f"SELECT COUNT(*) FROM {table}" )
    util.logger.debug( f"Table {table} has {res[0][0]} rows" )


def object_search( processing_version='default', object_processing_version=None,
                   return_format='json', just_objids=False, dbcon=None, mjd_now=None,
                   **kwargs ):
    """Search for objects.

    For parameters that define the search, if they are None, they are
    not considered in the search.  (I.e. that filter will be skipped.)

    Parameters
    ----------
      processing_version : UUID or str
         The processing version you're looking at (for sources and forced sources).

      object_processing_version : UUID or str, default None
          If not None, only consider diaobjects from this processing
          version.  If None, consider all diaobjects.  Note, however,
          that only diaobjects that have sources with the given
          processing_version, some object_processing_versions shouldn't
          really be considered.  There's no _quick_ automatic way to
          figure that out, so allow passing that to make the first
          diaobject cut more efficient.

          (Notice that a None here behaves differently than a None
          passed to object_ltcv.)

      return_format : string
         Either "json" or "pandas".  (TODO: pyarrow? polars?)  See "Results" below.

      just_objids : bool, default False
         See "Returns" below.

      dbcon: psycopg.Connection, db.DBCon, or None
         Database connection to use.  If None, will make a new
         connection and close it when done.

      mjd_now: float, default None
         Usually you want to leave this at None.  It's useful for
         testing.  If it's not None, pretend that the current date is
         mjd_now.  When searching or extracting lightcurves, ignore all
         data with mjd greater than mjd_now.

         Warning: it's possible to use this inconsistently.  If, for
         instance, you give a window_t0 and window_t1 that are greater
         than mjd_now, it will merrily search in in that window, and
         return you detection counts in that window, ignoring the fact
         that they aren't consistent with mjd_now.  Similar warnings go
         for other time cuts (e..g first detection, last detection).
         It's up to you to pass a self-consistent set of parameters.

      ra, dec : float, default None
         RA and Dec for the center of a cone search, in J2000 degrees.
         If None, won't filter on RA/Dec.  (Doesn't make sense to
         specify only one and not the other, or to specify this and not radius.)

      radius : float, default None
         Radius of the cone search in arcseconds.  Doesn't make sense to
         specfiy this without both ra and dec.  TODO : make a maximum supported
         value based on what's sane with Q3C.  Right now, accepts any number
         and merrily passes that number on to PostgreSQL.

      window_t0, window_t1 : float, default None
         If given, a pair of MJDs defining a "detection window" used for some filters.

      min_window_numdetections : int, default None
         Only return objects with at least this many detections between
         window_t0 and window_t1.  Requires window_t0 and window_t1 to
         be given.

      max_window_numdetections : int, default None
         Only return objects with at most this many detections between
         window_t0 and window_t1.  Requires window_t0 and window_t1 to
         be given.

      # relwindow_t0, relwindow_t1 : float, default None
      #    NOT IMPLEMENTED.  Intended to be a time window around maximum-flux detection.

      mint_firstdetection : float, default None
         Only return objects whose first detection is on this MJD or later.

      maxt_firstdetection : float, default None
         Only return objects whose first detection is on this MJD or earlier.

      minmag_firstdetection : float, default None
         Only return objects whose first detection has at least this
         magnitude.  (Filters out things that were bright when first
         discovered.)

      maxmag_firstdetection : float, default None
         Only return objects whose first detection had at most this
         magnitude.  It's hard to think of a reason why you'd want to use this,
         as the brightest object in the world might have been just starting
         when first discovered....

      mint_lastdetection : float, default None
         Only return objects whose last detection is on this MJD or later.

      maxt_lastdetection : float, default None
         Only return objects whose last detection is on this MJD or later.

      minmag_lastdetection : float, default None
         Only return objects whose last detection had at least this
         magnitude.  Use this to filter out things that had gotten too
         dim last time they were detected.  NOTE: min_lastmag is
         probably more useful than this.

      maxmag_lastdetection : float, default None
         Only return things whose last detection had at most this
         magnitude.  Probably max_lastmag is more useful if you're
         trying to through out things that are too dim.


      mint_maxdetection : float, default None
         Only return objects whose highest-flux detection is on this MJD or later.

      maxt_maxdetection : float, default None
         Only return objects whose highest-flux detection i son this MJD or earlier.

      minmag_maxdetection : float, default None
         Only return objects whose highest-flux detection has at least
         this magnitude.  Use to elimiate objects that were too bright
         at peak.

      maxmag_maxdetection : float, default None
         Only return objects whose highest-flux detection has at most
         this magnitude.  Use to eliminate obejcts that were too dim at
         peak.

      min_numdetections : int, default None
         Only return objects with at least this many detections.

      # min_bandsdetected : int, default None
      #    Only return objects that have been detected in at least this many different  bands.
      #    (Not yet implemented.)

      mindt_firstlastdetection : float, default None
         Only return objects that have at least this many days between the first and last detection.

      maxdt_firstlastdetection : float, default None
         Only return objects that have at most this many days between the first and last detection.


      min_lastmag : float, default None
         The most recent measurement (not detection! includes forced
         sources) must have a magnitude that is at least this.  (Use
         this to filter out things that are too bright.)

      max_lastmag : float, default None
         The most recent measurement (not detection! includes forced
         sources) must have a magnitude that is at most this.  (Use this
         to filter out things that are too dim.)


      statbands : list of string, default None
         Normally, all of the cuts based on detection dates, detection
         counts, magnitudes, etc. consider all bands equally.  If you
         only want to consider some bands, list those here.  For
         instance, if you're only interested in cutting on
         measurements of the g, r, and i bands, pass ['g', 'r', 'i']
         here.

         This parameter also affects what is included in the returned
         array; it will ignroe any measurements of bands that aren't
         included in this list.

    Returns
     --------
      A table of data.  If just_objids is true, this will have a single
      column, "diaobjectid" that has the object ids within the specified
      processing verison of objects that match the search.  Otherwise,
      there will be additional columns.  (For all of these columns,
      assume that there is a "within statbands" in the definition.)

          ROB THIS IS WRONG FIX ALL OF THIS DOCUMENTATION BELOW

          diaobjectid — object id (within the specified processing version)

          ra — ra of the object (from the object table... *not* necessarily the best position)
          dec — dec of the object
          numdet — number of detections
          numdetinwindow — number of detections in [window_t0, window_t0].  (Null if window not given.)

          firstdetmjd — mjd of first detection
          firstdetband — band of first detection
          firstdetflux — flux (nJy) of first detection
          firstdetfluxerr — uncertainty on firstdetflux

          lastdetmjd — mjd of last detection
          lastdetband — band of last detection
          lastdetflux — flux (nJy) of last detection
          lastdetfluxerr — uncertainty on lastdetflux

          maxdetmjd — mjd of max-flux detection
          maxdetband — band of max-flux detection
          maxdetflux — flux (nJy) of max-flux detection
          maxdetfluxerr — uncertainty on maxdetflux

          lastforcedmjd — mjd of last measurement
          lastforcedband — band of last measurement
          lastforcedflux — flux (nJy) of last forced detection
          lastforcedfluxerr — uncertainty on lastforcedflux

      If return_format is json, then the return is actually a
      dictionary; the keys of the dictionary are the names listed above,
      and the values are lists.  All lists have the same length.

      If return_format is pandas, then the return is a pandas DataFrame
      with those columns.  (Indexing of the DataFrame is... unclear.
      Pandas sometimes does stuff automatically, and the author of this
      code needs to pay more attention to know what's happening.)

    """

    util.logger.debug( f"In object_search : kwargs = {kwargs}" )
    knownargs = { 'ra', 'dec', 'radius',
                  'window_t0', 'window_t1', 'min_window_numdetections', 'max_window_numdetections',
                  'mint_firstdetection', 'maxt_firstdetection', 'minmag_firstdetection', 'maxmag_firstdetection',
                  'mint_lastdetection', 'maxt_lastdetection', 'minmag_lastdetection', 'maxmag_lastdetection',
                  'mint_maxdetection', 'maxt_maxdetection', 'minmag_maxdetection', 'maxmag_maxdetection',
                  'min_numdetections', 'min_bandsdetected',
                  'mindt_firstlastdetection','maxdt_firstlastdetection',
                  'min_lastmag', 'max_lastmag',
                  'statbands' }

    unknownargs = set( kwargs.keys() ) - knownargs
    if len( unknownargs ) != 0:
        raise ValueError( f"Unknown search keywords: {unknownargs}" )

    if return_format not in [ 'json', 'pandas' ]:
        raise ValueError( f"Unknown return format {return_format}" )

    mjd_now = None if mjd_now is None else float( mjd_now )

    # Parse out statbands, allowing either a single string or a list of strings
    statbands = None
    if 'statbands' in kwargs:
        if util.isSequence( kwargs['statbands'] ):
            if not all( isinstance(b, str) for b in kwargs['statbands'] ):
                return TypeError( 'statbands must be a str or a list of str' )
            statbands = kwargs['statbands']
        elif isinstance( kwargs['statbands'], str ):
            statbands = [ kwargs['statbands'] ]
        else:
            return TypeError( 'statbands must be a str or a list of str' )

    # WARNING: hardcoding the 31.4 zeropoint for nJy fluxes here.  (The schema do define
    #   flux as being in nJy.)
    zp = 31.4

    # Parse out (the rest of the) arguments to variables
    ra = util.float_or_none_from_dict_float_or_hms( kwargs, 'ra' )
    dec = util.float_or_none_from_dict_float_or_dms( kwargs, 'dec' )
    radius = util.float_or_none_from_dict( kwargs, 'radius' )

    window_t0 = util.float_or_none_from_dict( kwargs, 'window_t0' )
    window_t1 = util.float_or_none_from_dict( kwargs, 'window_t1' )
    min_window_numdetections = util.int_or_none_from_dict( kwargs, 'min_window_numdetections' )
    max_window_numdetections = util.int_or_none_from_dict( kwargs, 'max_window_numdetections' )

    mint_firstdetection = util.float_or_none_from_dict( kwargs, 'mint_firstdetection' )
    maxt_firstdetection = util.float_or_none_from_dict( kwargs, 'maxt_firstdetection' )
    maxt_firstdetection = mjd_now if maxt_firstdetection is None else maxt_firstdetection
    minmag_firstdetection = util.float_or_none_from_dict( kwargs, 'minmag_firstdetection' )
    maxmag_firstdetection = util.float_or_none_from_dict( kwargs, 'maxmag_firstdetection' )

    mint_lastdetection = util.float_or_none_from_dict( kwargs, 'mint_lastdetection' )
    maxt_lastdetection = util.float_or_none_from_dict( kwargs, 'maxt_lastdetection' )
    maxt_lastdetection = mjd_now if maxt_lastdetection is None else maxt_lastdetection
    minmag_lastdetection = util.float_or_none_from_dict( kwargs, 'minmag_lastdetection' )
    maxmag_lastdetection = util.float_or_none_from_dict( kwargs, 'maxmag_lastdetection' )

    mint_maxdetection = util.float_or_none_from_dict( kwargs, 'mint_maxdetection' )
    maxt_maxdetection = util.float_or_none_from_dict( kwargs, 'maxt_maxdetection' )
    maxt_maxdetection = mjd_now if maxt_maxdetection is None else maxt_maxdetection
    minmag_maxdetection = util.float_or_none_from_dict( kwargs, 'minmag_maxdetection' )
    maxmag_maxdetection = util.float_or_none_from_dict( kwargs, 'maxmag_maxdetection' )

    min_numdetections = util.int_or_none_from_dict( kwargs, 'min_numdetections' )
    min_bandsdetected = util.int_or_none_from_dict( kwargs, 'min_bandsdetected' )

    mindt_firstlastdetection = util.float_or_none_from_dict( kwargs, 'mindt_firstlastdetection' )
    maxdt_firstlastdetection = util.float_or_none_from_dict( kwargs, 'maxdt_firstlastdetection' )

    min_lastmag = util.float_or_none_from_dict( kwargs, 'min_lastmag' )
    max_lastmag = util.float_or_none_from_dict( kwargs, 'max_lastmag' )

    # Stuff currently not implemented
    if min_bandsdetected is not None:
        raise NotImplementedError( "min_bandsdetected is not yet implemented" )


    with db.DBCon( dbcon ) as con:
        procver = util.procver_id( processing_version, dbcon=con.con )
        objprocver = None if object_processing_version is None else util.procver_id( object_processing_version )

        # Search criteria consistency checks
        if ( any( [ ( ra is None ), ( dec is None ), ( radius is None ) ] )
             and not all( [ ( ra is None ), ( dec is None ), ( radius is None ) ] ) ):
            raise ValueError( "Must give either all or none of ra dec, radius, not just one or two" )

        if ( window_t0 is None ) != ( window_t1 is None ):
            raise ValueError( "Must give both or neither of window_t0, window_t1, not just one" )
        if ( ( ( min_window_numdetections is not None ) or ( max_window_numdetections is not None) )
             and ( window_t0 is None ) ):
            raise ValueError( "(min|max)_window_numdetections requires window_t0 and window_t1" )
        if ( ( window_t0 is not None ) and ( maxt_lastdetection is not None )
             and ( maxt_lastdetection < window_t0 ) ):
            raise ValueError( f"window_t0={window_t0} and maxt_lastdetection={maxt_lastdetection} inconsistent" )
        if ( ( window_t1 is not None ) and ( mint_firstdetection is not None )
             and ( mint_firstdetection > window_t1 ) ):
            raise ValueError( f"window_t1={window_t1} and mint_firstdetection={mint_firstdetection} inconsistent" )

        # TODO : compare max detection time to first and last detection time

        nexttable = 'diaobject'
        addlfields = []

        # Filter by ra and dec if given
        if ra is not None:
            radius = util.float_or_none_from_dict( kwargs, 'radius' )
            radius = radius if radius is not None else 10.
            # For reasons I don't understand, adding the hint slows this next query down.
            # q3c / pg_hint_plan interaction weirdness?
            q = ( # "/*+ IndexScan(o idx_diaobject_q3c) */"
                  "SELECT DISTINCT ON(diaobjectid) diaobjectid,ra,dec INTO TEMP TABLE objsearch_radeccut\n"
                  "FROM diaobject o\n" )
            if objprocver is not None:
                q += ( "INNER JOIN base_procver_of_procver pv ON pv.base_procver_id=o.base_procver_id\n"
                       "  AND pv.procver_id=%(pv)s\n" )
            q += ( f"WHERE q3c_radial_query( ra, dec, %(ra)s, %(dec)s, %(rad)s )\n"
                   f"ORDER BY diaobjectid{',pv.priority DESC' if objprocver is not None else ''}\n" )
            subdict = { 'pv': objprocver, 'ra': ra, 'dec': dec, 'rad': radius/3600. }
            con.execute_nofetch( q, subdict )
            # ****
            debug_count_temp_table( con, 'objsearch_radeccut' )
            # ****
            nexttable = 'objsearch_radeccut'

        # Count (and maybe filter) by number of detections within the time window
        # ROB TODO : use processing version index
        if window_t0 is not None:
            if nexttable != 'diaobject':
                # Make a primary key so we can group by
                con.execute_nofetch( f"ALTER TABLE {nexttable} ADD PRIMARY KEY (diaobjectid)", explain=False )
            subdict = { 'pv': procver, 't0': window_t0, 't1': window_t1 }
            q = ( f"/*+ IndexScan(s idx_diasource_diaobjectid idx_diasource_mjd) */\n"
                  f"SELECT diaobjectid, ra, dec, numdetinwindow\n"
                  f"INTO TEMP TABLE objsearch_windowdet\n"
                  f"FROM (\n"
                  f"  SELECT diaobjectid,ra,dec,COUNT(visit) AS numdetinwindow\n"
                  f"  FROM (\n"
                  f"    SELECT DISTINCT ON (o.diaobjectid,s.visit) o.diaobjectid, o.ra, o.dec, s.visit\n"
                  f"    FROM {nexttable} o\n"
                  f"    INNER JOIN diasource s ON s.diaobjectid=o.diaobjectid \n"
                  f"    INNER JOIN base_procver_of_procver pv ON s.base_procver_id=pv.base_procver_id\n"
                  f"           AND pv.procver_id=%(pv)s\n"
                  f"    WHERE s.midpointmjdtai>=%(t0)s AND s.midpointmjdtai<=%(t1)s\n"
                  f"    ORDER BY o.diaobjectid, s.visit, pv.priority DESC\n" )
            if statbands is not None:
                q += "     AND s.band=ANY(%(bands)s)\n"
            q += ( "  ) subsubq\n"
                   "  GROUP BY diaobjectid, ra, dec\n"
                   ") subq\n" )
            _and = "WHERE"
            if min_window_numdetections is not None:
                q += f"{_and} numdetinwindow>=%(minn)s\n"
                subdict['minn'] = min_window_numdetections
                _and = "  AND"
            if max_window_numdetections is not None:
                q += f"{_and} numdetinwindow<=%(maxn)s\n"
                subdict['maxn'] = max_window_numdetections
                _and = "  AND"
            con.execute_nofetch( q, subdict )
            # ****
            debug_count_temp_table( con, 'objsearch_windowdet' )
            # ****
            nexttable = 'objsearch_windowdet'
            addlfields.append( "numdetinwindow" )


        # First pass cut that has *any* detection with (min(minfirst,minlast) < t < max(maxfirst,maxlast)
        #   to try to cut down the total size of stuff to think about in our next big join
        # TODO : also thing about adding magnitude cuts here!  May not
        #   be worth it since we don't have indexes on fluxes.  (Maybe we should?)  (No way.)
        if any( i is not None for i in [ mint_firstdetection, maxt_firstdetection,
                                         mint_lastdetection, maxt_lastdetection ] ):
            if ( ( maxt_lastdetection is not None ) and ( mint_firstdetection is not None ) and
                 ( mint_firstdetection < maxt_lastdetection ) ):
                raise RuntimeError( "maxt_lastdetection > mint_firstdetection, which makes no sense." )
            subdict = { 'pv': procver }
            q = ( "/*+ IndexScan(s idx_diasource_diaobjectid idx_diasource_mjd) */\n"
                  "SELECT * INTO TEMP TABLE objsearch_detcut FROM (\n"
                  "  SELECT DISTINCT ON (o.diaobjectid) o.diaobjectid,o.ra,o.dec" )
            if len(addlfields) > 0:
                q += f",{','.join( [f'o.{x}' for x in addlfields] )}\n"
            else:
                q += "\n"
            q += ( f"  FROM {nexttable} o\n"
                   f"  INNER JOIN diasource s ON o.diaobjectid=s.diaobjectid\n"
                   f"  INNER JOIN base_procver_of_procver pv ON s.base_procver_id=pv.base_procver_id\n"
                   f"    AND pv.procver_id=%(pv)s\n" )
            _and = "WHERE"
            if ( mint_firstdetection is not None ) or ( mint_lastdetection is not None ):
                q += f"  {_and} s.midpointmjdtai>=%(mint)s\n"
                subdict['mint'] = ( mint_firstdetection if mint_lastdetection is None
                                    else mint_lastdetection if mint_firstdetection is None
                                    else min( mint_firstdetection, mint_lastdetection ) )
                _and = "  AND"
            if ( maxt_firstdetection is not None ) or ( maxt_lastdetection is not None ):
                q += f"  {_and} s.midpointmjdtai<=%(maxt)s\n"
                subdict['maxt'] = ( maxt_firstdetection if maxt_lastdetection is None
                                    else maxt_lastdetection if maxt_firstdetection is None
                                    else max( maxt_firstdetection, maxt_lastdetection ) )
                _and = "  AND"
            if statbands is not None:
                q += f"  {_and} s.band=ANY(%(bands)s)\n"
                subdict['bands'] = statbands
            q += ( "  ORDER BY o.diaobjectid\n"           # , s.visit, pv.priority DESC\n"
                   ") subq\n" )
            con.execute_nofetch( q, subdict )
            # ****
            debug_count_temp_table( con, 'objsearch_detcut' )
            # ****
            nexttable = "objsearch_detcut"

        # Make a temp table that has number of detections, and first, last, and max detections
        # NOTE.  We're being cavalier here with INNER JOIN.  The assumption is that
        #   there will ALWAYS be at least one diasource for any diaobject, otherwise
        #   the diaobject would never have been defined in the first place.
        # TODO THINK : what about when statbands is given?  ROB THINK A LOT.

        # Another note: we're putting in hints to make sure postgres is
        #   using indicies where it should, because sometimes the
        #   postgres query planner does the wrong thing.  Additionally,
        #   we specify the join order to force it first to join our
        #   table of objects (which, presumably, has many fewer objects
        #   than exist in the database) to the source or forced source
        #   table, and only then join to processing versions.  We do
        #   *not* want it scanning all the way through a source or
        #   forced source index to filter out processing verions first,
        #   because it will consider millions of rows that could be
        #   ignored once we filtered by object.
        # Also, analyze the latest table on column diaobjectid,
        #   so where the postgres query optimizer is still doing
        #   things it will work with reasonable data.  (It's possible
        #   that this is enough, and we don't need the hints,
        #   but this is a case where we are quite sure which join
        #   order is going to be best.)  (Hurm; I still felt the
        #   need to specify the join type sometimes....)

        subdict = { 'pv': procver }

        con.execute( f"ANALYZE {nexttable}(diaobjectid)", explain=False )

        # First: build the table, put in first detection
        q = ( "/*+ IndexScan(s idx_diasource_diaobjectid)\n"
              "    Leading( ( (o s) pv ) ) */\n"
              "SELECT * INTO TEMP TABLE objsearch_stattab FROM (\n"
              "  SELECT DISTINCT ON (diaobjectid)\n"
              "         diaobjectid,ra,dec,\n" )
        if len(addlfields) > 0:
            q += f"         {','.join(addlfields)},\n"
        q += ( "         NULL::integer as numdet,\n"
               "         midpointmjdtai AS firstdetmjd, band AS firstdetband,\n"
               "         psfflux AS firstdetflux, psffluxerr AS firstdetfluxerr,\n"
               "         NULL::double precision as lastdetmjd, NULL::text as lastdetband,\n"
               "         NULL::double precision as lastdetflux, NULL::double precision as lastdetfluxerr,\n"
               "         NULL::double precision as maxdetmjd, NULL::text as maxdetband,\n"
               "         NULL::double precision as maxdetflux, NULL::double precision as maxdetfluxerr\n"
               "  FROM (\n"
               "    SELECT diaobjectid, ra, dec, midpointmjdtai, band, psfflux, psffluxerr\n" )
        if len(addlfields) > 0:
            q += f"           ,{','.join(addlfields)}\n"
        q += ( "    FROM (\n"
               "      SELECT DISTINCT ON (o.diaobjectid,s.visit) o.diaobjectid, o.ra, o.dec,\n"
               "                                                 s.midpointmjdtai, s.band, s.psfflux, s.psffluxerr\n"
              )
        if len(addlfields) > 0:
            q += f"           ,{','.join( [ f'o.{x}' for x in addlfields ] )}\n"
        q += ( f"      FROM {nexttable} o\n"
               f"      INNER JOIN diasource s ON o.diaobjectid=s.diaobjectid\n"
               f"      INNER JOIN base_procver_of_procver pv ON s.base_procver_id=pv.base_procver_id\n"
               f"        AND pv.procver_id=%(pv)s\n" )
        _and = "WHERE"
        if statbands is not None:
            subdict['bands'] = statbands
            q += f"      {_and} s.band=ANY(%(bands)s)\n"
            _and = "  AND"
        if mjd_now is not None:
            subdict['mjdnow'] = mjd_now
            q += f"      {_and} s.midpointmjdtai<=%(mjdnow)s\n"
            _and = "  AND"
        q += ( "      ORDER BY o.diaobjectid, s.visit, pv.priority DESC\n"
               "    ) subsubq\n"
               "    ORDER BY diaobjectid, midpointmjdtai\n"
               "  ) subq\n"
               ") outersubq" )
        con.execute_nofetch( q, subdict )

        # Analyze the objsearch_stattab table to help postgres do the right thing
        con.execute( f"ANALYZE {nexttable}(diaobjectid)", explain=False )

        # Add in last detection
        q = ( f"/*+ IndexScan(s idx_diasource_diaobjectid)\n"
              f"    Leading( ( (o s) pv ) ) */\n"
              f"UPDATE objsearch_stattab ost\n"
              f"SET lastdetmjd=midpointmjdtai, lastdetband=band,\n"
              f"    lastdetflux=psfflux, lastdetfluxerr=psffluxerr\n"
              f"FROM (\n"
              f"  SELECT DISTINCT ON (diaobjectid) diaobjectid, midpointmjdtai, band, psfflux, psffluxerr\n"
              f"  FROM (\n"
              f"    SELECT DISTINCT ON (o.diaobjectid, s.visit) o.diaobjectid, s.midpointmjdtai,\n"
              f"                                                s.band, s.psfflux, s.psffluxerr\n"
              f"    FROM {nexttable} o\n"
              f"    INNER JOIN diasource s ON o.diaobjectid=s.diaobjectid\n"
              f"    INNER JOIN base_procver_of_procver pv ON s.base_procver_id=pv.base_procver_id\n"
              f"      AND pv.procver_id=%(pv)s\n" )
        _and = "WHERE"
        if statbands is not None:
            q += f"    {_and} s.band=ANY(%(bands)s)\n"
            _and = "  AND"
        if mjd_now is not None:
            q += f"    {_and} s.midpointmjdtai<=%(mjdnow)s\n"
            _and = "  AND"
        q += ( "    ORDER BY o.diaobjectid, s.visit, pv.priority DESC\n"
               "  ) subsubq\n"
               "  ORDER BY diaobjectid, midpointmjdtai DESC\n"
               ") subq\n"
               "WHERE subq.diaobjectid=ost.diaobjectid" )
        con.execute_nofetch( q, subdict )

        # Add in max detection
        q = ( f"/*+ IndexScan(s idx_diasource_diaobjectid)\n"
              f"    Leading( ( ( o s ) pv ) ) */\n"
              f"UPDATE objsearch_stattab ost\n"
              f"SET maxdetmjd=midpointmjdtai, maxdetband=band,\n"
              f"    maxdetflux=psfflux, maxdetfluxerr=psffluxerr\n"
              f"FROM (\n"
              f"  SELECT DISTINCT ON (diaobjectid) diaobjectid, midpointmjdtai, band, psfflux, psffluxerr\n"
              f"  FROM (\n"
              f"    SELECT DISTINCT ON (o.diaobjectid, s.visit) o.diaobjectid, s.midpointmjdtai,\n"
              f"                                                s.band, s.psfflux, s.psffluxerr\n"
              f"    FROM {nexttable} o\n"
              f"    INNER JOIN diasource s ON o.diaobjectid=s.diaobjectid\n"
              f"    INNER JOIN base_procver_of_procver pv ON s.base_procver_id=pv.base_procver_id\n"
              f"      AND pv.procver_id=%(pv)s\n" )
        _and = "WHERE"
        if statbands is not None:
            q += f"    {_and} s.band=ANY(%(bands)s)\n"
            _and = "  AND"
        if mjd_now is not None:
            q += f"    {_and} s.midpointmjdtai<=%(mjdnow)s\n"
            _and = "  AND"
        q += ( "    ORDER BY o.diaobjectid, s.visit, pv.priority DESC\n"
               "  ) subsubq\n"
               "  ORDER BY diaobjectid, psfflux DESC\n"
               ") subq\n"
               "WHERE subq.diaobjectid=ost.diaobjectid" )
        con.execute_nofetch( q, subdict )

        # Add in number of detections
        q = ( f"/*+ IndexScan(s idx_diasource_diaobjectid)\n"
              f"    Leading( ( (o s) pv ) ) */\n"
              f"UPDATE objsearch_stattab o\n"
              f"SET numdet=n "
              f"FROM (\n"
              f"  SELECT diaobjectid, COUNT(visit) AS n\n"
              f"  FROM (\n"
              f"    SELECT DISTINCT ON (o.diaobjectid, s.visit) o.diaobjectid, s.visit\n"
              f"    FROM {nexttable} o\n"
              f"    INNER JOIN diasource s ON s.diaobjectid=o.diaobjectid\n"
              f"    INNER JOIN base_procver_of_procver pv ON s.base_procver_id=pv.base_procver_id\n"
              f"      AND pv.procver_id=%(pv)s\n" )
        _and = "WHERE"
        if statbands is not None:
            q += f"    {_and} s.band=ANY(%(bands)s)\n"
            _and = "  AND"
        if mjd_now is not None:
            q += f"    {_and} s.midpointmjdtai<=%(mjdnow)s\n"
            _and = "  AND"
        q += ( "    ORDER BY o.diaobjectid, s.visit, pv.priority DESC\n"
               "  ) subsubq\n"
               "  GROUP BY diaobjectid\n"
               ") subq\n"
               "WHERE subq.diaobjectid=o.diaobjectid" )
        con.execute_nofetch( q, subdict )
        # ****
        debug_count_temp_table( con, 'objsearch_stattab' )
        # ****

        # Delete from this table based on numdet and detection time as appropriate
        if min_numdetections is not None:
            con.execute_nofetch( "DELETE FROM objsearch_stattab WHERE numdet<%(n)s",
                                 { 'n': min_numdetections } )
            debug_count_temp_table( con, 'objsearch_stattab' )
        if mint_firstdetection is not None:
            con.execute_nofetch( "DELETE FROM objsearch_stattab WHERE firstdetmjd<%(t)s",
                                 { 't': mint_firstdetection } )
            debug_count_temp_table( con, 'objsearch_stattab' )
        if maxt_firstdetection is not None:
            con.execute_nofetch( "DELETE FROM objsearch_stattab WHERE firstdetmjd>%(t)s",
                                 { 't': maxt_firstdetection } )
            debug_count_temp_table( con, 'objsearch_stattab' )
        if mint_lastdetection is not None:
            con.execute_nofetch( "DELETE FROM objsearch_stattab WHERE lastdetmjd<%(t)s",
                                 { 't': mint_lastdetection } )
            debug_count_temp_table( con, 'objsearch_stattab' )
        if maxt_lastdetection is not None:
            con.execute_nofetch( "DELETE FROM objsearch_stattab WHERE lastdetmjd>%(t)s",
                                 { 't': maxt_lastdetection }  )
            debug_count_temp_table( con, 'objsearch_stattab' )
        if mint_maxdetection is not None:
            con.execute_nofetch( "DELETE FROM objsearch_stattab WHERE maxdetmjd<%(t)s",
                                 { 't': mint_maxdetection } )
            debug_count_temp_table( con, 'objsearch_stattab' )
        if maxt_maxdetection is not None:
            con.execute_nofetch( "DELETE FROM objsearch_stattab WHERE maxdetmjd>%(t)s",
                                 { 't': maxt_maxdetection }  )
            debug_count_temp_table( con, 'objsearch_stattab' )
        if mindt_firstlastdetection is not None:
            con.execute_nofetch( "DELETE FROM objsearch_stattab WHERE lastdetmjd-firstdetmjd<%(t)s",
                                 { 't': mindt_firstlastdetection } )
            debug_count_temp_table( con, 'objsearch_stattab' )
        if maxdt_firstlastdetection is not None:
            con.execute_nofetch( "DELETE FROM objsearch_stattab WHERE lastdetmjd-firstdetmjd>%(t)s",
                                 { 't': maxdt_firstlastdetection } )
            debug_count_temp_table( con, 'objsearch_stattab' )

        # Delete from this table based on first/last/max detection magnitude cuts
        if minmag_firstdetection is not None:
            con.execute_nofetch( "DELETE FROM objsearch_stattab WHERE firstdetflux>%(f)s",
                                 { 'f': 10**((minmag_firstdetection-zp)/-2.5) } )
            debug_count_temp_table( con, 'objsearch_stattab' )
        if maxmag_firstdetection is not None:
            con.execute_nofetch( "DELETE FROM objsearch_stattab WHERE firstdetflux<%(f)s",
                                 { 'f': 10**((maxmag_firstdetection-zp)/-2.5) } )
            debug_count_temp_table( con, 'objsearch_stattab' )
        if minmag_lastdetection is not None:
            con.execute_nofetch( "DELETE FROM objsearch_stattab WHERE lastdetflux>%(f)s",
                                 { 'f': 10**((minmag_lastdetection-zp)/-2.5) } )
            debug_count_temp_table( con, 'objsearch_stattab' )
        if maxmag_lastdetection is not None:
            con.execute_nofetch( "DELETE FROM objsearch_stattab WHERE lastdetflux<%(f)s",
                                 { 'f': 10**((maxmag_lastdetection-zp)/-2.5) } )
            debug_count_temp_table( con, 'objsearch_stattab' )
        if minmag_maxdetection is not None:
            con.execute_nofetch( "DELETE FROM objsearch_stattab WHERE maxdetflux>%(f)s",
                                 { 'f': 10**((minmag_maxdetection-zp)/-2.5) } )
            debug_count_temp_table( con, 'objsearch_stattab' )
        if maxmag_maxdetection is not None:
            con.execute_nofetch( "DELETE FROM objsearch_stattab WHERE maxdetflux<%(f)s",
                                 { 'f': 10**((maxmag_maxdetection-zp)/-2.5) } )
            debug_count_temp_table( con, 'objsearch_stattab' )

        nexttable = 'objsearch_stattab'

        # Because the diaforcedsource table is going to be the hugest one,
        #   create an index diabojectid of {nexttable} here to help
        #   this next query along.  We hope.
        con.execute( f"CREATE INDEX idx_t_diaobjectid ON {nexttable}(diaobjectid)", explain=False )

        # Reanalyze this table to help postgres do the right thing
        con.execute( f"ANALYZE {nexttable}(diaobjectid)", explain=False )

        # Get the last forced source
        q = ( f"/*+ IndexScan(f idx_diaforcedsource_diaobjectid )\n"
              # f"    HashJoin( f t )\n"
              f"    Leading( ( (f t) pv ) )\n"
              f"    Parallel( f 3 hard )\n"
              f"*/\n"
              f"SELECT * INTO TEMP TABLE objsearch_final FROM (\n"
              f"  SELECT DISTINCT ON (diaobjectid) *\n"
              f"  FROM (\n"
              f"    SELECT DISTINCT ON (t.diaobjectid, f.visit) t.*,\n"
              f"        f.psfflux AS lastforcedflux, f.psffluxerr AS lastforcedfluxerr,\n"
              f"        f.midpointmjdtai AS lastforcedmjd, f.band AS lastforcedband\n"
              f"    FROM {nexttable} t\n"
              f"    INNER JOIN diaforcedsource f ON f.diaobjectid=t.diaobjectid\n"
              f"    INNER JOIN base_procver_of_procver pv ON f.base_procver_id=pv.base_procver_id\n"
              f"      AND pv.procver_id=%(pv)s\n" )
        _and = "WHERE"
        if statbands is not None:
            q += f"    {_and} f.band=ANY(%(bands)s)\n"
            _and = "  AND"
        if mjd_now is not None:
            q += f"    {_and} f.midpointmjdtai<=%(mjdnow)s\n"
            _and = "  AND"
        q += ( "    ORDER BY t.diaobjectid, f.visit, pv.priority DESC\n"
               "  ) subsubq\n"
               "  ORDER BY diaobjectid, lastforcedmjd DESC\n"
               ") subq" )
        con.execute_nofetch( q, subdict )
        debug_count_temp_table( con, 'objsearch_final' )

        # Filter based on last magnitude
        if min_lastmag is not None:
            con.execute_nofetch( "DELETE FROM objsearch_final WHERE lastforcedflux>%(f)s",
                                 { 'f': 10**((min_lastmag-zp)/-2.5) } )
            debug_count_temp_table( con, 'objsearch_final' )
        if max_lastmag is not None:
            con.execute_nofetch( "DELETE FROM objsearch_final WHERE lastforcedflux<%(f)s",
                                 { 'f': 10**((max_lastmag-zp)/-2.5) } )
            debug_count_temp_table( con, 'objsearch_final' )

        # Pull down the results
        if just_objids:
            rows, columns = con.execute( "SELECT diaobjectid FROM objsearch_final" )
        else:
            rows, columns = con.execute( "SELECT * FROM objsearch_final" )
        colummap = { columns[i]: i for i in range(len(columns)) }
        util.logger.debug( f"object_search returning {len(rows)} objects in format {return_format}" )

    if return_format == 'json':
        rval = { c: [ r[colummap[c]] for r in rows ] for c in columns }
        if ( not just_objids ) and ( 'numdetinwindow' not in rval ):
            rval['numdetinwindow'] = [ None for r in rows ]
        # util.logger.debug( f"returning json\n{json.dumps(rval,indent=4)}" )
        return rval

    elif return_format == 'pandas':
        df = pandas.DataFrame( rows, columns=columns )
        if ( not just_objids ) and ( 'numdetinwindow' not in df.columns ):
            df['numdetinwindow'] = None
        # util.logger.debug( f"object_search pandas dataframe: {df}" )
        return df

    else:
        raise RuntimeError( "This should never happen." )


def get_hot_ltcvs( processing_version, detected_since_mjd=None, detected_in_last_days=None,
                   mjd_now=None, source_patch=False, include_hostinfo=False, host_processing_version=None,
                   object_processing_version=None, dbcon=None ):
    """Get lightcurves of objects with a recent detection.

    Parameters
    ----------
      processing_version: string
        The description of the processing version, or processing version
        alias, to use for searching diasource and diaforcedsource tables.

      detected_since_mjd: float, default None
        If given, will search for all objects detected (i.e. with an
        entry in the diasource table) since this mjd.

      detected_in_last_days: float, default 30
        If given, will search for all objects detected since this many
        days before now.  Can't explicitly pass both this and
        detected_since_mjd.  If detected_since_mjd is given, the default
        here is ignored.

      mjd_now : float, default None
        What "now" is.  By default, this does an MJD conversion of
        datetime.datetime.now(), which is usually what you want.  But,
        for simulations or reconstructions, you might want to pretend
        it's a different time.

      source_patch : bool, default False
        Normally, returned light curves only return fluxes from the
        diaforcedsource table.  However, during the campaign, there will
        be sources detected for which there is no forced photometry.
        (Alerts are sent as sources are detected, but forced photometry
        is delayed.)  Set this to True to get more complete, but
        heterogeneous, lightcurves.  When this is True, it will look for
        all detections that don't have a corresponding forced photometry
        point (i.e. observation of the same ojbject in the same visit),
        and add the detections to the lightcurve.  Be aware that these
        photometry points don't mean exactly the same thing, as forced
        photometry is all at one position, but detections are where they
        are.  This is useful for doing real-time filtering and the like,
        but *not* for any kind of precision photometry or lightcurve fitting.

      include_hostinfo : bool, default False
        If true, return a second data frame with information about the hosts.

      host_processing_version : str, default None
        WARNING : not currently supported.  Right now, it just assumes
        the base processing version of the host is the same as that of
        the diaobject.

      dbcon: psycopg.Connection, db.DBCon, or None
         Database connection to use.  If None, will make a new
         connection and close it when done.

      Returns
      -------
        ltcvdf, objinfo, hostdf

        ltcvdf: pandas.DataFrame
           A dataframe with lightcurves. It is sorted and indexed by
           diaobjectid (bigint) and mjd (float), and has columns:

             visit -- the visit number
             mjd -- the MJD of the obervation
             band -- the filter (u, g, r, i, z, or Y)
             flux -- the PSF flux in nJy
             fluxerr -- uncertaintly on psfflux in nJy
             istdet -- bool, True if this was detected (i.e. has an associated source)
             ispatch -- bool, True if this data is from a source rather than a forced source
                        (only included if source_patch is True)

        objinfo: pandas.DataFrame
           Information about the objects.  Sorted and indexed by diaobjectid,
           with all the columns from diaobject.

        hostdf: pandas.DataFrame or None
           If include_hostinfo is True, then this is a dataframe indexed by diaobjectid with the following columns:
           the following columns

             id -- UUID, the unique UUID primary key of the host galaxy
             objectid -- the objectid of the host galaxy
             base_procver_id -- the processing version of the host galaxy.
             stdcolor_u_g -- "standard" colors as (not really) defined by the DPDD, in AB mags
             stdcolor_g_r --
             stdcolor_r_i --
             stdcolor_i_z --
             stdcolor_z_y --
             stdcolor_u_g_err -- uncertainty on standard colors
             stdcolor_g_r_err --
             stdcolor_r_i_err --
             stdcolor_i_z_err --
             stdcolor_z_y_err --
             petroflux_r -- the flux in nJy within some standard multiple of the petrosian radius
             petroflux_r_err -- uncertainty on petroflux_r
             nearbyextobj1sep -- "Second moment-based separation of nearbyExtObj1 (unitless)" [????]
                                 For SNANA-based sims, this is ROB FIGURE THIS OUT
             pzmean -- mean photoredshift (nominally from the "photoZ_pest" column of the DPD Object table)
             pzstd -- standard deviation of photoredshift (also nominally from "photoZ_pest")

    """

    mjd0 = None

    if host_processing_version is not None:
        raise NotImplementedError( "Error, host processing version is not currently supported." )

    if detected_since_mjd is not None:
        if detected_in_last_days is not None:
            raise ValueError( "Only specify at most one of detected_since_mjd and detected_in_last_days" )
        mjd0 = float( detected_since_mjd )
    else:
        lastdays = 30
        if detected_in_last_days is not None:
            lastdays = float( detected_in_last_days )

    if mjd_now is not None:
        mjd_now = float( mjd_now )
        if mjd0 is None:
            mjd0 = mjd_now - lastdays
    elif mjd0 is None:
        mjd0 = astropy.time.Time( datetime.datetime.now( tz=datetime.UTC )
                                  - datetime.timedelta( days=lastdays ) ).mjd

    bands = [ 'u', 'g', 'r', 'i', 'z', 'y' ]

    with db.DBCon( dbcon ) as con:
        procver = util.procver_id( processing_version, dbcon=con.con )

        # First : get a table of all the object ids (root object ids)
        #   that have a detection (i.e. a diasource) in the
        #   desired time period.

        q = ( "/*+ IndexScan(s idx_diasource_mjd) */\n"
              "SELECT DISTINCT ON(s.diaobjectid) s.diaobjectid\n"
              "INTO TEMP TABLE tmp_objids\n"
              "FROM diasource s\n"
              "INNER JOIN base_procver_of_procver pv ON s.base_procver_id=pv.base_procver_id\n"
              "                                     AND pv.procver_id=%(procver)s\n"
              "WHERE s.midpointmjdtai>=%(t0)s\n" )
        if mjd_now is not None:
            q += "  AND s.midpointmjdtai<=%(t1)s\n"
        q += "ORDER BY s.diaobjectid\n"
        con.execute( q, { 'procver': procver, 't0': mjd0, 't1': mjd_now } )

        # Second: pull out the object info for these objects
        objdf = get_object_infos( objids_table='tmp_objids', dbcon=con, return_format='pandas' )

        # Third : pull out host info for those objects if requested
        # TODO : right now it just pulls out nearby extended object 1.
        # make it configurable to get up to all three.
        # TODO : is distinct on objectid the right thing to do?
        # TODO : think about host processing versions!  Issue #64
        hostdf = None
        if include_hostinfo:
            q = "SELECT o.diaobjectid,\n"
            for bandi in range( len(bands)-1 ):
                q += ( f"       h.stdcolor_{bands[bandi]}_{bands[bandi+1]},"
                       f"h.stdcolor_{bands[bandi]}_{bands[bandi+1]}_err,\n" )
            q += ( "       h.petroflux_r, h.petroflux_r_err, o.nearbyextobj1sep, h.pzmean, h.pzstd\n"
                   "FROM tmp_objids t\n"
                   "INNER JOIN diaobject o ON t.diaobjectid=o.diaobjectid\n"
                   "LEFT JOIN host_galaxy h ON o.nearbyextobj1id=h.objectid\n"
                   "                       AND o.base_procver_id=h.base_procver_id\n"
                   "ORDER BY o.diaobjectid" )
            rows, columns = con.execute( q )
            hostdf = pandas.DataFrame( rows, columns=columns )

        # Fourth: get the lightcurves
        df = many_object_ltcvs( processing_version=processing_version, objids_table='tmp_objids',
                                which='patch' if source_patch else 'forced',
                                return_format='pandas', mjd_now=mjd_now, dbcon=con )

    return df, objdf, hostdf
