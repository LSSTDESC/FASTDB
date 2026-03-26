__all__ = [ "object_ltcv", "object_search", "get_hot_ltcvs" ]

import datetime
import numbers
import uuid
import textwrap
import json   # noqa: F401

from psycopg import sql
import numpy as np
import pandas
import astropy.time

import db
import util
from util import FDBLogger, laboriously_construct_pandas


def get_object_infos( objids=None, objids_table=None, processing_version=None,
                      position_processing_version=None, columns=None, return_format='json', dbcon=None ):
    """Get information from the diaobject table.

    Parameters
    ----------
      objids : list of int or uuid
        Either the diaobjectid or rootid values for the objects to get.
        Either this or objids_table is required.

      objids_table : str, default None
        The name of a table (probably a temporary table) with either a
        diaobjectid or a rootid column that has the ids of the objects
        you want information for.  (If both columns are present, the
        rootid column will be used.)  Cannot pass objids if you
        pass this.

      processing_version : str, uuid, or None
        The processing version (*not* base processing version) for the
        objects.  If not given, will default to 'default'.  Note that if
        you pass a list of integer diaobjectid values in objids, or a
        table with diaobjectids in objids_table, and the
        processing_verson you ask for is not consistent with those ids,
        then you will not get back information on all the objects you
        asked for.

      position_processing_version : str, uuid, or None
        The processing version (*not* base processing version) for the
        object positions.  If None, then the position processing version
        will be assumed to be the same as the diaobject processing
        version.  If both processing_version and position_processing
        version is None, then you will not get back positions, those
        columns will all be null.

      columns : list of str, default None
        If given only include these columns in the returned data.  If
        'diaobjectid' is not included in this list, it will be prepended
        to it.  (You can't not get diaobjectid back, because it's the
        index of the returned dataframe or the keys of the returned
        dictionary.)  See "Returns" below for allowed columns.

      return_format : str, default 'json'
        Either 'pandas' or 'json'

      dbcon : db.DBCon or psycopg.Connection, default None
        If given, use this database connection.  If not given,
        then open a connection, closing it when done.

    Returns
    -------
      rval: pandas.DataFrame or dict
        If return_format is 'pandas', get back a dataframe indexed by
        'diaobject'.  (Not 'rootid', because there may be multiple
        diaobjects for one root, but there will never be multiple roots
        for one diaobject.  Base on how call this function, you might
        get back more than one row with the same rootid.)  If
        return_format is 'json', then the return is a dictionary whose
        keys are the columns names, and whose values are lists all of
        the same length.

        Columns included come from the diaobject and diaboject_position tables:

           diaobjectid         | bigint           | Globally unique (across all proc vers) diaobject id [Index]
           rootid              | uuid             | root_diaobject id for this object
           obj_base_procver    | uuid             | base processing version for the diaobject
           pos_base_procver    | uuid             | base processing version for the diaobject_position
           ra                  | double precision | ra
           dec                 | double precision | dec
           raerr               | real             | uncertainty (NOT variance) on ra
           decerr              | real             | uncertainty (NOT variance) on dec
           ra_dec_cov          | real             | covariance between ra and dec

    """

    if objids_table is not None:
        if dbcon is None:
            raise ValueError( "objids_table requires dbcon" )
        if objids is not None:
            raise ValueError( "objids_table and objids cannot be used together" )
        with db.DBCon( dbcon ) as con:
            q = sql.SQL( "SELECT column_name FROM information_schema.columns "
                         "WHERE table_name={table_name}" ).format( table_name=objids_table )
            rows, _cols = con.execute( q )
            if len(rows) == 0:
                raise RuntimeError( f"Could not find objids table {objids_table}" )
            cols = { r[0] for r in rows }
            if 'rootid' in cols:
                if 'diaobjectid' in cols:
                    FDBLogger.warning( f"Both rootid and diaobjectid are in {objids_table}, using rootid" )
                obj_is_root = True
            elif 'diaobjectid' in cols:
                obj_is_root = False
            else:
                raise RuntimeError( f"Could not find column diaobjectid nor rootid in table {objids_table}" )
    elif objids is None:
        raise ValueError( "must pass either objids or objids_table" )
    else:
        if not util.isSequence( objids ):
            objids = [ objids ]
        if all( isinstance( o, numbers.Integral ) for o in objids ):
            # Make sure they're int, because if it's something like np.int64, postgres may choke
            objids = [ int(o) for o in objids ]
            obj_is_root = False
        else:
            try:
                # See if we they're all uuids
                objids = [ util.asUUID(o) for o in objids ]
                obj_is_root = True
            except ValueError:
                try:
                    # Check to see if they were stringified integers (e.g. from a webap)
                    objids = [ int(o) for o in objids ]
                    obj_is_root = False
                except ValueError:
                    raise ValueError( "objids must be a list of integers or a list of uuids" )
        if len(objids) == 0:
            raise ValueError( "no objids requested" )

    pv = db.ProcessingVersion.procver_id( processing_version if processing_version is not None else 'default' )
    if position_processing_version is None:
        if pv is not None:
            pospv = pv
        elif processing_version is not None:
            pospv = db.ProcessingVersion.procver_id( processing_version )
        else:
            # Pick a random UUID, this will effectively null out the join to the diaobject_position table
            pospv = uuid.uuid4()
    else:
        pospv = db.ProcessingVersion.procver_id( position_processing_version )

    if return_format not in ( 'pandas', 'json' ):
        raise ValueError( f"return_format must be pandas or json, not {return_format}" )

    objcols = [ 'diaobjectid', 'rootid', 'obj_base_procver' ]
    poscols = [ 'pos_base_procver', 'ra', 'dec', 'raerr', 'decerr', 'ra_dec_cov' ]
    joincolumn = "rootid" if obj_is_root else "diaobjectid"
    sqlcolumns = []
    gotsomepos = False
    if columns is None:
        columns = objcols + poscols
    else:
        if not util.isSequence( columns ):
            columns = [ columns ]
        else:
            columns = list( columns )
        if not all( ( c in objcols ) or ( c in poscols ) for c in columns ):
            unknown = set(columns) - set( objcols ).union( poscols )
            raise ValueError( f"Unknown Columns: {unknown}" )
        if 'diaobjectid' not in columns:
            columns = columns.copy()
            columns.insert( 0, 'diaobjectid' )

    for c in columns:
        if c in objcols:
            sqlcolumns.append( sql.Identifier( 'o', c ) if c != 'obj_base_procver'
                               else sql.Identifier( 'b', 'description' ) + sql.SQL( " AS " ) + sql.Identifier( c ) )
        else:
            gotsomepos = True
            sqlcolumns.append( sql.Identifier( 'p', c ) if c != 'pos_base_procver'
                               else sql.Identifier( 'p', 'description' ) + sql.SQL( " AS " ) + sql.Identifier( c ) )
    sqlcolumns = sql.SQL(',').join( c for c in sqlcolumns )

    with db.DBCon( dbcon ) as dbcon:
        if gotsomepos:
            positionclause = sql.SQL( textwrap.dedent(
                """
                SELECT DISTINCT ON(p1.diaobjectid) p1.*, b1.description
                FROM diaobject_position p1
                INNER JOIN base_procver_of_procver p1pv ON p1.base_procver_id=p1pv.base_procver_id
                                                       AND p1pv._table='diaobject_position'
                                                       AND p1pv.procver_id={pospv}
                INNER JOIN base_processing_version b1 ON p1pv.base_procver_id=b1.id
                ORDER BY p1.diaobjectid, p1pv.priority DESC
                """ ) ).format( pospv=pospv )

        if objids_table is not None:
            q = sql.SQL( textwrap.dedent(
                """
                /*+ IndexScan(o idx_diaobject_diaobjectid)
                    IndexScan(p1 idx_diaobject_position_diaobjectid)
                */
                SELECT DISTINCT ON(o.diaobjectid) {sqlcolumns}
                FROM {objids_table} t
                INNER JOIN diaobject o ON o.{joincolumn}=t.{joincolumn}
                INNER JOIN base_procver_of_procver pv ON o.base_procver_id=pv.base_procver_id
                                                     AND pv._table='diaobject'
                                                     AND pv.procver_id={pv}
                INNER JOIN base_processing_version b ON b.id=pv.base_procver_id
                """ ) ).format( sqlcolumns=sqlcolumns,
                                objids_table=sql.Identifier(objids_table),
                                joincolumn=sql.Identifier(joincolumn),
                                pv=pv )
            if gotsomepos:
                q += sql.SQL( "LEFT JOIN (\n" )
                q += positionclause
                q += sql.SQL( ") p ON p.diaobjectid=o.diaobjectid\n" )
            q += sql.SQL( "ORDER BY o.diaobjectid" )
        else:
            q = sql.SQL( textwrap.dedent(
                """
                /*+ IndexScan(o idx_diaobject_diaobjectid)
                    IndexScan(p1 idx_diaobject_position_diaobjectid)
                */
                SELECT DISTINCT ON(o.diaobjectid) {sqlcolumns}
                FROM diaobject o
                INNER JOIN base_procver_of_procver pv ON o.base_procver_id=pv.base_procver_id
                                                     AND pv._table='diaobject'
                                                     AND pv.procver_id={pv}
                INNER JOIN base_processing_version b ON b.id=pv.base_procver_id
                """ ) ).format( sqlcolumns=sqlcolumns, pv=pv )
            if gotsomepos:
                q += sql.SQL( "LEFT JOIN (\n" )
                q += positionclause
                q += sql.SQL( ") p ON p.diaobjectid=o.diaobjectid\n" )
            q += sql.SQL( "WHERE o.{joincolumn}=ANY(%(objids)s)\n"
                          "ORDER BY o.diaobjectid" ).format( joincolumn=sql.Identifier(joincolumn) )

        # ****
        # TEMP DEBUGGING, TAKE THIS OUT
        # dbcon.echoqueries = True
        # ****
        rows, cols = dbcon.execute( q, { 'objids': objids } )
        # Next line deals with what I think is a dysfunctional psycopg return
        cols = columns if len(rows) == 0 else cols
        if return_format == 'pandas':
            df = laboriously_construct_pandas( rows, columns=cols,
                                               int64cols=[ 'diaobjectid' ],
                                               doublecols=[ 'ra', 'dec' ],
                                               floatcols=[ 'raerr', 'decerr', 'ra_dec_cov' ],
                                               ignore_missing_cols=True
                                              )
            if len(df) > 0:
                df.set_index( 'diaobjectid', inplace=True )
            return df
        elif return_format == 'json':
            return { c: [ r[i] for r in rows ] for i, c in enumerate( cols ) }
        else:
            raise RuntimeError( "This should never happen" )


def many_object_ltcvs( processing_version='default', objids=None, objids_table=None,
                       bands=None, which='patch', include_base_procver=False, include_source_positions=False,
                       use_weighted_source_positions=False, always_use_weighted_source_positions=False,
                       return_format='json',
                       return_object_info=False, object_processing_version=None, position_processing_version=None,
                       mjd_now=None, dbcon=None ):
    """Get lightcurves for objects.

    Parameters
    ----------
      processing_version : UUID or str, default 'default'
         The processing version (or alias) to search photometry.

      objids: int, uuid, list of int, or list of uuid
         Objects to search for.  If None, will get ALL OBJECTS; if
         you're doing this, you probably want to use limit and offset,
         but those aren't implemented yet, so don't do this!  If the
         values ints, they are interpreted as be diaobjectids.  If the
         values or uuids (or strings that can be made into uuids), they
         will be interpreted as rootids.  If ints, the diaobjectid must
         be consistent with processing_version, or nothing will be
         found.  If uuids, then the diaobjectids found will be the ones
         that match the photometry with the right processing_version.

      objid_table : str, default None
         If not None, then this is the name of a table (probably a
         temporary table) that has the object ids and/or root object ids
         already loaded into it in the columns "diaobjectid" or
         "rootid", respectively.  (If both column are loaded, "rootid"
         will be used and "diaobjectid" will be ignored.)  Use of this
         requires dbcon to be non-None.

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

      include_source_positions : bool, default False
         If True, there will be additional columns (ra, dec, raerr,
         decerr, ra_dec_cov) that have the positions that were found for
         the sources (the detections).

      return_format : str, default 'json'
         'json' or 'pandas'

      return_object_info : bool, default False
         If True, you get a second return.  See Returns below

      object_processing_version : str or uuid, default None
         The processing version for getting object info.  YOU REALLY
         WANT TO GET THIS RIGHT, otherwise the object info table won't
         have what you expect in it.  Defaults to the same as
         processing_version, which is often what you want, but not
         always.  Ignored if return_object_info is False.

      position_processing_version : str or uuid, default None
         The processing version for getting object position info.
         Defaults to the same as object_processing_version.

      use_weighted_source_positions : bool, default False
         See Returns below

      always_use_weighted_source_positions : bool, default False
         See Returns below.  Implies use_weighted_source_positions.

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
      retval: either one or two things.  Only one thing if
        return_object_info is False, two things if return_object_info is
        True.

        The first thing is a pandas.DataFrame or a list.  If
        return_format is 'pandas', then you get back a DataFrame with
        indexes (rootid, mjd).  The columns are:

            diaobjectid : bigint, the diaObjectId associated with this diasource
                          or diaforcedsource (*see below)
            diasourceid : bigint, the diaSoruceId, or null
            [ diafordedsourceid : bigint or null; only included if which isn't 'detections' ]
            visit : bigint
            band : str
            flux : float
            fluxerr : float
            isdet : int ; 1 if there is a diaSource at this mjd, 0 otherwise
            [ ispatch : int; 1 if photomtery is from a diaSource, 0 if it's from a diaForcedSource;
                        only included if which is 'patch' ]
            [ base_procver_s : str uuid, only included if include_base_procver is True ]
            [ base_procver_f : str uuid, only included if include_base_procver is True and which isn't 'detections' ]
            [ det_ra : double or None, only included if include_source_positions is True ]
            [ det_dec : double or None, only included if include_source_positions is True ]
            [ det_raerr : float or None, only included if include_source_positions is True ]
            [ det_decerr : float or None, only included if include_source_positions is True ]
            [ det_ra_dec_cov : float or None, only included if include_source_positions is True ]


        If return_format is 'json', then you get back a list, each
        element of which is a dictionary.  The dictionary has key
        'rootid', the value which is the uuid of the object.  The other
        keys are mjd and all the columns that you'd get in pandas; the
        values of each of these is a list, which gives the lightcurve
        for this object.

        If return_object_info is True, then there's a second return,
        which is another dataframe or dictionary (based on
        return_format).  The columns of the dataframe, or the keys if
        the dictionary, are 'diaobjectid', 'rootid',
        'obj_base_procver_id', 'pos_base_procver_id', 'ra', 'dec',
        'raerr', 'decerr', 'ra_dec_cov'.  The dataframe is indexed by
        diaobjectid, *not* rootid.  Reason: there may be multiple
        diaobjectids for the same rootid, but not vice versa.  (This
        means that to use this, you have to be a little careful.)  If
        you specified a list of integer diaobjectids, instead of
        rootids, in the function call, you may get back more than you
        asked for, because internally the database works on rootids.
        (*See below.)

        Normally, the position fields come from the diaobject_position
        table.  While presumably for data releases this is going to be
        the best possible position, for objects and sources from alerts,
        the provenance of the diaobject_position table is highly dubious
        for multiple reasons.  What's more, because of limited
        information from some brokers, there may be some diaobjects for
        which we don't have a diaobject_position.  In that case, these
        fields will all be null.  If use_weighted_source_positions is
        true, then for objects for which we don't have a
        diaobject_position, these fields will be filled with (S/N)²
        weighted averages from the ra and dec of all sources with S/N>3.
        (They may still be null, if there aren't any sources with high
        enough S/N, or if we don't have positions for those sources,
        which can also happen due to limited information from brokers.)
        If always_use_weighted_source_positions is True, then there will
        never be information from diaobject_position here, only weighted
        averages from source positions.

        *NOTE ON diaobjectid : empirically, these are not unique in the
         LSST alert stream.  Not only does LSST have morethan one
         diaOjbect at the same position on the sky, but at different
         times in the alerts the same diaSource will be associated with
         a different diaObject.  (For instance, the diaObject that a
         diaSource had when it was first discovered may be differnt from
         the diaOjbect associated with a diaSource when it shows up in
         the prvDiaSources array in a later alert.)  FASTDB only stores
         the diaObjectId of a diaSource the *first* time it heard about
         it; it does not try to track all the diaObjectId values that
         were ever associated with a diaSource in the alert stream.  For
         all these reasons, it's much safer to use rootid where possible
         to identify objects.  FASTDB deduplicates these by associating
         objects that are within (by default) 1" of each other as the
         same rootid.  (This is still not a perfect process, and we
         could get into the weeds discussing it, so be aware that there
         may be two rootids very close to each other that are actually
         the same physical event, and each will have a different set of
         diaSources associated with it, whereas what you might really
         want is the union of those.)

    """

    # Parse objids, set objfield
    if objids_table is not None:
        if dbcon is None:
            raise ValueError( "objids_table requires dbcon" )
        if objids is not None:
            raise ValueError( "objids_table and objids cannot be used together" )
    else:
        objids_table = 'tmp_objids'
        if objids is None:
            raise ValueError( "objids is required" )
        if not util.isSequence( objids ):
            objids = [ objids ]
        if all( isinstance( o, numbers.Integral ) for o in objids ):
            # Make sure they're int, because if it's something like np.int64, postgres will choke
            objids = [ int(o) for o in objids ]
            objids_are_root = False
        else:
            objids_are_root = True
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

    # Figure out what stuff we're going to have to get
    use_weighted_source_positions = use_weighted_source_positions or always_use_weighted_source_positions
    if use_weighted_source_positions and ( not return_object_info ):
        FDBLogger.warning( "Asked for weighted source positions, but return_object_info was False. "
                           "Ignoring weighted source positions." )
        use_weighted_source_positions = False
    must_get_source_positions = include_source_positions or use_weighted_source_positions or return_object_info

    with db.DBCon( dbcon ) as dbcon:
        pvid = db.ProcessingVersion.procver_id( processing_version, dbcon=dbcon )
        if return_object_info:
            objpvid = ( db.ProcessingVersion.procver_id( object_processing_version, dbcon=dbcon )
                        if object_processing_version is not None else pvid )
            pospvid = ( db.ProcessingVersion.procver_id( position_processing_version, dbcon=dbcon )
                        if position_processing_version is not None else objpvid )

        if objids is not None:
            # For efficiency, we're going to make a first pass and extract just the object ids.
            # If these are root ids, then we can't be sure which diaobjectid will correspond
            # to them, so we will pull the *all* out.  (Think about this.  It's possible we
            # could be doing something with the object's processing version, but consisder
            # all the complicated messy cases.)

            q = sql.SQL( "CREATE TEMP TABLE tmp_objids( diaobjectid bigint, rootid uuid )" )
            dbcon.execute( q, explain=False )
            if objids_are_root:
                q = sql.SQL( textwrap.dedent(
                    """INSERT INTO tmp_objids (
                         SELECT diaobjectid, rootid FROM diaobject
                         WHERE rootid=ANY(%(roots)s)
                       )
                    """
                ) )
                dbcon.execute_nofetch( q, { 'roots': objids } )
            else:
                q = sql.SQL( "CREATE TEMP TABLE temp_input_diaobject( diaobjectid bigint )" )
                dbcon.execute( q, explain=False )
                q = sql.SQL( "COPY temp_input_diaobject(diaobjectid) FROM STDIN"
                            ).format( objids_table=sql.Identifier( objids_table ) )
                with dbcon.cursor.copy( q ) as copier:
                    for objid in objids:
                        copier.write_row( [ objid ] )
                # Join all other objectids that are from the same roots
                #   and base processing verson as these.  Frustratingly,
                #   in the LSST alert stream, there were cases where the
                #   same physical object had different diaObjectIds.
                #   There were even cases, in previous source arrays,
                #   where the same diaSource was associated with
                #   different values of diaObjectId at different times.
                #   (Not within the same alert, but in different alerts
                #   that had the same diaSourceId in the previous
                #   sources array.)
                q = sql.SQL( textwrap.dedent(
                    """
                    INSERT INTO tmp_objids( diaobjectid, rootid ) (
                      SELECT o.diaobjectid, o.rootid FROM temp_input_diaobject t
                      INNER JOIN diaobject ot ON ot.diaobjectid=t.diaobjectid
                      INNER JOIN diaobject o ON ot.rootid=o.rootid
                                            AND ot.base_procver_id=o.base_procver_id
                    )
                    """ ) )
                dbcon.execute( q )

        # Extract detections
        pos_fields = sql.SQL( "det_ra double precision, det_dec double precision, "
                              "det_raerr real, det_decerr real, det_ra_dec_cov real,"
                              if must_get_source_positions
                              else "" )
        procver_fields = sql.SQL( "base_procver_s text, " if include_base_procver else "" )
        q = sql.SQL( textwrap.dedent(
            """
            CREATE TEMP TABLE tmp_sources( rootid uuid, diasourceid bigint, diaobjectid bigint, visit bigint,
                                           mjd double precision, band text, flux real, fluxerr real,
                                           {pos_fields} {procver_fields}
                                           isdet bool )
            """
        ) ).format( pos_fields=pos_fields, procver_fields=procver_fields )
        dbcon.execute( q, explain=False )

        pos_fields = sql.SQL( "ra AS det_ra, dec AS det_dec, raerr AS det_raerr, "
                              "decerr AS det_decerr, ra_dec_cov AS det_ra_dec_cov, "
                              if must_get_source_positions
                              else "" )
        procver_fields = sql.SQL( "p.description AS base_procver_s, " if include_base_procver else "" )
        q = sql.SQL( textwrap.dedent(
            """
            /*+ IndexScan(s idx_diasource_diaobjectid) */
            INSERT INTO tmp_sources
            SELECT DISTINCT ON (t.rootid, s.visit)
              t.rootid, s.diasourceid, s.diaobjectid, s.visit, s.midpointmjdtai AS mjd,
              s.band, s.psfflux AS flux, s.psffluxerr AS fluxerr, {pos_fields} {procver_fields}
              TRUE as isdet
            FROM {objids_table} t
            INNER JOIN diasource s ON s.diaobjectid=t.diaobjectid
            INNER JOIN base_procver_of_procver pv ON s.base_procver_id=pv.base_procver_id
                                                 AND pv._table='diasource'
                                                 AND pv.procver_id={procver}
            """
        ) ).format( procver=pvid, objids_table=sql.Identifier(objids_table),
                    pos_fields=pos_fields, procver_fields=procver_fields )
        if include_base_procver:
            q += sql.SQL( "INNER JOIN base_processing_version p ON pv.base_procver_id=p.id" )
        _and = "WHERE"
        if mjd_now is not None:
            q += sql.SQL( f"                   {_and} s.midpointmjdtai<={{t0}}" ).format( t0=mjd_now )
            _and = "  AND"
        if bands is not None:
            q += sql.SQL( f"                   {_and} s.band=ANY(%(bands)s)" )
            _and = "  AND"
        q += sql.SQL( "   ORDER BY t.rootid, s.visit, pv.priority DESC\n")
        dbcon.execute_nofetch( q, { 'bands': bands } )

        if which == 'detections':
            rows, cols = dbcon.execute( "SELECT * FROM tmp_sources "
                                        "ORDER BY rootid, mjd" )

        else:
            # Extract forced photometry if necessary
            procver_fields = sql.SQL( "base_procver_f text, " if include_base_procver else "" )
            q = sql.SQL( textwrap.dedent(
                """CREATE TEMP TABLE tmp_forced( rootid uuid, diaforcedsourceid bigint,
                                                 diaobjectid bigint, visit bigint,
                                                 mjd double precision, band text,
                                                 flux real, fluxerr real, {procver_fields}
                                                 isdet bool )
                """
            ) ).format( procver_fields=procver_fields )
            dbcon.execute( q, explain=False )
            procver_fields = sql.SQL( "p.description as base_procver_f, " if include_base_procver else "" )
            q = sql.SQL( textwrap.dedent(
                """
                /*+ IndexScan(s idx_diaforcedsource_diaobjectid) */
                INSERT INTO tmp_forced
                SELECT DISTINCT ON (t.rootid, s.visit)
                  t.rootid, s.diaforcedsourceid, s.diaobjectid, s.visit, s.midpointmjdtai AS mjd,
                  s.band, s.psfflux AS flux, s.psffluxerr AS fluxerr, {procver_fields}
                  FALSE as isdet
                FROM {objids_table} t
                INNER JOIN diaforcedsource s ON s.diaobjectid=t.diaobjectid
                INNER JOIN base_procver_of_procver pv ON s.base_procver_id=pv.base_procver_id
                                                     AND pv._table='diaforcedsource'
                                                     AND pv.procver_id={procver}
                """
            ) ).format( procver=pvid, procver_fields=procver_fields, objids_table=sql.Identifier(objids_table) )
            if include_base_procver:
                q += sql.SQL( "INNER JOIN base_processing_version p ON pv.base_procver_id=p.id" )
            _and = "WHERE"
            if mjd_now is not None:
                q += sql.SQL( f"                   {_and} s.midpointmjdtai<={{t0}}" ).format( t0=mjd_now )
                _and = "  AND"
            if bands is not None:
                q += sql.SQL( f"                   {_and} s.band=ANY(%(bands)s)" )
                _and = "  AND"
            q += sql.SQL( "   ORDER BY t.rootid, s.visit, pv.priority DESC\n" )
            dbcon.execute_nofetch( q, { 'bands': bands } )

            # Join detections to forced photometry to set the 'isdet' and 'ispatch' flags.
            # The term FULL OUTER JOIN is extremely scary, of course
            pos_fields = sql.SQL( "s.det_ra, s.det_dec, s.det_raerr, s.det_decerr, s.det_ra_dec_cov, "
                                  if must_get_source_positions
                                  else "" )
            procver_fields = sql.SQL( "f.base_procver_f, s.base_procver_s, " if include_base_procver else "" )
            q = sql.SQL( textwrap.dedent(
                """
                SELECT CASE WHEN f.rootid IS NULL THEN s.rootid ELSE f.rootid END AS rootid,
                       f.diaforcedsourceid,
                       s.diasourceid,
                       {procver_fields}
                       CASE WHEN f.diaobjectid IS NULL THEN s.diaobjectid ELSE f.diaobjectid END AS diaobjectid,
                       CASE WHEN f.visit IS NULL THEN s.visit ELSE f.visit END AS visit,
                       CASE WHEN f.mjd IS NULL THEN s.mjd ELSE f.mjd END AS mjd,
                       CASE WHEN f.band IS NULL THEN s.band ELSE f.band END AS band,
                       CASE WHEN f.flux IS NULL THEN s.flux ELSE f.flux END AS flux,
                       CASE WHEN f.fluxerr IS NULL THEN s.fluxerr ELSE f.fluxerr END AS fluxerr,
                       {pos_fields}
                       CASE WHEN s.mjd IS NULL THEN FALSE ELSE TRUE END AS isdet,
                       CASE WHEN f.mjd IS NULL THEN TRUE ELSE FALSE END as ispatch
                FROM tmp_forced f
                FULL OUTER JOIN tmp_sources s ON f.rootid=s.rootid AND s.visit=f.visit
                ORDER BY rootid, mjd
                """ ) ).format( pos_fields=pos_fields, procver_fields=procver_fields )
            rows, cols = dbcon.execute( q )

        # We might also need to get object info
        if return_object_info:
            columns = [ 'diaobjectid', 'rootid' ]
            if include_base_procver:
                columns.extend( [ 'obj_base_procver', 'pos_base_procver' ] )
            if not always_use_weighted_source_positions:
                columns.extend( [ 'ra', 'dec', 'raerr', 'decerr', 'ra_dec_cov' ] )
            objdf = get_object_infos( objids_table=objids_table, processing_version=objpvid,
                                      position_processing_version=pospvid, columns=columns,
                                      return_format='pandas', dbcon=dbcon )

    # ...OK. I can't do this next one, because pandas can't handle None as integers,
    #   and will silently convert them to doubles so it can put NA in place.
    #   That loses precision off of 64-bit integers.  I should really move away
    #   from using pandas.
    #   (There may be some hope with pandas and pyarrow datatypes, but I think
    #   that would still require a lot of intervention.)

    # ltcvsdf = pandas.DataFrame( rows, columns=cols )

    #   Because pandas.DataFrame only allows a single dtype in the constructor,
    #   we have to do it the long way.

    ltcvsdf = laboriously_construct_pandas( rows, cols,
                                            int64cols=['diaforcedsourceid', 'diasourceid', 'diaobjectid', 'visit'],
                                            floatcols=['flux', 'fluxerr', 'det_raerr', 'det_decerr', 'det_ra_dec_cov'],
                                            doublecols=['mjd', 'det_ra', 'det_dec'],
                                            boolcols=['isdet', 'ispatch'],
                                            ignore_missing_cols=True )

    # Update object positions if necessary
    if use_weighted_source_positions and return_object_info:
        if always_use_weighted_source_positions:
            # Pandas is so annoying.  Things like objdf.at[ :, 'pos_base_procver'] = None was
            #   not working.  So, drop the columns first, then maybe it will work.
            cols = [ i for i in [ 'pos_base_procver', 'ra', 'dec', 'raerr', 'decerr', 'ra_dec_cov' ]
                     if i in objdf.columns ]
            if len( cols ) > 0:
                objdf.drop( columns=cols, inplace=True )
            if include_base_procver:
                objdf.at[ :, 'pos_base_procver' ] = None
            objdf.at[ :, 'ra' ] = None
            objdf.at[ :, 'dec' ]= None
            objdf.at[ :, 'raerr' ] = None
            objdf.at[ :, 'decerr' ] = None
            objdf.at[ :, 'ra_dec_cov' ] = None

        # I'm not sure we can count on really getting ra_err for everything, so instead
        #  of using that for weighting, we'll use S/N^2 (like variance weighting)
        usesource = ltcvsdf[ ( ltcvsdf.flux > ltcvsdf.fluxerr * 3. ) &
                             ( ~pandas.isna(ltcvsdf.det_ra) ) &
                             ( ~pandas.isna(ltcvsdf.det_dec) ) ].reset_index()
        if len(usesource) > 0:
            # Only do things if there are things to do.
            usesource = usesource.loc[ :, [ 'rootid', 'flux', 'fluxerr', 'det_ra', 'det_dec' ] ]
            # Flux and fluxerr are floats.  To avoid floating point roundoff in sums, make sure that
            #   the weight array is doubles.
            usesource['weight'] = pandas.Series( usesource['flux'] / usesource['fluxerr'], dtype='float64[pyarrow]' )
            usesource['weight'] = usesource['weight'] ** 2
            usesource['weightedra'] = usesource['weight'] * usesource['det_ra']
            usesource['weighteddec'] = usesource['weight'] * usesource['det_dec']
            combsource = usesource.groupby('rootid').agg( sumra=('weightedra', 'sum'),
                                                          sumdec=('weighteddec', 'sum'),
                                                          sumweight=('weight', 'sum') )
            combsource['ra'] = combsource['sumra'] / combsource['sumweight']
            combsource['dec'] = combsource['sumdec'] / combsource['sumweight']
            usesource = usesource.join( combsource, how='inner', on='rootid' )
            usesource['weightedvarra'] = usesource['weight'] * ( usesource['det_ra'] - usesource['ra'] ) ** 2
            usesource['weightedvardec'] = usesource['weight'] * ( usesource['det_dec'] - usesource['dec'] ) **2
            usesource['weightedcovar'] = usesource['weight'] * ( ( usesource['det_ra'] - usesource['ra'] ) *
                                                                 ( usesource['det_dec'] - usesource['dec'] ) )
            combsourcevar = usesource.groupby('rootid').agg( sumvarra=('weightedvarra', 'sum'),
                                                             sumvardec=('weightedvardec', 'sum'),
                                                             sumcovar=('weightedcovar', 'sum'),
                                                             sumweight=('weight', 'sum') )
            combsourcevar['ra_err'] = np.sqrt( combsourcevar['sumvarra'] / combsourcevar['sumweight'] )
            combsourcevar['dec_err'] = np.sqrt( combsourcevar['sumvardec'] / combsourcevar['sumweight'] )
            combsourcevar['ra_dec_cov'] = combsourcevar['sumcovar'] / combsourcevar['sumweight']
            combsourcevar = combsourcevar.loc[ :, ['ra_err', 'dec_err', 'ra_dec_cov'] ]
            combsource = combsource.join( combsourcevar, how='inner' )

            # ****
            # I put this in for debugging purposes.  It lead to the conversion of the weights
            #   dtype above to double....  (Tests were failing.)
            # FDBLogger.info( "WEIGHTED POSITIONS FROM GET_HOT_TLCVS" )
            # import io
            # thing = usesource.set_index( 'rootid' )
            # for rootid in ltcvsdf.index.unique(level='rootid').values: # thing.index.unique().values:
            #     if rootid in thing.index.values:
            #         subltcvsdf = thing.xs( rootid )
            #         strio = io.StringIO()
            #         strio.write( f"For rootid {rootid}, there are {len(subltcvsdf)} values to combine.\n" )
            #         for row in subltcvsdf.itertuples():
            #             strio.write( f"     ra={row.det_ra:12.8f}  dec={row.det_dec:12.8f}  weight={row.weight}\n" )
            #         strio.write( f"  Combined: ra={combsource.loc[rootid,'ra']:12.8f}, "
            #                      f" dec={combsource.loc[rootid, 'dec']:12.8f}\n" )
            #         strio.write( f"  sumra={combsource.loc[rootid, 'sumra']}, "
            #                      f"sumdec={combsource.loc[rootid, 'sumdec']}, "
            #                      f"sumweight={combsource.loc[rootid, 'sumweight']}\n" )
            #         FDBLogger.info( strio.getvalue() )
            #     else:
            #         FDBLogger.info( f"For rootid {rootid}, there are not any high s/n sources.\n" )
            # ****

            # There is probably a cleverer pandas way to do this
            #   that would avoid a for loop
            for row in objdf.itertuples():
                if ( pandas.isna( row.ra ) and pandas.isna( row.dec ) and
                     row.rootid in combsource.index.values ):
                    if include_base_procver:
                        objdf.at[ row.Index, 'pos_base_procver' ] = None
                    objdf.at[ row.Index, 'ra' ] = combsource.loc[ row.rootid, 'ra' ]
                    objdf.at[ row.Index, 'dec' ] = combsource.loc[ row.rootid, 'dec' ]
                    objdf.at[ row.Index, 'raerr' ] = combsource.loc[ row.rootid, 'ra_err' ]
                    objdf.at[ row.Index, 'decerr' ] = combsource.loc[ row.rootid, 'dec_err' ]
                    objdf.at[ row.Index, 'ra_dec_cov' ] = combsource.loc[ row.rootid, 'ra_dec_cov' ]

    if must_get_source_positions and ( not include_source_positions ):
        ltcvsdf.drop( columns=[ 'det_ra', 'det_dec', 'det_raerr', 'det_decerr', 'det_ra_dec_cov' ], inplace=True )

    if which == 'forced':
        if len(ltcvsdf) > 0:
            # Pandas annoyance: if retframe has 0 length, this next
            #   statement wipes out the columns.  Grrr.
            ltcvsdf = ltcvsdf[ ltcvsdf.ispatch==0 ]
        ltcvsdf.drop( 'ispatch', axis='columns', inplace=True )

    if return_format == 'pandas':
        ltcvsdf.set_index( ['rootid', 'mjd'], inplace=True )
        if return_object_info:
            return ltcvsdf, objdf
        else:
            return ltcvsdf

    elif return_format == 'json':
        retval = []
        for objid in ltcvsdf.rootid.unique():
            subf = ltcvsdf[ ltcvsdf.rootid==objid  ]
            thisretval = { 'rootid': subf.rootid.iloc[0],
                           'diaobjectid': list( subf.diaobjectid.values ),
                           'visit': list( subf.visit.values ),
                           'diasourceid': list( subf.diasourceid.values )
                          }
            if which != 'detections':
                thisretval['diaforcedsourceid'] = list( subf.diaforcedsourceid.values )
            thisretval.update( {'mjd': list( subf.mjd.values ),
                                'band': list( subf.band.values ),
                                'flux': list( subf.flux.values ),
                                'fluxerr': list( subf.fluxerr.values ),
                                'isdet': [ int(i) for i in subf.isdet.values ] } )
            if include_source_positions:
                thisretval.update( { 'det_ra': list( subf.det_ra.values ),
                                     'det_dec': list( subf.det_dec.values ),
                                     'det_raerr': list( subf.det_raerr.values ),
                                     'det_decerr': list( subf.det_decerr.values ),
                                     'det_ra_dec_cov': list( subf.det_ra_dec_cov.values )
                                    } )
            if which == 'patch':
                thisretval['ispatch'] = [ int(i) for i in subf.ispatch.values ]
            if include_base_procver:
                thisretval['base_procver_s'] = list( subf.base_procver_s )
                if which != 'detections':
                    thisretval['base_procver_f'] = list( subf.base_procver_f )
            retval.append( thisretval )

        if return_object_info:
            objdf.reset_index( inplace=True )
            objjson = { c: list( objdf.loc[ :, c ] ) for c in objdf.columns }
            return retval, objjson
        else:
            return retval

    else:
        raise RuntimeError( "This should never happen." )


def object_ltcv( processing_version='default', diaobjectid=None, bands=None, which='patch',
                 include_base_procver=False, include_source_positions=False,
                 use_weighted_source_positions=False, always_use_weighted_source_positions=False,
                 return_format='json',
                 return_object_info=False, object_processing_version=None, position_processing_version=None,
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

      include_source_positions : bool, default False
         If True, there will be additional columns (ra, dec, raerr,
         decerr, ra_dec_cov) that have the positions that were found for
         the sources (the detections).

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
       One or two things, similar to what you get back from
       many_object_ltcvs only if pandas, there is no index, and 'rootid'
       column has been removed as there will only be one.

    """

    rval = many_object_ltcvs( processing_version=processing_version,
                              objids=[ diaobjectid ],
                              bands=bands,
                              which=which,
                              include_base_procver=include_base_procver,
                              include_source_positions=include_source_positions,
                              return_object_info=return_object_info,
                              object_processing_version=object_processing_version,
                              position_processing_version=position_processing_version,
                              use_weighted_source_positions=use_weighted_source_positions,
                              always_use_weighted_source_positions=always_use_weighted_source_positions,
                              return_format=return_format,
                              mjd_now=mjd_now,
                              dbcon=dbcon )
    if return_object_info:
        ltcv = rval[0]
        objinfo = rval[1]
    else:
        ltcv = rval

    if len(ltcv) == 0:
        raise RuntimeError( f"Could not find object for diaobjectid {diaobjectid}" )

    if return_format == 'pandas':
        ltcv.reset_index( inplace=True )
        ltcv.drop( 'rootid', axis='columns', inplace=True )
    else:
        ltcv = ltcv[0]

    if return_object_info:
        return ltcv, objinfo
    else:
        return ltcv


def debug_count_temp_table( con, table ):
    res = con.execute( f"SELECT COUNT(*) FROM {table}" )
    FDBLogger.debug( f"Table {table} has {res[0][0]} rows" )


def object_search( processing_version='default', ignore_object_processing_version=False,
                   object_processing_version=None, position_processing_version=None,
                   return_format='json', just_objids=False, noforced=False, dbcon=None, mjd_now=None,
                   **kwargs ):
    """Search for objects.

    For parameters that define the search, if they are None, they are
    not considered in the search.  (I.e. that filter will be skipped.)

    THIS IS WAY TOO SLOW RIGHT NOW.  Even in tests with modest database.
    TODO: explore and figure out where the queries are slow, make them
    faster.

    Parameters
    ----------
      processing_version : UUID or str
         The processing version you're looking at (for sources and forced sources).

      ignore_object_processing_version: book default False
          If True, then consider all diaobjects.  In practice, if you're
          doing any detection cuts, only objects that had photometry in
          the desired processing_version will be included anyway.  Only
          set this to True if you know what you're doing, because it
          probably makes things slower.  (But, it may be necessary
          eventually.)

      object_processing_version : UUID or str, default None
          The processing version for objects.  Defaults to
          processing_version if not given.

      position_processing_version : UUID or str, default None
          The processing version of diaobject positions.  If None, will
          use the same as processing_version.  IMPORTANT.  This one
          is easy to get wrong.  Make sure you know what processing
          versions are in the database and what to do with them.

      return_format : string
         Either "json" or "pandas".  (TODO: pyarrow? polars?)  See "Results" below.

      just_objids : bool, default False
         See "Returns" below.

      noforced : bool, default False
         Do not include forced photometry.  This can speed up the
         queries.  Ignored if either min_lastmag or max_lastmag is
         given.

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
      #    NOT IMPLEMENTED.  May never be.  Intended to be a time window
      #    around maximum-flux detection.

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
      #    (Not yet implemented.  May never be.)

      mindt_firstlastdetection : float, default None
         Only return objects that have at least this many days between the first and last detection.

      maxdt_firstlastdetection : float, default None
         Only return objects that have at most this many days between the first and last detection.


      min_lastmag : float, default None
         The most recent measurement (not detection! searches forced
         photometry) must have a magnitude that is at least this.  (Use
         this to filter out things that are too bright.)

      max_lastmag : float, default None
         The most recent measurement (not detection! searches forced
         photometry) must have a magnitude that is at most this.  (Use this
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
    -------
      retval : pandas.DataFrame or dict

      A table of data.  If just_objids is true, this will have a single
      column, "rootid" that has the root diaobject ids that match the
      search.  Otherwise, there will be additional columns.  (For all of
      these columns, assume that there is a "within statbands" in the
      definition.)

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

      If noforced is False, and min_lastmag and max_lastmag are both None, there will also be columns:

          lastforcedmjd — mjd of last forced-photometry measurement
          lastforcedband — band of last forced-photometry measurement
          lastforcedflux — flux (nJy) of last forced-photometry detection
          lastforcedfluxerr — uncertainty on lastforcedflux

      Note that it is possible that the latest detection will be *later*
      than the latest forced photometry point!  This will happen when
      detections have been reported, but forced photometry is not fully
      up to date.

      If return_format is json, then the return is actually a
      dictionary; the keys of the dictionary are the names listed above,
      and the values are lists.  All lists have the same length.

      The table *may* be sorted by diaobjectid, but don't depend on
      this; this function makes no promises about the sorting of the
      results.

    """

    FDBLogger.debug( f"In object_search : kwargs = {kwargs}" )
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
        posprocver = procver if position_processing_version is None else util.procver_id( position_processing_version )
        objprocver = ( None if ignore_object_processing_version
                       else procver if object_processing_version is None
                       else util.procver_id( object_processing_version ) )

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

        addlfields = []

        # So that futher queries won't have to worry about the object procver join
        #  or the root join for position, make a view that does that for us
        if ra is not None:
            q = sql.SQL( textwrap.dedent(
                """
                CREATE TEMPORARY VIEW tmp_diaobject_with_position AS
                SELECT DISTINCT ON (o.rootid) o.rootid, p.ra, p.dec""" ) )
        else:
            q = sql.SQL( textwrap.dedent(
                """
                CREATE TEMPORARY VIEW tmp_diaobject_with_position AS
                SELECT DISTINCT ON (o.rootid) o.rootid, r.ra, r.dec""" ) )
        if objprocver is not None:
            q += sql.SQL( ", pv.priority" )
        q += sql.SQL( "\nFROM diaobject o\n" )
        if ra is None:
            q += sql.SQL( "INNER JOIN root_diaobject r ON o.rootid=r.id\n" )
        if objprocver is not None:
            q += sql.SQL( textwrap.dedent(
                """
                INNER JOIN base_procver_of_procver pv ON pv.base_procver_id=o.base_procver_id
                                                     AND pv.procver_id={objprocver}
                """ ) ).format( objprocver=str(objprocver) )
        if ra is not None:
            q += sql.SQL( textwrap.dedent(
                """
                INNER JOIN (
                  SELECT DISTINCT ON(p1.diaobjectid) p1.diaobjectid, p1.ra, p1.dec
                  FROM diaobject_position p1
                  INNER JOIN base_procver_of_procver p1pv ON p1.base_procver_id=p1pv.base_procver_id
                                                         AND p1pv.procver_id={pospv}
                  ORDER BY p1.diaobjectid, p1pv.priority DESC
                ) p ON p.diaobjectid=o.diaobjectid
                """ ) ).format( pospv=str(posprocver) )
        con.execute_nofetch( q )
        nexttable = "tmp_diaobject_with_position"

        # Filter by ra and dec if given
        if ra is not None:
            FDBLogger.debug( "Object search filtering by ra/dec" )
            radius = util.float_or_none_from_dict( kwargs, 'radius' )
            radius = radius if radius is not None else 10.
            q = sql.SQL( textwrap.dedent(
                """
                SELECT DISTINCT ON(o.rootid) o.rootid, o.ra, o.dec INTO TEMP TABLE objsearch_radeccut
                FROM {nexttable} o
                WHERE q3c_radial_query( o.ra, o.dec, %(ra)s, %(dec)s, %(rad)s )
                ORDER BY rootid
                """ ) ).format( nexttable=sql.Identifier( nexttable ) )
            if objprocver is not None:
                q += sql.SQL( ", o.priority DESC\n" )
            subdict = { 'ra': ra, 'dec': dec, 'rad': radius/3600. }
            con.execute_nofetch( q, subdict )
            # ****
            debug_count_temp_table( con, 'objsearch_radeccut' )
            # ****
            nexttable = 'objsearch_radeccut'

        # Count (and maybe filter) by number of detections within the time window
        # ROB TODO : use processing version index
        if window_t0 is not None:
            FDBLogger.debug( "Object search finding detections within window" )
            # # ROB -- whgy was thus necessary?
            # # Needs to be adapted fo runreliable diabjectid
            # if nexttable != 'diaobject':
            #     # Make a primary key so we can group by
            #     con.execute_nofetch( f"ALTER TABLE {nexttable} ADD PRIMARY KEY (diaobjectid)",
            #                          explain=False, analyze=False )
            subdict = { 'pv': procver, 't0': window_t0, 't1': window_t1 }
            q = sql.SQL( textwrap.dedent(
                """
                /*+ IndexScan(s idx_diasource_diaobjectid idx_diasource_mjd) */
                SELECT rootid, ra, dec, numdetinwindow
                INTO TEMP TABLE objsearch_windowdet
                FROM (
                  SELECT rootid,ra,dec,COUNT(visit) AS numdetinwindow
                  FROM (
                    SELECT DISTINCT ON (o.rootid,s.visit) o.rootid, o.ra, o.dec, s.visit
                    FROM {nexttable} o
                    INNER JOIN diaobject o2 ON o.rootid=o2.rootid
                    INNER JOIN diasource s ON s.diaobjectid=o2.diaobjectid
                    INNER JOIN base_procver_of_procver pv ON s.base_procver_id=pv.base_procver_id
                           AND pv.procver_id=%(pv)s
                    WHERE s.midpointmjdtai>=%(t0)s AND s.midpointmjdtai<=%(t1)s
                    ORDER BY o.rootid, s.visit, pv.priority DESC
                """ ) ).format( nexttable=sql.Identifier(nexttable) )
            if statbands is not None:
                q += sql.SQL( " AND s.band=ANY(%(bands)s)\n" )
                subdict['bands'] = statbands
            q += sql.SQL( textwrap.dedent(
                """
                  ) subsubq
                  GROUP BY rootid, ra, dec
                ) subq
                """ ) )
            _and = "WHERE"
            if min_window_numdetections is not None:
                q += sql.SQL( f"{_and} numdetinwindow>=%(minn)s" )
                subdict['minn'] = min_window_numdetections
                _and = "  AND"
            if max_window_numdetections is not None:
                q += sql.SQL( f"{_and} numdetinwindow<=%(maxn)s" )
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
            FDBLogger.debug( "Object search doing first rough cut on detection times" )
            subdict = { 'pv': procver }
            q = sql.SQL( textwrap.dedent(
                """
                /*+ IndexScan(s idx_diasource_diaobjectid idx_diasource_mjd) */
                SELECT * INTO TEMP TABLE objsearch_detcut FROM (
                  SELECT DISTINCT ON (o.rootid) o.rootid,o.ra,o.dec""" ) )
            for f in addlfields:
                q += sql.SQL( "," ) + sql.Identifier( 'o', f )
            q += sql.SQL( textwrap.dedent(
                """
                  FROM {nexttable} o
                  INNER JOIN diaobject o2 ON o.rootid=o2.rootid
                  INNER JOIN diasource s ON o2.diaobjectid=s.diaobjectid
                  INNER JOIN base_procver_of_procver pv ON s.base_procver_id=pv.base_procver_id
                    AND pv.procver_id=%(pv)s
                """ ) ).format( nexttable=sql.Identifier( nexttable ) )
            _and = "WHERE"
            if ( mint_firstdetection is not None ) or ( mint_lastdetection is not None ):
                q += sql.SQL( f"  {_and} s.midpointmjdtai>=%(mint)s" )
                subdict['mint'] = ( mint_firstdetection if mint_lastdetection is None
                                    else mint_lastdetection if mint_firstdetection is None
                                    else min( mint_firstdetection, mint_lastdetection ) )
                _and = "  AND"
            if ( maxt_firstdetection is not None ) or ( maxt_lastdetection is not None ):
                q += sql.SQL( f"  {_and} s.midpointmjdtai<=%(maxt)s\n" )
                subdict['maxt'] = ( maxt_firstdetection if maxt_lastdetection is None
                                    else maxt_lastdetection if maxt_firstdetection is None
                                    else max( maxt_firstdetection, maxt_lastdetection ) )
                _and = "  AND"
            if statbands is not None:
                q += sql.SQL( f"  {_and} s.band=ANY(%(bands)s)\n" )
                subdict['bands'] = statbands
            q += sql.SQL( "  ORDER BY o.rootid\n"           # , s.visit, pv.priority DESC\n"
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
        # Also, analyze the latest table on column rootid,
        #   so where the postgres query optimizer is still doing
        #   things it will work with reasonable data.  (It's possible
        #   that this is enough, and we don't need the hints,
        #   but this is a case where we are quite sure which join
        #   order is going to be best.)  (Hurm; I still felt the
        #   need to specify the join type sometimes....)

        subdict = { 'pv': procver }

        con.execute( f"ANALYZE {nexttable}(rootid)", explain=False )

        # First: build the table, put in first detection
        FDBLogger.debug( "Object search making stat tab with first detection" )
        q = sql.SQL( textwrap.dedent(
            """
            /*+ IndexScan(s idx_diasource_diaobjectid)
                Leading( ( (o s) pv ) ) */
            SELECT * INTO TEMP TABLE objsearch_stattab FROM (
              SELECT DISTINCT ON (rootid) rootid,ra,dec
            """ ) )
        for f in addlfields:
            q += sql.SQL(",") + sql.Identifier( f )
        q += sql.SQL( textwrap.dedent(
            """,
                     NULL::integer as numdet,
                     midpointmjdtai AS firstdetmjd, band AS firstdetband,
                     psfflux AS firstdetflux, psffluxerr AS firstdetfluxerr,
                     NULL::double precision as lastdetmjd, NULL::text as lastdetband,
                     NULL::double precision as lastdetflux, NULL::double precision as lastdetfluxerr,
                     NULL::double precision as maxdetmjd, NULL::text as maxdetband,
                     NULL::double precision as maxdetflux, NULL::double precision as maxdetfluxerr
              FROM (
                SELECT rootid, ra, dec, midpointmjdtai, band, psfflux, psffluxerr""" ) )
        for f in addlfields:
            q += sql.SQL( ", " ) + sql.Identifier( f )
        q += sql.SQL( textwrap.dedent(
            """
                FROM (
                  SELECT DISTINCT ON (o.rootid,s.visit) o.rootid, o.ra, o.dec,
                    s.midpointmjdtai, s.band, s.psfflux, s.psffluxerr""" ) )
        for f in addlfields:
            q += sql.SQL( ", " ) + sql.Identifier( 'o', f )
        q += sql.SQL( textwrap.dedent(
            """
                  FROM {nexttable} o
                  INNER JOIN diaobject o2 ON o.rootid=o2.rootid
                  INNER JOIN diasource s ON o2.diaobjectid=s.diaobjectid
                  INNER JOIN base_procver_of_procver pv ON s.base_procver_id=pv.base_procver_id
                                                       AND pv.procver_id=%(pv)s\
            """ ) ).format( nexttable=sql.Identifier(nexttable) )
        _and = "WHERE"
        if statbands is not None:
            subdict['bands'] = statbands
            q += sql.SQL( f"      {_and} s.band=ANY(%(bands)s)\n" )
            _and = "  AND"
        if mjd_now is not None:
            subdict['mjdnow'] = mjd_now
            q += sql.SQL( f"      {_and} s.midpointmjdtai<=%(mjdnow)s\n" )
            _and = "  AND"
        q += sql.SQL( textwrap.dedent(
            """
                  ORDER BY o.rootid, s.visit, pv.priority DESC
                ) subsubq
                ORDER BY rootid, midpointmjdtai
              ) subq
            ) outersubq
            """ ) )
        con.execute_nofetch( q, subdict )

        # Add in last detection
        FDBLogger.debug( "Object search adding last detection to stat tab" )
        q = sql.SQL( textwrap.dedent(
            """
            /*+ IndexScan(s idx_diasource_diaobjectid)
                Leading( ( (o s) pv ) ) */
            UPDATE objsearch_stattab ost
            SET lastdetmjd=midpointmjdtai, lastdetband=band,
                lastdetflux=psfflux, lastdetfluxerr=psffluxerr
            FROM (
              SELECT DISTINCT ON (rootid) rootid, midpointmjdtai, band, psfflux, psffluxerr
              FROM (
                SELECT DISTINCT ON (o.rootid, s.visit) o.rootid, s.midpointmjdtai,
                                                       s.band, s.psfflux, s.psffluxerr
                FROM {nexttable} o
                INNER JOIN diaobject o2 ON o.rootid=o2.rootid
                INNER JOIN diasource s ON o2.diaobjectid=s.diaobjectid
                INNER JOIN base_procver_of_procver pv ON s.base_procver_id=pv.base_procver_id
                  AND pv.procver_id=%(pv)s\n
            """ ) ).format( nexttable=sql.Identifier( nexttable ) )
        _and = "WHERE"
        if statbands is not None:
            q += sql.SQL( f"    {_and} s.band=ANY(%(bands)s)\n" )
            _and = "  AND"
        if mjd_now is not None:
            q += sql.SQL( f"    {_and} s.midpointmjdtai<=%(mjdnow)s\n" )
            _and = "  AND"
        q += sql.SQL( textwrap.dedent(
            """
                ORDER BY o.rootid, s.visit, pv.priority DESC
              ) subsubq
              ORDER BY rootid, midpointmjdtai DESC
            ) subq
            WHERE subq.rootid=ost.rootid
            """ ) )
        con.execute_nofetch( q, subdict )

        # Add in max detection
        FDBLogger.debug( "Object search adding max detection to stat tab" )
        q = sql.SQL( textwrap.dedent(
            """
            /*+ IndexScan(s idx_diasource_diaobjectid)
                Leading( ( ( o s ) pv ) ) */
            UPDATE objsearch_stattab ost
            SET maxdetmjd=midpointmjdtai, maxdetband=band,
                maxdetflux=psfflux, maxdetfluxerr=psffluxerr
            FROM (
              SELECT DISTINCT ON (rootid) rootid, midpointmjdtai, band, psfflux, psffluxerr
              FROM (
                SELECT DISTINCT ON (o.rootid, s.visit) o.rootid, s.midpointmjdtai,
                                                       s.band, s.psfflux, s.psffluxerr
                FROM {nexttable} o
                INNER JOIN diaobject o2 ON o.rootid=o2.rootid
                INNER JOIN diasource s ON o2.diaobjectid=s.diaobjectid
                INNER JOIN base_procver_of_procver pv ON s.base_procver_id=pv.base_procver_id
                  AND pv.procver_id=%(pv)s
            """ ) ).format( nexttable=sql.Identifier( nexttable ) )
        _and = "WHERE"
        if statbands is not None:
            q += sql.SQL( f"    {_and} s.band=ANY(%(bands)s)\n" )
            _and = "  AND"
        if mjd_now is not None:
            q += sql.SQL( f"    {_and} s.midpointmjdtai<=%(mjdnow)s\n" )
            _and = "  AND"
        q += sql.SQL( textwrap.dedent(
            """
                ORDER BY o.rootid, s.visit, pv.priority DESC
              ) subsubq
              ORDER BY rootid, psfflux DESC
            ) subq
            WHERE subq.rootid=ost.rootid
            """ ) )
        con.execute_nofetch( q, subdict )

        # Add in number of detections
        FDBLogger.debug( "Object search adding detection count to stat tab" )
        q = sql.SQL( textwrap.dedent(
            """
            /*+ IndexScan(s idx_diasource_diaobjectid)
                Leading( ( (o s) pv ) ) */
            UPDATE objsearch_stattab o
            SET numdet=n
            FROM (
              SELECT rootid, COUNT(visit) AS n
              FROM (
                SELECT DISTINCT ON (o.rootid, s.visit) o.rootid, s.visit
                FROM {nexttable} o
                INNER JOIN diaobject o2 ON o.rootid=o2.rootid
                INNER JOIN diasource s ON s.diaobjectid=o2.diaobjectid
                INNER JOIN base_procver_of_procver pv ON s.base_procver_id=pv.base_procver_id
                  AND pv.procver_id=%(pv)s
            """ ) ).format( nexttable=sql.Identifier( nexttable ) )
        _and = "WHERE"
        if statbands is not None:
            q += sql.SQL( f"    {_and} s.band=ANY(%(bands)s)\n" )
            _and = "  AND"
        if mjd_now is not None:
            q += sql.SQL( f"    {_and} s.midpointmjdtai<=%(mjdnow)s\n" )
            _and = "  AND"
        q += sql.SQL( textwrap.dedent(
            """
                ORDER BY o.rootid, s.visit, pv.priority DESC
              ) subsubq
              GROUP BY rootid
            ) subq
            WHERE subq.rootid=o.rootid
            """ ) )
        con.execute_nofetch( q, subdict )
        # ****
        debug_count_temp_table( con, 'objsearch_stattab' )
        # ****

        # Delete from this table based on numdet and detection time as appropriate
        FDBLogger.debug( "Object search applying cuts" )
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

        if ( just_objids or noforced ) and ( min_lastmag is None ) and ( max_lastmag is None ):
            FDBLogger.debug( "Object search pulling down results" )
            # No need to search the forced source table, and that can be slow because the
            #  forced photometry table is huge, so just skip it.
            if just_objids:
                rows, columns = con.execute( "SELECT rootid FROM objsearch_stattab" )
            else:
                rows, columns = con.execute( "SELECT * FROM objsearch_stattab" )

        else:
            FDBLogger.debug( "Object search adding last forced photometry to stat tab" )
            # In this else block, we need to get the latest forced photometry, so do that.
            nexttable = 'objsearch_stattab'

            # Because the diaforcedsource table is going to be the hugest one,
            #   create an index rootid of {nexttable} here to help
            #   this next query along.  We hope.
            con.execute( f"CREATE INDEX idx_t_rootid ON {nexttable}(rootid)", explain=False )

            # Reanalyze this table to help postgres do the right thing
            con.execute( f"ANALYZE {nexttable}(rootid)", explain=False )

            # Get the last forced source
            # NOTE: I had a HashJoin( f t ) in here that I removed
            q = sql.SQL( textwrap.dedent(
                """
                /*+ IndexScan(f idx_diaforcedsource_diaobjectid )
                    Leading( ( (f t) pv ) )
                    Parallel( f 3 hard )
                */
                SELECT * INTO TEMP TABLE objsearch_final FROM (
                  SELECT DISTINCT ON (rootid) *
                  FROM (
                    SELECT DISTINCT ON (t.rootid, f.visit) t.*,
                        f.psfflux AS lastforcedflux, f.psffluxerr AS lastforcedfluxerr,
                        f.midpointmjdtai AS lastforcedmjd, f.band AS lastforcedband
                    FROM {nexttable} t
                    INNER JOIN diaobject o ON t.rootid=o.rootid
                    INNER JOIN diaforcedsource f ON f.diaobjectid=o.diaobjectid
                    INNER JOIN base_procver_of_procver pv ON f.base_procver_id=pv.base_procver_id
                                                         AND pv.procver_id=%(pv)s
                """ ) ).format( nexttable=sql.Identifier( nexttable ) )
            _and = "WHERE"
            if statbands is not None:
                q += sql.SQL( f"    {_and} f.band=ANY(%(bands)s)\n" )
                _and = "  AND"
            if mjd_now is not None:
                q += sql.SQL( f"    {_and} f.midpointmjdtai<=%(mjdnow)s\n" )
                _and = "  AND"
            q += sql.SQL( textwrap.dedent(
                """
                    ORDER BY t.rootid, f.visit, pv.priority DESC
                  ) subsubq
                  ORDER BY rootid, lastforcedmjd DESC
                ) subq
                """ ) )
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
            FDBLogger.debug( "Object search pulling down results" )
            if just_objids:
                rows, columns = con.execute( "SELECT rootid FROM objsearch_final" )
            else:
                rows, columns = con.execute( "SELECT * FROM objsearch_final" )

    columnmap = { columns[i]: i for i in range(len(columns)) }
    FDBLogger.debug( f"object_search returning {len(rows)} objects in format {return_format}" )

    if return_format == 'json':
        rval = { c: [ r[columnmap[c]] for r in rows ] for c in columns }
        if ( not just_objids ) and ( 'numdetinwindow' not in rval ):
            rval['numdetinwindow'] = [ None for r in rows ]
        # FDBLogger.debug( f"returning json\n{json.dumps(rval,indent=4)}" )
        return rval

    elif return_format == 'pandas':
        df = pandas.DataFrame( rows, columns=columns )
        if ( not just_objids ) and ( 'numdetinwindow' not in df.columns ):
            df['numdetinwindow'] = None
        # FDBLogger.debug( f"object_search pandas dataframe: {df}" )
        return df

    else:
        raise RuntimeError( "This should never happen." )


def get_hot_ltcvs( processing_version, object_processing_version=None, position_processing_version=None,
                   include_object_positions=True, include_source_positions=False,
                   use_weighted_source_positions=False, always_use_weighted_source_positions=False,
                   detected_since_mjd=None, detected_in_last_days=None,
                   mjd_now=None, source_patch=True, dbcon=None ):
    """Get lightcurves of objects with a recent detection.

    Parameters
    ----------
      processing_version: string
        The description of the processing version, or processing version
        alias, to use for searching diasource and diaforcedsource tables.

      object_processing_version: string, default None
        The description of the processing version, or processing version
        alias, to use for searching for diaobjects.  If None, will be
        the same as processing_version.

      position_processing_version: string, default None
        Ignored if always_use_weighted_source_positions is True or if
        include_object_positions=False.  The processing version for
        getting object positions.  If not given, will use
        object_processing_version.  If the position from the desired
        processing version isn't found, then the position fields in the
        returned object info dataframe will be null, unless
        use_weighted_source_positions is true, in which case

      use_weighted_source_positions: bool, default False
        Normally, if a position is not found with the desired position
        processing version, the ra and dec fields are NULL or NA or
        something.  Set this to True to fill them in with a (S/N)^2
        weighted average of the positions from the detections.  NOTE: if
        there aren't any sources with S/N>3, then you won't get an
        a weighted source position for that object.

      always_use_weighted_source_positions: bool, default False
        Don't bother searching for object positions, just use weighted
        source positions.  Implies use_weighted_source_positions, and
        implies include_object_positions=False.

      include_object_positions: bool, default True
        Include positions from the diaobject_position table.

      include_source_positions: bool, default False
        Include positions from the diasource table.

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

      source_patch : bool, default True
        If False, returned light curves only return fluxes from the
        diaforcedsource table, which for a data release is probably what
        you want.  However, during the campaign, there will be sources
        detected for which there is no forced photometry.  (Alerts are
        sent as sources are detected, but forced photometry is delayed.)
        Set this to True to get more complete, but heterogeneous,
        lightcurves.  When this is True, it will look for all detections
        that don't have a corresponding forced photometry point
        (i.e. observation of the same ojbject in the same visit), and
        add the detections to the lightcurve.  Be aware that these
        photometry points don't mean exactly the same thing, as forced
        photometry is all at one position, but detections are where they
        are.  This is useful for doing real-time filtering and the like,
        but *not* for any kind of precision photometry or lightcurve
        fitting.  (In fact, all of the data from alerts is probably not
        good for precision cosmology or lightcurve fitting.)

      dbcon: psycopg.Connection, db.DBCon, or None
         Database connection to use.  If None, will make a new
         connection and close it when done.

      Returns
      -------
        ltcvdf, objinfo, hostdf

        ltcvdf: pandas.DataFrame
           A dataframe with lightcurves. It is sorted and indexed by
           rootid (uuid) and mjd (float), and has columns:

             diasourceid -- the diaSourceId for this source
             diaobjectid -- the diaObjectId that FASTDB thinks is associated with this source (*see below)
             visit -- the visit number
             mjd -- the MJD of the obervation
             band -- the filter (u, g, r, i, z, or Y)
             flux -- the PSF flux in nJy
             fluxerr -- uncertaintly on psfflux in nJy
             istdet -- bool, True if this was detected (i.e. has an associated source)
             ispatch -- bool, True if this data is from a source rather than a forced source
                        (only included if source_patch is True)
             det_ra, det_dec, det_raerr, det_decerr, det_ra_dec_cov -- float
                  The positions from the diaSource.  These will only be included if
                  include_source_positions is True.  They will all be null if
                  source_patch is False.  They will be null for points that
                  have forced photometry but no detection photometry in the database.


        objinfo: pandas.DataFrame
           Information about the objects.  Sorted and indexed by
           diaobjectid.  Will have a colum rootid (uuid) with the rootid
           of the object.  If either include_objecT_positions or
           use_weighted_source_positions is True, will also have give
           additional columns, ra, dec, raerr, decerr, ra_dec_cov.

           Normally, the position columns come from the
           diaobject_position table.  Exactly what these positions mean
           for objects ingested from alerts is not completely clear.
           The columns will be null for objects that don't have an entry
           with the right processing version in diaobject_positions.  If
           you specify use_weighted_source_positions, then were no
           diaboject_position was available, a weighted (by (S/N)²)
           average of source positions for all sources *with the same
           rootid* will be in these fields where there was no
           diaobject_position.  If you specify
           always_use_weighted_source_positions, then the position
           fields will *only* have positions from weighted source
           positions.  (They can still be null if there are no sources
           with S/N>3, or if there are sources in the database for which
           the ra/dec columns weren't filled.)

           WARNING: this may well have a different number of rows than
           ltcvdf, because there may be multiple diaObjectIds in a given
           processing_version associated with a single rootid.  (It
           should never have fewer rows tan ltcvdf.)

        hostdf: None
           NOT CURRENTLY SUPPORTED.

        *A note on diaobjectid of sources : LSST may put more than one
         diaObjectId at the same point in the sky.  What's more,
         empirically, in different alerts, the same diaSource may be
         associated with a different diaObjectId (in particular, in a
         later alert, the diaObjectId of a given diaSource in the
         prvDiaSources array may be different from what the diaObjectId
         of that source was when it got its own alert).  FASTDB does not
         track all of the diaObjectIds that have been associated with a
         given diaSource, but just the first one that it learned about.
         For this reason, it's better when possible to use rootid (which
         attempts to deuplicate).

    """

    mjd0 = None

    use_weighted_source_positions = use_weighted_source_positions or always_use_weighted_source_positions
    include_object_positions = include_object_positions and ( not always_use_weighted_source_positions )

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

    with db.DBCon( dbcon ) as con:
        procver = util.procver_id( processing_version, dbcon=con.con )
        if object_processing_version is None:
            objprocver = procver
        else:
            objprocver = util.procver_id( object_processing_version, dbcon=con.con )

        if always_use_weighted_source_positions:
            posprocver = None
        elif position_processing_version is None:
            posprocver = objprocver
        else:
            posprocver = util.procver_id( position_processing_version, dbcon=con.con )

        # First : get a table of all root object ids that have a
        #   detection (i.e. a diasource) in the desired time period.

        q = ( "/*+ IndexScan(s idx_diasource_mjd) */\n"
              "SELECT DISTINCT ON(o.rootid) o.rootid\n"
              "INTO TEMP TABLE tmp_objids\n"
              "FROM diasource s\n"
              "INNER JOIN diaobject o ON s.diaobjectid=o.diaobjectid\n"
              "INNER JOIN base_procver_of_procver pv ON s.base_procver_id=pv.base_procver_id\n"
              "                                     AND pv.procver_id=%(procver)s\n"
              "WHERE s.midpointmjdtai>=%(t0)s\n" )
        if mjd_now is not None:
            q += "  AND s.midpointmjdtai<=%(t1)s\n"
        # ...any reason to order?
        # q += "ORDER BY o.rootid\n"
        con.execute_nofetch( q, { 'procver': procver, 't0': mjd0, 't1': mjd_now } )

        # Second: pull out the object info for these objects
        columns = [ 'diaobjectid', 'rootid' ]
        if include_object_positions:
            columns.extend( [ 'ra', 'dec', 'raerr', 'decerr', 'ra_dec_cov' ] )
        objdf = get_object_infos( objids_table='tmp_objids', return_format='pandas', columns=columns, dbcon=con,
                                  processing_version=objprocver, position_processing_version=posprocver )

        # Third: get the host stuff -- hosts aren't currently implemented, and probably won't be
        #   until a few years from now when DR1 is out
        hostdf = None

        # Fourth: get the lightcurves
        df = many_object_ltcvs( processing_version=procver, objids_table='tmp_objids',
                                return_format='pandas', mjd_now=mjd_now, dbcon=con,
                                which='patch' if source_patch else 'forced',
                                include_source_positions=use_weighted_source_positions )
        if ( ( always_use_weighted_source_positions ) or
             ( use_weighted_source_positions and ( not include_object_positions ) )
            ):
            # Zero out all columns if we're always using weighted source positions,
            #   or if we're sometimes using weighted source positions but didn't
            #   ask for the position columns from get_object_infos
            objdf.pos_base_procver_id = None
            objdf.ra = None
            objdf.dec = None
            objdf.raerr = None
            objdf.decerr = None
            objdf.ra_dec_cov = None
        if use_weighted_source_positions:
            raise RuntimeError( "OMG ROB YOU ARE IN THE MIDDLE OF EDITING CODE" )

        return df, objdf, hostdf
