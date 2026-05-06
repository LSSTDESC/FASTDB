__all__ = [ "object_ltcv", "object_search", "get_hot_ltcvs" ]

import datetime
import time
import io
import numbers
import textwrap
import random
import json   # noqa: F401

from psycopg import sql
import numpy as np
import pandas
import astropy.time

import db
import util
from util import FDBLogger, laboriously_construct_pandas


def _is_objids_table_rootid( objids_table, dbcon ):
    q = sql.SQL( "SELECT column_name FROM information_schema.columns "
                 "WHERE table_name={table_name}" ).format( table_name=objids_table )
    rows, _cols = dbcon.execute( q )
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

    return obj_is_root


def get_object_infos( objids=None, objids_table=None, processing_version=None, position_processing_version=None,
                      base_procvers=None, columns=None, return_format='json', dbcon=None ):
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
        columns will all be null.  (Note: if you pass base_procvers and
        also ask for any position columns (which is the default), then
        this is required.)

      base_procvers : list of uuid or None
        You usually do not want to specify this.  They're used
        internally by other functions in this module.  If you find that
        you really need it (e.g. if you are Rob reading this six months
        later and wondering what the heck you were thinking six months
        ago), bug Rob to document them.

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

        NOTE THAT it's possible you will get back more diaobjectids than
        you asked for if you specified a list of diaobjectids!  This is
        because sometimes there is more than one diaobjectid in the same
        processing version with the same rootid.  (It's also possible
        that you will get back fewer if you didn't give the right object
        processing version, or if the diaobjectids don't exist.)

        Columns included come from the diaobject and diaboject_position tables:

           diaobjectid         | bigint           | Globally unique (across all proc vers) diaobject id [Index]
           rootid              | uuid             | root_diaobject id for this object; this is true object identifier
           obj_base_procver    | uuid             | base processing version for the diaobject
           pos_base_procver    | uuid             | base processing version for the diaobject_position
           ra                  | double precision | ra
           dec                 | double precision | dec
           raerr               | real             | uncertainty (NOT variance) on ra
           decerr              | real             | uncertainty (NOT variance) on dec
           ra_dec_cov          | real             | covariance between ra and dec

    """

    if return_format not in ( 'pandas', 'json' ):
        raise ValueError( f"return_format must be pandas or json, not {return_format}" )

    if objids_table is not None:
        if dbcon is None:
            raise ValueError( "objids_table requires dbcon" )
        if objids is not None:
            raise ValueError( "objids_table and objids cannot be used together" )
        obj_is_root = _is_objids_table_rootid( objids_table, dbcon )
    elif objids is None:
        raise ValueError( "must pass either objids or objids_table" )
    else:
        if not util.isSequence( objids ):
            objids = [ objids ]
        if all( isinstance( o, numbers.Integral ) for o in objids ):
            # Make sure they're int, because if it's something like np.int64, postgres may choke
            #  (...really??)
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

    objpvid = None
    if base_procvers is not None:
        if not util.isSequence( base_procvers ):
            raise TypeError( "base_procvers must be a list of uuids" )
        base_procvers = [ util.asUUID(v) for v in base_procvers ]
        if processing_version is not None:
            FDBLogger.warning( "Both processing_version and base_procvers given, ignoring processing_version" )
    else:
        objpvid = db.ProcessingVersion.procver_id( processing_version
                                                   if processing_version is not None
                                                   else "default" )

    pospvid = ( objpvid if position_processing_version is None
                else db.ProcessingVersion.procver_id( position_processing_version ) )

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

    if gotsomepos and ( pospvid is None ):
        raise ValueError( "Must supply a position processing_version with base_procvers" )

    if ( not gotsomepos ) and ( position_processing_version is not None ):
        FDBLogger.warning( "Didn't ask for positon columns, but provided position processing version; "
                           "ignoring the position processing version." )

    with db.DBCon( dbcon ) as dbcon:
        if obj_is_root:
            q = sql.SQL( "/*+ IndexScan(o idx_diaobject_rootid)\n" )
        else:
            q = sql.SQL( "/*+ IndexScan(o idx_diaobject_diaobjectid)\n" )

        if gotsomepos:
            q += sql.SQL( "    IndexScan(p1 idx_position_diaobjectid)\n" )

        q +=  sql.SQL( textwrap.dedent(
            """\
            */
            SELECT DISTINCT ON(o.diaobjectid) {sqlcolumns}
            FROM diaobject o
            INNER JOIN base_processing_version b ON b.id=o.base_procver_id
            """ ) ).format( sqlcolumns=sqlcolumns )
        if base_procvers is None:
            q += sql.SQL( textwrap.dedent(
                """\
                INNER JOIN base_procver_of_procver pv ON b.id=pv.base_procver_id
                                                     AND pv.procver_id={objpvid}
                """ ) ).format( objpvid=objpvid )
        else:
            q += sql.SQL( "                          AND b.id=ANY({base_procvers})\n"
                         ).format( base_procvers=base_procvers )

        if gotsomepos:
            q += sql.SQL( textwrap.dedent(
                """\
                LEFT JOIN (
                  SELECT DISTINCT ON(p1.diaobjectid) p1.*, b1.description
                  FROM diaobject o1
                  INNER JOIN diaobject_position p1 ON o1.diaobjectid=p1.diaobjectid
                  INNER JOIN base_processing_version b1 ON p1.base_procver_id=b1.id
                  INNER JOIN base_procver_of_procver pv1 ON b1.id=pv1.base_procver_id
                                                        AND pv1.procver_id={pospvid}
                  ORDER BY p1.diaobjectid, pv1.priority DESC
                ) p ON o.diaobjectid=p.diaobjectid
                """ ) ).format( pospvid=pospvid )

        if objids_table is not None:
            q += sql.SQL( textwrap.dedent(
                """\
                INNER JOIN {objids_table} t ON {ojoin}={tjoin}
                """ ) ).format( objids_table=sql.Identifier(objids_table),
                                ojoin=sql.Identifier( 'o', joincolumn ),
                                tjoin=sql.Identifier( 't', joincolumn ) )
        else:
            q += sql.SQL( textwrap.dedent(
                """\
                WHERE {ojoin}=ANY({objids})
                """ ) ).format( ojoin=sql.Identifier( 'o', joincolumn ), objids=objids )

        q += sql.SQL( "ORDER BY o.diaobjectid\n" )

        # ****
        # TEMP DEBUGGING, TAKE THIS OUT
        # dbcon.echoqueries = True
        # ****
        rows, cols = dbcon.execute( q )
        # Next line deals with what I think is a dysfunctional psycopg return
        cols = columns if len(rows) == 0 else cols
        if return_format == 'pandas':
            FDBLogger.debug( "Constructing pandas dataframe..." )
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
            FDBLogger.debug( "Extracting postgres return to dictionary" )
            return { c: [ r[i] for r in rows ] for i, c in enumerate( cols ) }
        else:
            raise RuntimeError( "This should never happen" )

        FDBLogger.debug( "get_object_infos done." )


def many_object_ltcvs( processing_version='default', objids=None, objids_table=None, return_format='json',
                       bands=None, which='patch', include_base_procver=False, include_obj_base_procver_id=False,
                       include_source_positions=False,
                       use_weighted_source_positions=False, always_use_weighted_source_positions=False,
                       return_object_info=False, include_object_positions=False, position_processing_version=None,
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

      objids_table : str, default None
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

      include_base_procver_id : bool, default False
         Two more columns.

      include_obj_base_procver_id : bool, default False
         Probably don't use this.  (It's used internally by
         many_object_ltcvs.  Only use this if you really know what
         you're doing, and Rob isn't even sure that he does.)

      include_source_positions : bool, default False
         If True, there will be additional columns (ra, dec, raerr,
         decerr, ra_dec_cov) that have the positions that were found for
         the sources (the detections).

      return_format : str, default 'json'
         'json' or 'pandas'

      return_object_info : bool, default False
         If True, you get a second return.  See Returns below

      include_object_positions: bool, default False
         Irrelevant if return_object_info is False or if
         alwyas_use_weighted_source positions is True.  Otherwise, try
         to get object positions from the diaobject_position table.

      position_processing_version : str or uuid, default None
         The processing version for getting object position info.  Not
         used if return_object_info is False, or if
         always_use_weighted_source_positions is True.  Defaults to the
         same as processing_version.  WARNING: just... worry.  If
         processing versions get complicated, this gets hard.

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

            diasourceid : bigint, the diaSoruceId, or null
            [ diafordedsourceid : bigint or null; only included if which isn't 'detections' ]
            source_diaobjectid : bigint or None, the diaObjectId associated with this diasource
            [ forced_diaobjectid : bigint or None, the diaObjectId associated with this forcedsource ]
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
    FDBLogger.debug( "Starting many_object_ltcvs..." )

    # Parse objids, set objfield
    if objids_table is not None:
        if dbcon is None:
            raise ValueError( "objids_table requires dbcon" )
        if objids is not None:
            raise ValueError( "objids_table and objids cannot be used together" )
        objids_are_root = _is_objids_table_rootid( objids_table, dbcon )
    else:
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
    if use_weighted_source_positions and ( ( not return_object_info ) or ( not include_object_positions ) ):
        FDBLogger.warning( "Asked for weighted source positions, but you didn't ask for object positions."
                           "Ignoring weighted source positions." )
        use_weighted_source_positions = False
    must_get_source_positions = ( include_source_positions or use_weighted_source_positions or
                                  ( return_object_info and include_object_positions ) )

    with db.DBCon( dbcon ) as dbcon:
        tmpsmade = []
        try:
            pvid = db.ProcessingVersion.procver_id( processing_version, dbcon=dbcon )
            pospvid = None
            if return_object_info and include_object_positions:
                pospvid = ( db.ProcessingVersion.procver_id( position_processing_version, dbcon=dbcon )
                            if position_processing_version is not None else pvid )

            # Make a first pass and extract ALL diaobjectids from all base
            #   processing versions that share the same roots as the
            #   requested objects. Even *within* a base processing version
            #   there are multiple diaOjbects in the lsst alert stream, and
            #   what's more, the same diaSource will at different time
            #   (original alert, previous soruces in later alerts) be
            #   associated with different diaObjects.
            # However, also, we can't really be sure the actual processing
            #   versions of objects for the diasources in the processing
            #   version the user asked for, so just yank them all, and then
            #   trust the join to the source table to filter out the
            #   irrelevant ones.
            if objids is not None:
                objids_table = 'tmp_objids'
                tmpsmade.append( objids_table )
                if objids_are_root:
                    q = sql.SQL( textwrap.dedent(
                        """\
                        SELECT diaobjectid, rootid
                        INTO TEMP TABLE tmp_objids
                        FROM diaobject
                        WHERE rootid=ANY(%(roots)s)
                        """
                    ) ).format()
                    FDBLogger.debug( "...inserting objects from passed root ids into tmp_objids table" )
                    dbcon.execute_nofetch( q, {'roots': objids} )
                else:
                    q = sql.SQL( "CREATE TEMP TABLE temp_input_diaobject( diaobjectid bigint )" )
                    dbcon.execute( q, explain=False )
                    q = sql.SQL( "COPY temp_input_diaobject(diaobjectid) FROM STDIN"
                                ).format( objids_table=sql.Identifier( objids_table ) )
                    with dbcon.cursor.copy( q ) as copier:
                        for objid in objids:
                            copier.write_row( [ objid ] )

                    q = sql.SQL( textwrap.dedent(
                        """\
                        SELECT o.diaobjectid, o.rootid
                        INTO TEMP TABLE tmp_objids
                        FROM temp_input_diaobject t
                        INNER JOIN diaobject ot ON t.diaobjectid=ot.diaobjectid
                        INNER JOIN diaobject o ON ot.rootid=o.rootid
                        """ ) )
                    FDBLogger.debug( "...inserting objects from passed diaobjectid into tmp_objids table" )
                    dbcon.execute( q )
            else:
                actual_objids_table = f'{objids_table}_withboth'
                tmpsmade.append( actual_objids_table )
                dbcon.execute( sql.SQL( "DROP TABLE IF EXISTS {t}" ).format( t=sql.Identifier(actual_objids_table) ),
                               explain=False )
                if objids_are_root:
                    q = sql.SQL( textwrap.dedent(
                        """\
                        SELECT o.diaobjectid, o.rootid
                        INTO TEMP TABLE {desttable}
                        FROM {sourcetable} x
                        INNER JOIN diaobject o ON x.rootid=o.rootid
                        """ ) ).format( desttable=sql.Identifier( actual_objids_table ),
                                        sourcetable=sql.Identifier( objids_table ) )
                    FDBLogger.debug( f"...inserting objects from passed root id table to {actual_objids_table}" )
                    dbcon.execute( q )
                else:
                    q = sql.SQL( textwrap.dedent(
                        """\
                        SELECT o.diaobjectid, o.rootid
                        INTO TEMP TABLE {desttable}
                        FROM {sourcetable} x
                        INNER JOIN diaobject ot ON t.diaobjectid=x.diaobjectid
                        INNER JOIN diaobject o ON t.rootid=o.rootid
                        """ ) ).format( desttable=sql.Identifier(actual_objids_table),
                                        sourcetable=sql.Identifier(objids_table) )
                    FDBLogger.debug( f"...inserting objects from passed diaobjectid table to {actual_objids_table}" )
                    dbcon.execute( q )
                objids_table = actual_objids_table

            # Extract detections
            pos_fields = sql.SQL( "ra AS det_ra, dec AS det_dec, raerr AS det_raerr, "
                                  "decerr AS det_decerr, ra_dec_cov AS det_ra_dec_cov, "
                                  if must_get_source_positions
                                  else "" )
            procver_fields = sql.SQL( "p.description AS base_procver_s, " if include_base_procver else "" )
            dbcon.execute( "DROP TABLE IF EXISTS tmp_sources", explain=False )
            tmpsmade.append( 'tmp_sources' )
            q = sql.SQL( textwrap.dedent(
                """\
                /*+ IndexScan(s idx_diasource_diaobjectid)
                    IndexScan(ot idx_diaobject_rootid)
                */
                SELECT DISTINCT ON (t.rootid, s.visit)
                  t.rootid, s.diasourceid, s.diaobjectid AS source_diaobjectid, s.visit, s.midpointmjdtai AS mjd,
                  s.band, s.psfflux AS flux, s.psffluxerr AS fluxerr, o.base_procver_id AS source_obj_bpv,
                  {pos_fields} {procver_fields} TRUE as isdet
                INTO tmp_sources
                FROM {objids_table} t
                INNER JOIN diaobject ot ON t.rootid=ot.rootid
                INNER JOIN diasource s ON s.diaobjectid=ot.diaobjectid
                INNER JOIN base_procver_of_procver pv ON s.base_procver_id=pv.base_procver_id
                                                     AND pv._table='diasource'
                                                     AND pv.procver_id={procver}
                INNER JOIN diaobject o ON s.diaobjectid=o.diaobjectid
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
            FDBLogger.debug( "...querying for detections" )
            dbcon.execute_nofetch( q, { 'bands': bands } )

            if which == 'detections':
                q = sql.SQL( "SELECT * FROM tmp_sources ORDER BY rootid, mjd" )

            else:
                # Extract forced photometry if necessary
                procver_fields = sql.SQL( "p.description as base_procver_f, " if include_base_procver else "" )
                dbcon.execute( "DROP TABLE IF EXISTS tmp_forced", explain=False )
                tmpsmade.append( 'tmp_forced' )
                q = sql.SQL( textwrap.dedent(
                    """\
                    /*+ IndexScan(s idx_diaforcedsource_diaobjectid)
                        IndexScan(ot idx_diaobject_rootid)
                    */
                    SELECT DISTINCT ON (t.rootid, s.visit)
                      t.rootid, s.diaforcedsourceid, s.diaobjectid AS forced_diaobjectid,
                      s.visit, s.midpointmjdtai AS mjd,
                      s.band, s.psfflux AS flux, s.psffluxerr AS fluxerr, o.base_procver_id AS forced_obj_bpv,
                      {procver_fields} FALSE as isdet
                    INTO tmp_forced
                    FROM {objids_table} t
                    INNER JOIN diaobject ot ON t.rootid=ot.rootid
                    INNER JOIN diaforcedsource s ON s.diaobjectid=ot.diaobjectid
                    INNER JOIN base_procver_of_procver pv ON s.base_procver_id=pv.base_procver_id
                                                         AND pv._table='diaforcedsource'
                                                         AND pv.procver_id={procver}
                    INNER JOIN diaobject o ON s.diaobjectid=o.diaobjectid
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
                FDBLogger.debug( "...querying for forced photometry" )
                dbcon.execute_nofetch( q, { 'bands': bands } )

                # Join detections to forced photometry to set the 'isdet' and 'ispatch' flags.
                # The term FULL OUTER JOIN is extremely scary, of course
                pos_fields = sql.SQL( "s.det_ra, s.det_dec, s.det_raerr, s.det_decerr, s.det_ra_dec_cov, "
                                      if must_get_source_positions
                                      else "" )
                procver_fields = sql.SQL( "f.base_procver_f, s.base_procver_s, " if include_base_procver else "" )
                q = sql.SQL( textwrap.dedent(
                    """\
                    SELECT CASE WHEN f.rootid IS NULL THEN s.rootid ELSE f.rootid END AS rootid,
                           f.diaforcedsourceid,
                           s.diasourceid,
                           f.forced_diaobjectid,
                           s.source_diaobjectid,
                           f.forced_obj_bpv,
                           s.source_obj_bpv,
                           {procver_fields}
                           CASE WHEN f.rootid IS NULL THEN s.visit ELSE f.visit END AS visit,
                           CASE WHEN f.rootid IS NULL THEN s.mjd ELSE f.mjd END AS mjd,
                           CASE WHEN f.rootid IS NULL THEN s.band ELSE f.band END AS band,
                           CASE WHEN f.rootid IS NULL THEN s.flux ELSE f.flux END AS flux,
                           CASE WHEN f.rootid IS NULL THEN s.fluxerr ELSE f.fluxerr END AS fluxerr,
                           {pos_fields}
                           CASE WHEN s.rootid IS NULL THEN FALSE ELSE TRUE END AS isdet,
                           CASE WHEN f.rootid IS NULL THEN TRUE ELSE FALSE END as ispatch
                    FROM tmp_forced f
                    FULL OUTER JOIN tmp_sources s ON f.rootid=s.rootid AND s.visit=f.visit
                    ORDER BY rootid, mjd
                    """ ) ).format( pos_fields=pos_fields, procver_fields=procver_fields )

            FDBLogger.debug( "...extracting results from postgres" )
            FDBLogger.debug( "...executing query" )
            barf = "".join( random.choices( "abcdefghijklmnopqrstuvwxyz", k=6 ) )
            cursor = dbcon.execute_nofetch( q, echo=True, cursorname=f'many_object_ltcvs_{barf}' )
            cursor.itersize = 1000
            FDBLogger.debug( "...fetching results from postgres" )
            cols = [ desc[0] for desc in cursor.description ]
            coldex = { c: i for i, c in enumerate(cols) }

            ltcvs = []
            currootid = None
            rowcache = []
            allobjbpvs = set()

            def _extract_rowcache():
                nonlocal ltcvs, rowcache, currootid

                if len(rowcache) > 0:
                    # Make some of the columns numpy arrays if we're using weighted
                    #   source positions, so that (hopefully) processing will be
                    #   faster later.
                    if use_weighted_source_positions:
                        tmp = { c: ( np.array( [ r[coldex[c]] for r in rowcache ], dtype=np.float64 )
                                     if c in [ 'flux', 'fluxerr', 'det_ra', 'det_dec' ]
                                     else [ r[coldex[c]] for r in rowcache ] )
                                for c in cols }
                    else:
                        tmp  = { c: [ r[coldex[c]] for r in rowcache ] for c in cols }
                    tmp['rootid'] = currootid
                    ltcvs.append( tmp )
                rowcache = []
                currootid = row[ coldex['rootid'] ]

            n = 0
            for row in cursor:
                if ( n % 50000 == 0 ) and ( n > 0 ):
                    FDBLogger.debug( f"...{n} rows, {len(ltcvs)} ltcvs so far" )
                n += 1
                if row[ coldex['source_obj_bpv'] ] is not None:
                    allobjbpvs.add( row[ coldex['source_obj_bpv'] ] )
                if ( which != 'detections' ) and ( row[ coldex['forced_obj_bpv'] ] is not None ):
                    allobjbpvs.add( row[ coldex['forced_obj_bpv'] ] )
                if row[ coldex['rootid'] ] != currootid:
                    _extract_rowcache()
                rowcache.append( row )
            if len(rowcache) > 0:
                # I wish python had inline functions
                _extract_rowcache()

            cursor.close()
            FDBLogger.debug( f"...done fetching {n} rows, {len(ltcvs)} lightcurves." )

            # We might also need to get object info.  Get all diaobjects
            #    that match the rootids the caller asked for, from any base
            #    processing version from any diaobjectid from any source or
            #    forced source that we found.
            if return_object_info:
                bpvs = list( allobjbpvs )
                columns = [ 'diaobjectid', 'rootid' ]
                if include_base_procver:
                    columns.append( 'obj_base_procver' )
                if include_object_positions:
                    columns.extend( [ 'ra', 'dec', 'raerr', 'decerr', 'ra_dec_cov' ] )
                    if include_base_procver:
                        columns.append( 'pos_base_procver' )

                objinfo = get_object_infos( objids_table=objids_table, base_procvers=bpvs,
                                            position_processing_version=pospvid, columns=columns,
                                            return_format=return_format, dbcon=dbcon )

        except Exception:
            dbcon.rollback()
            raise
        finally:
            # Drop any temp tables we created.  Do NOT commit, however.  Reason:
            #   If this is called with an existing dbcon, then the caller
            #   may be impolicitly assuming these temp tables don't exist.
            #   However, the caller might also (perversely?) be in the middle
            #   of a transaction, and we don't want to end that transaction
            #   by committing.
            for tab in tmpsmade:
                dbcon.execute( sql.SQL( "DROP TABLE IF EXISTS {tab}" ).format( tab=sql.Identifier(tab) ),
                               explain=False )


    # Update object positions if necessary
    if use_weighted_source_positions:
        FDBLogger.debug( "Calculating weighted source positions and updating objinfo..." )
        if always_use_weighted_source_positions:
            # Null out any given positions so that we will always reset them
            if return_format == 'pandas':
                if include_base_procver:
                    objinfo.loc[ :, 'pos_base_procver' ] = None
                objinfo.loc[ :, 'ra' ] = None
                objinfo.loc[ :, 'dec' ] = None
                objinfo.loc[ :, 'raerr' ] = None
                objinfo.loc[ :, 'decerr' ] = None
                objinfo.loc[ :, 'ra_dec_cov' ] = None
            else:
                if include_base_procver:
                    objinfo['pos_base_procver'] = [ None ] * len( objinfo['diaobjectid'] )
                objinfo['ra']         = [ None ] * len( objinfo['diaobjectid'] )
                objinfo['dec']        = [ None ] * len( objinfo['diaobjectid'] )
                objinfo['raerr']      = [ None ] * len( objinfo['diaobjectid'] )
                objinfo['decerr']     = [ None ] * len( objinfo['diaobjectid'] )
                objinfo['ra_dec_cov'] = [ None ] * len( objinfo['diaobjectid'] )

        for lc in ltcvs:
            rootid = lc['rootid']
            weight = lc['flux'] / lc['fluxerr']
            w = np.where( np.array( lc['isdet'] ) & ( weight > 3 ) )[0]
            weight = weight[w] ** 2
            meanra = ( lc['det_ra'][w] * weight ).sum() / weight.sum()
            meandec = ( lc['det_dec'][w] * weight ).sum() / weight.sum()
            raerr = np.sqrt( ( weight * ( lc['det_ra'][w] - meanra )**2 ).sum() / weight.sum() )
            decerr = np.sqrt( ( weight * ( lc['det_dec'][w] - meandec )**2 ).sum() / weight.sum() )
            ra_dec_cov = ( weight * ( lc['det_ra'][w] - meanra ) *
                           ( lc['det_dec'][w] - meandec ) ).sum() / weight.sum()

            if return_format == 'pandas':
                objinfo.loc[ (objinfo['rootid'] == rootid) & pandas.isna(objinfo['ra']) , 'dec' ] = meandec
                objinfo.loc[ (objinfo['rootid'] == rootid) & pandas.isna(objinfo['ra']) , 'raerr' ] = raerr
                objinfo.loc[ (objinfo['rootid'] == rootid) & pandas.isna(objinfo['ra']) , 'decerr' ] = decerr
                objinfo.loc[ (objinfo['rootid'] == rootid) & pandas.isna(objinfo['ra']) , 'ra_dec_cov' ] = ra_dec_cov
                # Do ra last so as not to screw up the loc selection in the previous lines
                objinfo.loc[ (objinfo['rootid'] == rootid) & pandas.isna(objinfo['ra']) , 'ra' ] = meanra
            else:
                for i in range( len( objinfo['diaobjectid'] ) ):
                    if ( objinfo['rootid'][i] == rootid ) and ( objinfo['ra'][i] is None ):
                        objinfo['ra'][i] = meanra
                        objinfo['dec'][i] = meandec
                        objinfo['raerr'][i] = raerr
                        objinfo['decerr'][i] = decerr
                        objinfo['ra_dec_cov'][i] = ra_dec_cov

        FDBLogger.debug( "...done with weighted source positions." )

    if must_get_source_positions:
        if not include_source_positions:
            for row in ltcvs:
                for col in ['det_ra', 'det_dec', 'det_raerr', 'det_decerr', 'det_ra_dec_cov']:
                    del row[col]
            if use_weighted_source_positions:
                for row in ltcvs:
                    for col in ['flux', 'fluxerr']:
                        row[col] = np.where( np.isnan(row[col]), None, row[col] ).tolist()
        elif use_weighted_source_positions:
            # Turn the few things we made into numpy arrays back into lists, making nan back into None
            for row in ltcvs:
                for col in ['det_ra', 'det_dec', 'flux', 'fluxerr']:
                    row[col] = np.where( np.isnan(row[col]), None, row[col] ).tolist()

    if which == 'forced':
        # Remove sources and the "patch" column
        for row in ltcvs:
            for k, v in row.items():
                if k == 'rootid':
                    continue
                row[k] = [ i for p, i in zip( row['ispatch'], v ) if not p ]
            del row['ispatch']

    if not include_obj_base_procver_id:
        for row in ltcvs:
            del row['source_obj_bpv']
            if which != 'detections':
                del row['forced_obj_bpv']

    if return_format == 'pandas':
        ltcvs = laboriously_construct_pandas( ltcvs, keyname='rootid', indices=['mjd'],
                                              int64cols=['diaforcedsourceid', 'diasourceid', 'visit',
                                                         'source_diaobjectid', 'forced_diaobjectid'],
                                              floatcols=['flux', 'fluxerr', 'det_raerr',
                                                         'det_decerr', 'det_ra_dec_cov'],
                                              doublecols=['mjd', 'det_ra', 'det_dec'],
                                              boolcols=['isdet', 'ispatch'],
                                              ignore_missing_cols=True )

    FDBLogger.debug( "...done with many_object_ltcvs" )
    if return_object_info:
        return ltcvs, objinfo
    else:
        return ltcvs


def object_ltcv( processing_version='default', diaobjectid=None, bands=None, which='patch',
                 include_base_procver=False, include_source_positions=False,
                 use_weighted_source_positions=False, always_use_weighted_source_positions=False,
                 return_format='json',
                 return_object_info=False, include_object_positions=False, position_processing_version=None,
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
       Same as what you'd get back from many_object_ltcvs, just that
       there will only be one key (if return_format='json'), or only one
       unique value in the 'rootid' index (if return_format='pandas').
    """

    rval = many_object_ltcvs( processing_version=processing_version,
                              objids=[ diaobjectid ],
                              bands=bands,
                              which=which,
                              include_base_procver=include_base_procver,
                              include_source_positions=include_source_positions,
                              return_object_info=return_object_info,
                              include_object_positions=include_object_positions,
                              position_processing_version=position_processing_version,
                              use_weighted_source_positions=use_weighted_source_positions,
                              always_use_weighted_source_positions=always_use_weighted_source_positions,
                              return_format=return_format,
                              mjd_now=mjd_now,
                              dbcon=dbcon )
    if return_object_info:
        ltcvs = rval[0]
        objinfo = rval[1]
    else:
        ltcvs = rval
        objinfo = None

    # Sanity check.  Some of these tests are gratuitous
    #  if many_object_ltcvs is written right.
    notfound = False
    morethanone = False
    if return_format == 'pandas':
        if not ( isinstance( ltcvs, pandas.DataFrame )
                 and
                 ( ( objinfo is None ) or isinstance( objinfo, pandas.DataFrame ) )
                ):
            raise RuntimeError( "This should never happen." )
        notfound = ( len(ltcvs) == 0 )
        morethanone = ( len( ltcvs.index.get_level_values('rootid').unique() ) > 1 )
    elif return_format == 'json':
        if not ( isinstance( ltcvs, list )
                 and
                 ( ( objinfo is None ) or isinstance( objinfo, dict ) )
                ):
            raise RuntimeError( "This should never happen." )
        notfound = ( len(ltcvs) == 0 )
        morethanone = ( len(ltcvs) > 1 )
    else:
        raise RuntimeError( "This should never happen." )

    if notfound:
        raise RuntimeError( f"Could not find object for diaobjectid {diaobjectid}" )
    if morethanone:
        raise RuntimeError( f"Woah, got multiple lightcurves for diaobjectid {diaobjectid}, "
                            f"processing version {processing_version}.  This shouldn't happen." )

    if return_format == 'pandas':
        return rval
    else:
        return ( ltcvs[0], objinfo ) if objinfo is not None else ltcvs[0]


def debug_count_temp_table( con, table ):
    res = con.execute( f"SELECT COUNT(*) FROM {table}" )
    FDBLogger.debug( f"Table {table} has {res[0][0]} rows" )


_object_search_timings = {}
_object_search_timings_count = {}


def object_search( processing_version='default', ignore_object_processing_version=False,
                   object_processing_version=None, position_processing_version=None,
                   return_format='json', just_objids=False, noforced=False, dbcon=None, mjd_now=None,
                   fall_back_to_root_position=True, only_use_root_position=False,
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

      object_processing_version : UUID or str, default None
          The processing version for objects.  Defaults to
          processing_version if not given, but it's complicated;
          see ignore_object_processing_version.

      ignore_object_processing_version: book default False
          OK, this is complicated.  It's possible that there are
          diasources whose diaobjects are in a *different* processing
          version.  Yes, that's perverse, but it can happen.  If you
          think that could happen here, then set
          ignore_object_processing_version=True.  (You will then also
          get an unpredictable diaobjectid back, but you probably don't
          care... you *shouldn't* care, you should be tracking root
          ids.)  It's possible this will make things slower, as there
          will be more rows in temporary tables with diaobjects that
          will eventually be thrown out anyway.

      position_processing_version : UUID or str, default None
          The processing version of diaobject positions.  If None, will
          use the same as processing_version.  IMPORTANT.  This one
          is easy to get wrong.  Make sure you know what processing
          versions are in the database and what to do with them.

      fall_back_to_root_position : bool, default True
          There may well be objects that don't have a diaobject position
          in the processing version we're talking about.  If this is True,
          when a diaobject_position is not found, just use the position
          that's stored in root_diaobject.

          Note: the actual *search* is always done on the rootdiaobject
          positions, as those will be good to an arcsecond or so.  It's
          just a matter of the reported ras and decs.

      only_use_root_position: bool, default False
          If True, ignore the diaobject_position table.  Returned
          positions will... probably... not be as good, but the search
          *might* be faster.

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
          position_base_procver — the description of the base_processing_version for the position,
                                  or None if the root diaobject position was used
          obj_base_procver — the desription of the base_processing_version used for the diaobject,
                               or None if ignore_object_processing_version is True
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
    global _object_search_timings, _object_search_timings_count
    timings = {}

    FDBLogger.debug( f"In object_search : kwargs = {kwargs}" )
    knownargs = { 'ra', 'dec', 'radius', 'fall_back_to_root_position', 'only_use_root_position',
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
        FDBLogger.debug( "Object search making initial view" )
        t0 = time.perf_counter()
        if only_use_root_position:
            q = sql.SQL( textwrap.dedent(
                """\
                CREATE TEMPORARY VIEW tmp_diaobject_with_position AS
                SELECT DISTINCT ON (o.rootid) o.rootid, r.ra AS rootra, r.dec AS rootdec,
                                              NULL::double precision AS ra,
                                              NULL::double precision AS dec,
                                              NULL::text AS pos_base_procver, """ ) )
        else:
            q = sql.SQL( textwrap.dedent(
                """\
                CREATE TEMPORARY VIEW tmp_diaobject_with_position AS
                SELECT DISTINCT ON (o.rootid) o.rootid, r.ra AS rootra, r.dec AS rootdec, p.ra, p.dec,
                                              p.description AS pos_base_procver, """ ) )
        q += sql.SQL( "                              obpv.description AS obj_base_procver" )
        if objprocver is not None:
            q += sql.SQL( ",\n                              pv.priority" )
        q += sql.SQL( textwrap.dedent(
            """
            FROM diaobject o
            INNER JOIN root_diaobject r ON o.rootid=r.id
            INNER JOIN base_processing_version obpv ON o.base_procver_id=obpv.id
            """ ) )
        if objprocver is not None:
            q += sql.SQL( textwrap.dedent(
                """\
                INNER JOIN base_procver_of_procver pv ON pv.base_procver_id=o.base_procver_id
                                                     AND pv.procver_id={objprocver}
                """ ) ).format( objprocver=str(objprocver) )
        if not only_use_root_position:
            q += sql.SQL( textwrap.dedent(
                """\
                LEFT JOIN (
                  SELECT DISTINCT ON(p1.diaobjectid) p1.diaobjectid, p1.ra, p1.dec, posbpv.description
                  FROM diaobject_position p1
                  INNER JOIN base_procver_of_procver p1pv ON p1.base_procver_id=p1pv.base_procver_id
                                                         AND p1pv.procver_id={pospv}
                  INNER JOIN base_processing_version posbpv ON p1pv.base_procver_id=posbpv.id
                  ORDER BY p1.diaobjectid, p1pv.priority DESC
                ) p ON p.diaobjectid=o.diaobjectid
                """ ) ).format( pospv=str(posprocver) )
        q += sql.SQL( "ORDER BY o.rootid" )
        if objprocver is not None:
            q += sql.SQL( ", pv.priority DESC" )
        con.execute_nofetch( q )
        t1 = time.perf_counter()
        timings[ 'make_initial_view' ] = t1 - t0
        # ****
        debug_count_temp_table( con, 'tmp_diaobject_with_position' )
        t2 = time.perf_counter()
        timings[ 'count_initial_view'] = t2 - t1
        # ****
        nexttable = 'tmp_diaobject_with_position'

        # Filter by ra and dec if given
        if ra is not None:
            t0 = time.perf_counter()
            FDBLogger.debug( "Object search filtering by ra/dec" )
            radius = util.float_or_none_from_dict( kwargs, 'radius' )
            radius = radius if radius is not None else 10.
            q = sql.SQL( textwrap.dedent(
                """\
                SELECT DISTINCT ON(o.rootid) * INTO TEMP TABLE objsearch_radeccut
                FROM {nexttable} o
                WHERE q3c_radial_query( o.rootra, o.rootdec, %(ra)s, %(dec)s, %(rad)s )
                ORDER BY rootid
                """ ) ).format( nexttable=sql.Identifier(nexttable) )
            if objprocver is not None:
                q += sql.SQL( ", o.priority DESC\n" )
            subdict = { 'ra': ra, 'dec': dec, 'rad': radius/3600. }
            con.execute_nofetch( q, subdict )
            t1 = time.perf_counter()
            timings[ 'object_radec_filter' ] = t1 - t0
            # ****
            debug_count_temp_table( con, 'objsearch_radeccut' )
            t2 = time.perf_counter()
            timings[ 'count_object_radec_filter'] = t2 - t1
            # ****
            nexttable = 'objsearch_radeccut'

        # Count (and maybe filter) by number of detections within the time window
        # ROB TODO : use processing version index
        if window_t0 is not None:
            t0 = time.perf_counter()
            FDBLogger.debug( "Object search finding detections within window" )
            # # ROB -- whgy was thus necessary?
            # # Needs to be adapted fo runreliable diabjectid
            # if nexttable != 'diaobject':
            #     # Make a primary key so we can group by
            #     con.execute_nofetch( f"ALTER TABLE {nexttable} ADD PRIMARY KEY (diaobjectid)",
            #                          explain=False, analyze=False )
            subdict = { 'pv': procver, 't0': window_t0, 't1': window_t1 }
            q = sql.SQL( textwrap.dedent(
                """\
                /*+ IndexScan(s idx_diasource_diaobjectid idx_diasource_mjd) */
                SELECT rootid, rootra, rootdec, ra, dec, pos_base_procver, obj_base_procver, numdetinwindow
                INTO TEMP TABLE objsearch_windowdet
                FROM (
                  SELECT rootid, rootra, rootdec, ra, dec, pos_base_procver, obj_base_procver,
                         COUNT(visit) AS numdetinwindow
                  FROM (
                    SELECT DISTINCT ON (o.rootid,s.visit) o.rootid, o.rootra, o.rootdec, o.ra, o.dec,
                                                          o.pos_base_procver, o.obj_base_procver, s.visit
                    FROM {nexttable} o
                    INNER JOIN diaobject o2 ON o.rootid=o2.rootid
                    INNER JOIN diasource s ON s.diaobjectid=o2.diaobjectid
                    INNER JOIN base_procver_of_procver pv ON s.base_procver_id=pv.base_procver_id
                                                         AND pv.procver_id=%(pv)s
                    WHERE s.midpointmjdtai>=%(t0)s AND s.midpointmjdtai<=%(t1)s
                """ ) ).format( nexttable=sql.Identifier(nexttable) )
            if statbands is not None:
                q += sql.SQL( "    AND s.band=ANY(%(bands)s)\n" )
                subdict['bands'] = statbands
            q += sql.SQL( textwrap.dedent(
                """\
                    ORDER BY o.rootid, s.visit, pv.priority DESC
                  ) subsubq
                  GROUP BY rootid, rootra, rootdec, ra, dec, pos_base_procver, obj_base_procver
                ) subq
                """ ) )
            _and = "WHERE"
            if min_window_numdetections is not None:
                q += sql.SQL( f"{_and} numdetinwindow>=%(minn)s\n" )
                subdict['minn'] = min_window_numdetections
                _and = "  AND"
            if max_window_numdetections is not None:
                q += sql.SQL( f"{_and} numdetinwindow<=%(maxn)s\n" )
                subdict['maxn'] = max_window_numdetections
                _and = "  AND"
            con.execute_nofetch( q, subdict )
            t1 = time.perf_counter()
            timings[ 'window' ] = t1 - t0
            # ****
            debug_count_temp_table( con, 'objsearch_windowdet' )
            t2 = time.perf_counter()
            timings[ 'count_window' ] = t2 - t1
            # ****
            nexttable = 'objsearch_windowdet'
            addlfields.append( "numdetinwindow" )

        # First pass cut that has *any* detection with (min(minfirst,minlast) < t < max(maxfirst,maxlast)
        #   to try to cut down the total size of stuff to think about in our next big join
        # TODO : also think about adding magnitude cuts here!  May not
        #   be worth it since we don't have indexes on fluxes.  (Maybe we should?)  (No way.)
        if any( i is not None for i in [ mint_firstdetection, maxt_firstdetection,
                                         mint_lastdetection, maxt_lastdetection ] ):
            if ( ( maxt_lastdetection is not None ) and ( mint_firstdetection is not None ) and
                 ( mint_firstdetection < maxt_lastdetection ) ):
                raise RuntimeError( "maxt_lastdetection > mint_firstdetection, which makes no sense." )
            t0 = time.perf_counter()
            FDBLogger.debug( "Object search doing first rough cut on detection times" )
            subdict = { 'pv': procver }
            q = sql.SQL( textwrap.dedent(
                """\
                /*+ IndexScan(s idx_diasource_diaobjectid idx_diasource_mjd) */
                SELECT * INTO TEMP TABLE objsearch_detcut FROM (
                  SELECT DISTINCT ON (o.rootid) o.rootid, o.rootra, o.rootdec, o.ra, o.dec,
                                                          o.pos_base_procver, o.obj_base_procver""" ) )
            for f in addlfields:
                q += sql.SQL( ", " ) + sql.Identifier( 'o', f )
            q += sql.SQL( textwrap.indent( textwrap.dedent(
                """
                  FROM {nexttable} o
                  INNER JOIN diaobject o2 ON o.rootid=o2.rootid
                  INNER JOIN diasource s ON o2.diaobjectid=s.diaobjectid
                  INNER JOIN base_procver_of_procver pv ON s.base_procver_id=pv.base_procver_id
                    AND pv.procver_id=%(pv)s
                """ ), "  " ) ).format( nexttable=sql.Identifier( nexttable ) )
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
            t1 = time.perf_counter()
            timings[ 'rough_detection_time_cut' ] = t1 - t0
            # ****
            debug_count_temp_table( con, 'objsearch_detcut' )
            t2 = time.perf_counter()
            timings[ 'count_rough_detection_time_cut' ] = t2 - t1
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

        t0 = time.perf_counter()
        con.execute( f"ANALYZE {nexttable}(rootid)", explain=False )
        t1 = time.perf_counter()
        timings[ 'analyze' ] = t1 - t0

        # First: build the table, put in first detection
        FDBLogger.debug( "Object search making stat tab with first detection" )
        t0 = time.perf_counter()
        q = sql.SQL( textwrap.dedent(
            """\
            /*+ IndexScan(s idx_diasource_diaobjectid)
                Leading( ( (o s) pv ) ) */
            SELECT * INTO TEMP TABLE objsearch_stattab FROM (
              SELECT DISTINCT ON (rootid) rootid, ra, dec, obj_base_procver, pos_base_procver""" ) )
        for f in addlfields:
            q += sql.SQL(", ") + sql.Identifier( f )
        q += sql.SQL( textwrap.dedent(
            """\
            ,
                    NULL::integer as numdet,
                    midpointmjdtai AS firstdetmjd, band AS firstdetband,
                    psfflux AS firstdetflux, psffluxerr AS firstdetfluxerr,
                    NULL::double precision as lastdetmjd, NULL::text as lastdetband,
                    NULL::double precision as lastdetflux, NULL::double precision as lastdetfluxerr,
                    NULL::double precision as maxdetmjd, NULL::text as maxdetband,
                    NULL::double precision as maxdetflux, NULL::double precision as maxdetfluxerr
              FROM (
                SELECT rootid, ra, dec, obj_base_procver, pos_base_procver,
                       midpointmjdtai, band, psfflux, psffluxerr""" ) )
        for f in addlfields:
            q += sql.SQL( ", " ) + sql.Identifier( f )
        q += sql.SQL( textwrap.indent( textwrap.dedent(
            """
                FROM (
                  SELECT DISTINCT ON (o.rootid,s.visit) o.rootid, o.pos_base_procver, o.obj_base_procver,
                    s.midpointmjdtai, s.band, s.psfflux, s.psffluxerr""" ), "    " ) )
        for f in addlfields:
            q += sql.SQL( ", " ) + sql.Identifier( 'o', f )
        q += sql.SQL( textwrap.dedent(
            """\
            ,
                    CASE WHEN o.pos_base_procver IS NULL THEN o.rootra ELSE o.ra END AS ra,
                    CASE WHEN o.pos_base_procver IS NULL THEN o.rootdec ELSE o.dec END AS dec
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
        t1 = time.perf_counter()
        timings[ 'make stat tab' ] = t1 - t0

        # Add in last detection
        FDBLogger.debug( "Object search adding last detection to stat tab" )
        q = sql.SQL( textwrap.dedent(
            """\
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
            """\
                ORDER BY o.rootid, s.visit, pv.priority DESC
              ) subsubq
              ORDER BY rootid, midpointmjdtai DESC
            ) subq
            WHERE subq.rootid=ost.rootid
            """ ) )
        con.execute_nofetch( q, subdict )
        t2 = time.perf_counter()
        timings[ 'add last det' ] = t2 - t1

        # Add in max detection
        FDBLogger.debug( "Object search adding max detection to stat tab" )
        q = sql.SQL( textwrap.dedent(
            """\
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
        t3 = time.perf_counter()
        timings[ 'add max det' ] = t3 - t2

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
        t4 = time.perf_counter()
        timings[ 'add num dets' ] = t4 - t3
        # ****
        debug_count_temp_table( con, 'objsearch_stattab' )
        t5 = time.perf_counter()
        timings[ 'count stattab' ] = t5 - t4
        # ****

        # Delete from this table based on numdet and detection time as appropriate
        t0 = time.perf_counter()
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
        t1 = time.perf_counter()
        timings[ 'apply cuts' ] = t1 - t0

        if ( just_objids or noforced ) and ( min_lastmag is None ) and ( max_lastmag is None ):
            FDBLogger.debug( "Object search pulling down results" )
            t0 = time.perf_counter()
            # No need to search the forced source table, and that can be slow because the
            #  forced photometry table is huge, so just skip it.
            if just_objids:
                rows, columns = con.execute( "SELECT rootid FROM objsearch_stattab" )
            else:
                rows, columns = con.execute( "SELECT * FROM objsearch_stattab" )
            t1 = time.perf_counter()
            timings[ 'extract' ] = t1 - t0

        else:
            FDBLogger.debug( "Object search adding last forced photometry to stat tab" )
            # In this else block, we need to get the latest forced photometry, so do that.
            nexttable = 'objsearch_stattab'

            # Because the diaforcedsource table is going to be the hugest one,
            #   create an index rootid of {nexttable} here to help
            #   this next query along.  We hope.
            t0 = time.perf_counter()
            con.execute( f"CREATE INDEX idx_t_rootid ON {nexttable}(rootid)", explain=False )
            t1 = time.perf_counter()
            timings[ 'create index' ] = t1 - t0

            # Reanalyze this table to help postgres do the right thing
            con.execute( f"ANALYZE {nexttable}(rootid)", explain=False )
            t2 = time.perf_counter()
            timings[ 'analyze 2' ] = t2 - t1

            # Get the last forced source
            # NOTE: I had a HashJoin( f t ) in here that I removed
            q = sql.SQL( textwrap.dedent(
                """\
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
            t3 = time.perf_counter()
            timings[ 'add last forcedsource' ] = t3 - t2
            # ****
            debug_count_temp_table( con, 'objsearch_final' )
            t4 = time.perf_counter()
            timings[ 'count after add last forcedsource' ] = t4 - t3
            # ****

            # Filter based on last magnitude
            t5 = time.perf_counter()
            if min_lastmag is not None:
                con.execute_nofetch( "DELETE FROM objsearch_final WHERE lastforcedflux>%(f)s",
                                     { 'f': 10**((min_lastmag-zp)/-2.5) } )
                debug_count_temp_table( con, 'objsearch_final' )
            if max_lastmag is not None:
                con.execute_nofetch( "DELETE FROM objsearch_final WHERE lastforcedflux<%(f)s",
                                     { 'f': 10**((max_lastmag-zp)/-2.5) } )
                debug_count_temp_table( con, 'objsearch_final' )
            t6 = time.perf_counter()
            timings[ 'last mag filter' ] = t6 - t5

            # Pull down the results
            FDBLogger.debug( "Object search pulling down results" )
            if just_objids:
                rows, columns = con.execute( "SELECT rootid FROM objsearch_final" )
            else:
                rows, columns = con.execute( "SELECT * FROM objsearch_final" )
            t7 = time.perf_counter()
            timings[ 'extract' ] = t7 - t6

    columnmap = { columns[i]: i for i in range(len(columns)) }
    FDBLogger.debug( f"object_search returning {len(rows)} objects in format {return_format}" )

    if return_format == 'json':
        rval = { c: [ r[columnmap[c]] for r in rows ] for c in columns }
        if ( not just_objids ) and ( 'numdetinwindow' not in rval ):
            rval['numdetinwindow'] = [ None for r in rows ]
        # FDBLogger.debug( f"returning json\n{json.dumps(rval,indent=4)}" )

    elif return_format == 'pandas':
        df = pandas.DataFrame( rows, columns=columns )
        if ( not just_objids ) and ( 'numdetinwindow' not in df.columns ):
            df['numdetinwindow'] = None
        # FDBLogger.debug( f"object_search pandas dataframe: {df}" )
        rval = df

    else:
        raise RuntimeError( "This should never happen." )

    strio = io.StringIO()
    strio.write( "Object Search timings:\n" )
    for k, v in timings.items():
        strio.write( f"      {k:>34s} : {v:8.5f}\n" )
        if k in _object_search_timings:
            _object_search_timings[k] += v
            _object_search_timings_count[k] += 1
        else:
            _object_search_timings[k] = v
            _object_search_timings_count[k] = 1
    FDBLogger.debug( strio.getvalue() )

    return rval


def get_hot_ltcvs( processing_version, position_processing_version=None,
                   include_object_positions=True, include_source_positions=False, include_base_procver=False,
                   use_weighted_source_positions=False, always_use_weighted_source_positions=False,
                   detected_since_mjd=None, detected_in_last_days=None,
                   mjd_now=None, source_patch=True, return_format='json', dbcon=None ):
    """Get lightcurves of objects with a recent detection.

    Parameters
    ----------
      processing_version: string
        The description of the processing version, or processing version
        alias, to use for searching diasource and diaforcedsource tables.

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
        implies include_object_positions=False.  (These positions may
        be better than the ones you get from the diaobject_position table,
        at least for realtime sources.  The case may be different in the
        future when we've loaded in actual data releases.)

      include_object_positions: bool, default True
        Include positions from the diaobject_position table.

      include_source_positions: bool, default False
        Include positions from the diasource table.

      include_processing_versions : bool, default False
        If true, then returned objects will include the names of the
        base processing versions of various things.

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
        it's a different time.  (This isn't a perfect recreation of the
        past.  This is just a cut on the MJDs of objects to return.  It
        will not return *what the database knew* when it really was
        mjd_now, but *everything you've asked for with mjd ≤ mjd_now*.
        In practice, the database won't get some information until some
        time (at least seconds, but potentially days or, in edge cases,
        more) after the time of the observation.

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
        ltcvdf, objinfo

        ltcvdf: pandas.DataFrame
           A dataframe with lightcurves. It is sorted and indexed by
           rootid (uuid) and mjd (float).  rootid is what you should
           use to uniquely identify objects, and is what FASTDB
           considers to be the identifier of an object.  rootid
           transcends processing version; all diaobjects in all
           processing versions at the same point on the sky will
           all have the same rootids.

           ltcvdf  has columns:

             diasourceid -- the diaSourceId for this source, or None
             diaforcedsourceid -- the diaForcedSourceId for this source, or None
             diaobjectid -- the diaObjectId that FASTDB thinks is associated with this (forced)source (*see below)
             visit -- the visit number
             mjd -- the MJD of the obervation
             band -- the filter (u, g, r, i, z, or Y)
             flux -- the PSF flux in nJy, from the diaForcedSource if it exists, otherwise the diaSource
             fluxerr -- uncertaintly on psfflux in nJy
             istdet -- bool, True if this was detected (i.e. has an associated source)
             ispatch -- bool, True if this data is from a source rather than a forced source
                        (only included if source_patch is True)

             Optional columns (will only be present if certain things
               are True when this function is called):

             det_ra, det_dec, det_raerr, det_decerr, det_ra_dec_cov -- float
                  The positions from the diaSource.  These will only be included if
                  include_source_positions is True.  They will all be null if
                  source_patch is False.  They will be null for points that
                  have forced photometry but no detection photometry in the database.

             source_base_procver -- string, the name of the base processing version of this diasource, or None
             forcedsource_base_procver -- str/None, the name of the base processsing version of this diaforcedsource
             obj_base_procver -- string, the name of the base processing version of this diaobject

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
           NOT CURRENTLY SUPPORTED.... so not returned

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
        try:
            procver = util.procver_id( processing_version, dbcon=con.con )

            # First : get a table of all root object ids that have a
            #   detection (i.e. a diasource) in the desired time period.

            q = sql.SQL( textwrap.dedent(
                """\
                /*+ IndexScan(s idx_diasource_mjd) */
                SELECT DISTINCT ON(o.rootid) o.rootid
                INTO TEMP TABLE tmp_objids
                FROM diasource s
                INNER JOIN diaobject o ON s.diaobjectid=o.diaobjectid
                INNER JOIN base_procver_of_procver pv ON s.base_procver_id=pv.base_procver_id
                                                     AND pv.procver_id=%(procver)s
                WHERE s.midpointmjdtai>=%(t0)s
                """ ) )
            if mjd_now is not None:
                q += sql.SQL( "  AND s.midpointmjdtai<=%(t1)s\n" )
            q += sql.SQL( "ORDER BY o.rootid\n" )
            con.execute_nofetch( q, { 'procver': procver, 't0': mjd0, 't1': mjd_now } )

            # Second: get the lightcurves and object info
            return many_object_ltcvs( processing_version=procver, objids_table='tmp_objids',
                                      return_format=return_format, mjd_now=mjd_now, dbcon=con,
                                      which='patch' if source_patch else 'forced',
                                      include_base_procver=include_base_procver,
                                      include_source_positions=include_source_positions,
                                      use_weighted_source_positions=use_weighted_source_positions,
                                      always_use_weighted_source_positions=always_use_weighted_source_positions,
                                      include_object_positions=include_object_positions,
                                      return_object_info=True )

        finally:
            # This is probably gratuitous.  However, *just in case*
            # somebody passed a dbcon, and might try to create temp
            # tables that have exactly the same names as the temp tables
            # we just created, drop them for cleanliness.
            con.execute( "DROP TABLE IF EXISTS tmp_objids", explain=False )

            # However, don't commit!  Reason: if somebody passed a dbcon
            # in the middle of a transaction, for whatever perverse
            # reason, we don't want to end that transaction.  If
            # somebody passed a dbcon, then the table is already deleted
            # for that dbcon now even without committing.  If there was
            # no passed dbcon, then the while db.DBCon() loop will close
            # the connection when it exits, which will automatically
            # delete the temp tables anyway.


def create_object_stats_materialized_view( procver ):
    with db.DBCon( dictcursor=True ) as dbcon:
        # Check to see if it already exists
        q = sql.SQL( "SELECT * FROM pg_class WHERE relname={viewname}" ).format( viewname=f'objstats_{procver}' )
        rows = dbcon.execute( q )
        if len(rows) > 0:
            row = rows[0]
            if row['relkind'] != 'm':
                raise RuntimeError( f"postgres class objstats_{procver} exists, but is not a materialized "
                                    f"view!  (It is a \"{row['relkind']}\")" )
            q = sql.SQL( "SELECT a.attname FROM pg_catalog.pg_attribute a "
                         "INNER JOIN pg_class c ON c.oid=a.attrelid "
                         "WHERE c.relname={viewname} AND a.attnum>0"
                        ).format( viewname=f'objstats_{procver}' )
            rows = dbcon.execute( q )
            if ( set( r['attname'] for r in rows ) !=
                 { 'rootid', 'band', 'ra', 'dec', 'firstdet_mjd', 'firstdet_flux', 'firstdet_fluxerr',
                   'lastdet_mjd', 'lastdet_flux', 'lastdet_fluxerr', 'maxdet_mjd', 'maxdet_flux', 'maxdet_fluxerr',
                   'ndets', 'ndets24', 'ndets23', 'ndets22', 'ndets21', 'nsn10', 'nsn7', 'nsn5' }
                ):
                raise RuntimeError( f"postgres view objstats_{procver} has the wrong set of columns" )

            FDBLogger.info( f"Refreshing materizalized view objstats_{procver}" )
            q = sql.SQL( "REFRESH MATERIALIZED VIEW {viewname}"
                        ).format( viewname=sql.Identifier( f'objstats_{procver}' ) )
            dbcon.execute( q )
            dbcon.commit()
            FDBLogger.info( f"Done refreshing materialized view objstats_{procver}" )
            return

        # If we get here, the materialized view does not exist

        FDBLogger.info( f"Creating materialized view objstats_{procver}" )
        pvid = db.ProcessingVersion.procver_id( procver, dbcon=dbcon )

        q = sql.SQL( textwrap.dedent(
            """
            CREATE MATERIALIZED VIEW {viewname} AS (
               SELECT d0.rootid AS rootid, d0.band AS band, r.ra AS ra, r.dec AS dec,
                   d0.midpointmjdtai AS firstdet_mjd, d0.psfflux AS firstdet_flux, d0.psffluxerr AS firstdet_fluxerr,
                   dn.midpointmjdtai AS lastdet_mjd, dn.psfflux AS lastdet_flux, dn.psffluxerr AS lastdet_fluxerr,
                   dx.midpointmjdtai AS maxdet_mjd, dx.psfflux AS maxdet_flux, dx.psffluxerr AS maxdet_fluxerr,
                   n.ndets AS ndets,
                   CASE WHEN n24.ndets IS NULL THEN 0 ELSE n24.ndets END as ndets24,
                   CASE WHEN n23.ndets IS NULL THEN 0 ELSE n23.ndets END AS ndets23,
                   CASE WHEN n22.ndets IS NULL THEN 0 ELSE n22.ndets END AS ndets22,
                   CASE WHEN n21.ndets IS NULL THEN 0 ELSE n21.ndets END AS ndets21,
                   CASE WHEN sn10.ndets IS NULL THEN 0 ELSE sn10.ndets END AS nsn10,
                   CASE WHEN sn7.ndets IS NULL THEN 0 ELSE sn7.ndets END AS nsn7,
                   CASE WHEN sn5.ndets IS NULL THEN 0 ELSE sn5.ndets END AS nsn5
               FROM (
                  SELECT DISTINCT ON(rootid, band) rootid, band, midpointmjdtai, psfflux, psffluxerr
                  FROM (
                     SELECT DISTINCT ON(o.rootid, s.visit) o.rootid, s.band, s.midpointmjdtai, s.psfflux, s.psffluxerr
                     FROM diasource s
                     INNER JOIN diaobject o ON s.diaobjectid=o.diaobjectid
                     INNER JOIN base_procver_of_procver j ON s.base_procver_id=j.base_procver_id
                                                         AND j.procver_id={pvid}
                     ORDER BY o.rootid, s.visit, j.priority DESC
                  ) subq
                  ORDER BY rootid, band, midpointmjdtai
               ) d0
               INNER JOIN root_diaobject r ON d0.rootid=r.id
               INNER JOIN (
                  SELECT DISTINCT ON(rootid, band) rootid, band, midpointmjdtai, psfflux, psffluxerr
                  FROM (
                     SELECT DISTINCT ON(o.rootid, s.visit) o.rootid, s.band, s.midpointmjdtai, s.psfflux, s.psffluxerr
                     FROM diasource s
                     INNER JOIN diaobject o ON s.diaobjectid=o.diaobjectid
                     INNER JOIN base_procver_of_procver j ON s.base_procver_id=j.base_procver_id
                                                         AND j.procver_id={pvid}
                     ORDER BY o.rootid, s.visit, j.priority DESC
                  ) subq
                  ORDER BY rootid, band, midpointmjdtai DESC
               ) dn ON d0.rootid=dn.rootid and d0.band=dn.band
               INNER JOIN (
                  SELECT DISTINCT ON(rootid, band) rootid, band, midpointmjdtai, psfflux, psffluxerr
                  FROM (
                     SELECT DISTINCT ON(o.rootid, s.visit) o.rootid, s.band, s.midpointmjdtai, s.psfflux, s.psffluxerr
                     FROM diasource s
                     INNER JOIN diaobject o ON s.diaobjectid=o.diaobjectid
                     INNER JOIN base_procver_of_procver j ON s.base_procver_id=j.base_procver_id
                                                         AND j.procver_id={pvid}
                     ORDER BY o.rootid, s.visit, j.priority DESC
                  ) subq
                  ORDER BY rootid, band, psfflux DESC
               ) dx ON d0.rootid=dx.rootid AND d0.band=dx.band
               INNER JOIN (
                  SELECT rootid, band, COUNT(diasourceid) AS ndets
                  FROM (
                     SELECT DISTINCT ON(o.rootid, s.visit) o.rootid, s.band, s.diasourceid
                     FROM diasource s
                     INNER JOIN diaobject o ON s.diaobjectid=o.diaobjectid
                     INNER JOIN base_procver_of_procver j ON s.base_procver_id=j.base_procver_id
                                                         AND j.procver_id={pvid}
                     ORDER BY o.rootid, s.visit, j.priority DESC
                  ) subq
                  GROUP BY rootid, band
               ) n ON d0.rootid=n.rootid AND d0.band=n.band
               LEFT JOIN (
                  SELECT rootid, band, COUNT(diasourceid) AS ndets
                  FROM (
                     SELECT DISTINCT ON(o.rootid, s.visit) o.rootid, s.band, s.diasourceid
                     FROM diasource s
                     INNER JOIN diaobject o ON s.diaobjectid=o.diaobjectid
                     INNER JOIN base_procver_of_procver j ON s.base_procver_id=j.base_procver_id
                                                         AND j.procver_id={pvid}
                     WHERE s.psfflux >= 912
                     ORDER BY o.rootid, s.visit, j.priority DESC
                  ) subq
                  GROUP BY rootid, band
               ) n24 ON d0.rootid=n24.rootid AND d0.band=n24.band
               LEFT JOIN (
                  SELECT rootid, band, COUNT(diasourceid) AS ndets
                  FROM (
                     SELECT DISTINCT ON(o.rootid, s.visit) o.rootid, s.band, s.diasourceid
                     FROM diasource s
                     INNER JOIN diaobject o ON s.diaobjectid=o.diaobjectid
                     INNER JOIN base_procver_of_procver j ON s.base_procver_id=j.base_procver_id
                                                         AND j.procver_id={pvid}
                     WHERE s.psfflux >= 2291
                     ORDER BY o.rootid, s.visit, j.priority DESC
                  ) subq
                  GROUP BY rootid, band
               ) n23 ON d0.rootid=n23.rootid AND d0.band=n23.band
               LEFT JOIN (
                  SELECT rootid, band, COUNT(diasourceid) AS ndets
                  FROM (
                     SELECT DISTINCT ON(o.rootid, s.visit) o.rootid, s.band, s.diasourceid
                     FROM diasource s
                     INNER JOIN diaobject o ON s.diaobjectid=o.diaobjectid
                     INNER JOIN base_procver_of_procver j ON s.base_procver_id=j.base_procver_id
                                                         AND j.procver_id={pvid}
                     WHERE s.psfflux >= 5754
                     ORDER BY o.rootid, s.visit, j.priority DESC
                  ) subq
                  GROUP BY rootid, band
               ) n22 ON d0.rootid=n22.rootid AND d0.band=n22.band
               LEFT JOIN (
                  SELECT rootid, band, COUNT(diasourceid) AS ndets
                  FROM (
                     SELECT DISTINCT ON(o.rootid, s.visit) o.rootid, s.band, s.diasourceid
                     FROM diasource s
                     INNER JOIN diaobject o ON s.diaobjectid=o.diaobjectid
                     INNER JOIN base_procver_of_procver j ON s.base_procver_id=j.base_procver_id
                                                         AND j.procver_id={pvid}
                     WHERE s.psfflux >= 14454
                     ORDER BY o.rootid, s.visit, j.priority DESC
                  ) subq
                  GROUP BY rootid, band
               ) n21 ON d0.rootid=n21.rootid AND d0.band=n21.band
               LEFT JOIN (
                  SELECT rootid, band, COUNT(diasourceid) AS ndets
                  FROM (
                     SELECT DISTINCT ON(o.rootid, s.visit) o.rootid, s.band, s.diasourceid
                     FROM diasource s
                     INNER JOIN diaobject o ON s.diaobjectid=o.diaobjectid
                     INNER JOIN base_procver_of_procver j ON s.base_procver_id=j.base_procver_id
                                                         AND j.procver_id={pvid}
                     INNER JOIN processing_version p ON j.procver_id=p.id AND p.description='realtime'
                     WHERE s.psfflux / s.psffluxerr > 10
                     ORDER BY o.rootid, s.visit, j.priority DESC
                  ) subq
                  GROUP BY rootid, band
               ) sn10 ON d0.rootid=sn10.rootid AND d0.band=sn10.band
               LEFT JOIN (
                  SELECT rootid, band, COUNT(diasourceid) AS ndets
                  FROM (
                     SELECT DISTINCT ON(o.rootid, s.visit) o.rootid, s.band, s.diasourceid
                     FROM diasource s
                     INNER JOIN diaobject o ON s.diaobjectid=o.diaobjectid
                     INNER JOIN base_procver_of_procver j ON s.base_procver_id=j.base_procver_id
                                                         AND j.procver_id={pvid}
                     WHERE s.psfflux / s.psffluxerr > 7
                     ORDER BY o.rootid, s.visit, j.priority DESC
                  ) subq
                  GROUP BY rootid, band
               ) sn7 ON d0.rootid=sn7.rootid AND d0.band=sn7.band
               LEFT JOIN (
                  SELECT rootid, band, COUNT(diasourceid) AS ndets
                  FROM (
                     SELECT DISTINCT ON(o.rootid, s.visit) o.rootid, s.band, s.diasourceid
                     FROM diasource s
                     INNER JOIN diaobject o ON s.diaobjectid=o.diaobjectid
                     INNER JOIN base_procver_of_procver j ON s.base_procver_id=j.base_procver_id
                                                         AND j.procver_id={pvid}
                     WHERE s.psfflux / s.psffluxerr > 5
                     ORDER BY o.rootid, s.visit, j.priority DESC
                  ) subq
                  GROUP BY rootid, band
               ) sn5 ON d0.rootid=sn5.rootid AND d0.band=sn5.band
            )
            """
        ) ).format( viewname=sql.Identifier( f'objstats_{procver}' ), pvid=pvid )

        dbcon.execute_nofetch( q, explain=False )

        for col in [ 'rootid', 'band', 'firstdet_mjd', 'lastdet_mjd', 'maxdet_mjd',
                     'firstdet_flux', 'lastdet_flux', 'maxdet_flux',
                     'ndets', 'ndets24', 'ndets23', 'ndets22', 'ndets21', 'nsn10', 'nsn7', 'nsn5' ]:
            q = sql.SQL( 'CREATE INDEX {idxname} ON {viewname}({col})'
                        ).format( idxname=sql.Identifier( f'idx_obstats_{procver}_{col}' ),
                                  viewname=sql.Identifier( f'objstats_{procver}' ),
                                  col=sql.Identifier( col ) )
            dbcon.execute( q )

        q = sql.SQL( 'CREATE INDEX {idxname} ON {viewname}(q3c_ang2ipix(ra, dec))'
                    ).format( idxname=sql.Identifier( f'idx_objstats_{procver}_q3c' ),
                              viewname=sql.Identifier( f'objstats_{procver}' ) )
        dbcon.execute( q, explain=False )

        dbcon.commit()

        FDBLogger.info( f"Done creating materialized view objstats_{procver}" )
