__all__ = [ "object_ltcv", "object_search", "get_hot_ltcvs" ]

import datetime
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


def object_search( processing_version='default', just_objids=False, searchband=None, dbcon=None, **kwargs ):
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

      just_objids : Return a list of root object ids, no other information

      searchband: str or None
         If None, then all the cuts will be for any band.  If a band is
         given, then the cuts will only consider photometry with this
         band.

      SEARCH FIELDS: For each of the following fields, you can have the
         field as is, in which case it will search for the quantity
         equal to the value (which does not make sense for real
         numbers), it can be {field}_max, in which case it will search
         for things where quantity is ≤ the value, or it can be
         {field}_min, in which case... you got it.

      ra: float
      dec: float
      radius: float
         ra and dec _min and _max.  But, also, if you give both,
         and "radius", then it will do a cone search at (ra, dec) in
         radius arcseconds.

      firstdet_mjd: float
         MJD of first detection

      firstdet_flux: float
         Flux (nJy) of first detection

      lastdet_mjd: float
         MJD of last detection

      lastdet_flux: float
         Flux (nJy) of last detection

      maxdet_mjd: float
         MJD of last detection

      maxdet_flux: float
         Flux (nJy) of last detection

      ndets: int
         Number of detections

      ndets24: int
         Number of detections with mag≤24

      ndets23: int
         Number of detections with mag≤23

      ndets22: int
         Number of detections with mag≤22

      ndets21: int
         Number of detections with mag≤21

      nsn10: int
         Number of detections with S/N > 10

      nsn7: int
         Number of detections with S/N > 7

      nsn5: int
         Number of detections with S/N > 5

    Returns
    -------
      dict

      Each key is the column name, and the value is a numpy array with
      column values.  It should be safe to stuff this directly into
      pandas.DataFrame().

    """

    pvobj = db.ProcessingVersion.get_procver( processing_version )
    viewname = f'objstatscomb_{pvobj.description}' if searchband is None else f'objstats_{pvobj.description}'

    searchspec = {
        'rootid':           { 'mult': True,   'substr': False, 'minmax': False, 'dtype': np.dtype('O') },
        'ra':               { 'mult': False,  'sbustr': False, 'minmax': True, 'dtype': np.float64 },
        'dec':              { 'mult': False,  'sbustr': False, 'minmax': True, 'dtype': np.float64 },
        'firstdet_mjd':     { 'mult': False,  'sbustr': False, 'minmax': True, 'dtype': np.float64 },
        'firstdet_flux':    { 'mult': False,  'sbustr': False, 'minmax': True, 'dtype': np.float32 },
        'firstdet_fluxerr': { 'mult': False,  'sbustr': False, 'minmax': True, 'dtype': np.float32 },
        'lastdet_mjd':      { 'mult': False,  'sbustr': False, 'minmax': True, 'dtype': np.float64 },
        'lastdet_flux':     { 'mult': False,  'sbustr': False, 'minmax': True, 'dtype': np.float32 },
        'lastdet_fluxerr':  { 'mult': False,  'sbustr': False, 'minmax': True, 'dtype': np.float32 },
        'maxdet_mjd':       { 'mult': False,  'sbustr': False, 'minmax': True, 'dtype': np.float64 },
        'maxdet_flux':      { 'mult': False,  'sbustr': False, 'minmax': True, 'dtype': np.float32 },
        'maxdet_fluxerr':   { 'mult': False,  'sbustr': False, 'minmax': True, 'dtype': np.int16 },
        'ndets':            { 'mult': False,  'sbustr': False, 'minmax': True, 'dtype': np.int16 },
        'ndets24':          { 'mult': False,  'sbustr': False, 'minmax': True, 'dtype': np.int16 },
        'ndets23':          { 'mult': False,  'sbustr': False, 'minmax': True, 'dtype': np.int16 },
        'ndets22':          { 'mult': False,  'sbustr': False, 'minmax': True, 'dtype': np.int16 },
        'ndets21':          { 'mult': False,  'sbustr': False, 'minmax': True, 'dtype': np.int16 },
        'nsn10':            { 'mult': False,  'sbustr': False, 'minmax': True, 'dtype': np.int16 },
        'nsn7':             { 'mult': False,  'sbustr': False, 'minmax': True, 'dtype': np.int16 },
        'nsn5':             { 'mult': False,  'sbustr': False, 'minmax': True, 'dtype': np.int16 },
    }

    radius = None
    if 'radius' in kwargs:
        if ( 'ra' not in kwargs ) or ( 'dec' not in kwargs ):
            raise ValueError( "radius requires both ra and dec" )
        ra = kwargs['ra']
        dec = kwargs['dec']
        radius = kwargs['radius']
        del kwargs['ra']
        del kwargs['dec']
        del kwargs['radius']

    with db.DBCon( dbcon ) as dbcon:
        rows, _cols = dbcon.execute( sql.SQL( "SELECT * FROM pg_class WHERE relname={viewname}" )
                                     .format( viewname=viewname ) )
        if len( rows ) == 0:
            raise RuntimeError( f"Can't do object search, materialized view {viewname} doesn't exist" )

        q = sql.SQL( "SELECT * FROM {viewname} " ).format( viewname=sql.Identifier(viewname) )
        where = "WHERE"
        if searchband is not None:
            q += sql.SQL( "WHERE band={band}" ).format( searchband )
            where = " AND"

        qwhere, subdict, remainder, where = db.construct_pgsql_where_clause( searchspec, where=where, **kwargs )
        if len(remainder) > 0:
            raise ValueError( f"Unknown arguments: {remainder}" )

        q += qwhere

        if radius is not None:
            q += sql.SQL( "{where} q3c_radial_query(ra, dec, {ra}, {dec}, {radius}"
                         ).format( where=sql.SQL(where), ra=ra, dec=dec, radius=radius/3600. )

        FDBLogger.debug( "Starting object search query..." )
        barf = "".join( random.choices( "abcdefghijklmnopqrstuvwxyz", k=6 ) )
        cursor = dbcon.execute_nofetch( q, subdict, cursorname=f'object_search_{barf}' )
        cursor.itersize = 1000
        FDBLogger.debug( "...fetching results from postgres..." )
        cols = [ desc[0] for desc in cursor.description ]

        # nrows = cursor.rowcount
        # GAH... it doesn't know it until after all of the fetch
        # if nrows < 0:
        #     import pdb; pdb.set_trace()
        #     pass
        # rval = { c: np.empty( (nrows,), dtype=searchspec[c]['dtype'] ) for c in cols }
        # for rown, row in enumerate( cursor ):
        #     for i, c in enumerate( cols ):
        #         rval[c][rown] = row[i]

        rval = { c: [] for c in cols }
        for row in cursor:
            for i, c in enumerate( cols ):
                rval[c].append( row[i] )

        cursor.close()
        FDBLogger.debug( "...done with object search query." )

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
            expectedcols = { 'rootid', 'band', 'ra', 'dec', 'firstdet_mjd', 'firstdet_flux', 'firstdet_fluxerr',
                   'lastdet_mjd', 'lastdet_flux', 'lastdet_fluxerr', 'maxdet_mjd', 'maxdet_flux', 'maxdet_fluxerr',
                   'ndets', 'ndets24', 'ndets23', 'ndets22', 'ndets21', 'nsn10', 'nsn7', 'nsn5' }
            if set( r['attname'] for r in rows ) != expectedcols:
                raise RuntimeError( f"postgres view objstats_{procver} has the wrong set of columns" )

            q = sql.SQL( "SELECT * FROM pg_class WHERE relname={viewname}"
                        ).format( viewname=f'objstatscomb_{procver}' )
            rows = dbcon.execute( q )
            if len(rows) == 0:
                raise RuntimeError( f"view objstats_{procver} exists, but objstatscomb_{procver} does not" )
            row = rows[0]
            if row['relkind'] != 'm':
                raise RuntimeError( f"postgres class objstatscomb_{procver} exists, but is not a materialized "
                                    f"view!  (It is a \"{row['relkind']}\")" )
            q = sql.SQL( "SELECT a.attname FROM pg_catalog.pg_attribute a "
                         "INNER JOIN pg_class c ON c.oid=a.attrelid "
                         "WHERE c.relname={viewname} AND a.attnum>0"
                        ).format( viewname=f'objstatscomb_{procver}' )
            rows = dbcon.execute( q )
            if set( r['attname'] for r in rows ) != ( expectedcols - { 'band' } ):
                raise RuntimeError( f"postgrew view objstatscomb_{procver} has the wrong set of columns" )


            FDBLogger.info( f"Refreshing materizalized view objstats_{procver}" )
            q = sql.SQL( "REFRESH MATERIALIZED VIEW {viewname}"
                        ).format( viewname=sql.Identifier( f'objstats_{procver}' ) )
            dbcon.execute( q )
            FDBLogger.info( f"Refreshing materizalized view objstatscomb_{procver}" )
            q = sql.SQL( "REFRESH MATERIALIZED VIEW {viewname}"
                        ).format( viewname=sql.Identifier( f'objstatscomb_{procver}' ) )
            dbcon.execute( q )
            dbcon.commit()
            FDBLogger.info( f"Done refreshing materialized views for {procver}" )
            return

        # If we get here, the materialized view does not exist
        #
        # Note: there are hardcoded flux numbers below.
        #   For zeropoint = 31.4,
        #     m = 24 : f =   912
        #     m = 23 : f =  2291
        #     m = 22 : f =  5754
        #     m = 21 : f = 14454

        FDBLogger.info( f"Creating materialized view objstats_{procver}" )
        pvid = db.ProcessingVersion.procver_id( procver, dbcon=dbcon )

        q = sql.SQL( textwrap.dedent(
            """
            CREATE MATERIALIZED VIEW {viewname} AS (
               SELECT r.id AS rootid, d0.band AS band, r.ra AS ra, r.dec AS dec,
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
               FROM root_diaobject r
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
                  ORDER BY rootid, band, midpointmjdtai
               ) d0 ON d0.rootid=r.id
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
                     SELECT DISTINCT ON(o.rootid, s.visit) o.rootid, s.band, s.diasourceid, s.psfflux
                     FROM diasource s
                     INNER JOIN diaobject o ON s.diaobjectid=o.diaobjectid
                     INNER JOIN base_procver_of_procver j ON s.base_procver_id=j.base_procver_id
                                                         AND j.procver_id={pvid}
                     ORDER BY o.rootid, s.visit, j.priority DESC
                  ) subq
                  WHERE psfflux >= 912
                  GROUP BY rootid, band
               ) n24 ON d0.rootid=n24.rootid AND d0.band=n24.band
               LEFT JOIN (
                  SELECT rootid, band, COUNT(diasourceid) AS ndets
                  FROM (
                     SELECT DISTINCT ON(o.rootid, s.visit) o.rootid, s.band, s.diasourceid, s.psfflux
                     FROM diasource s
                     INNER JOIN diaobject o ON s.diaobjectid=o.diaobjectid
                     INNER JOIN base_procver_of_procver j ON s.base_procver_id=j.base_procver_id
                                                         AND j.procver_id={pvid}
                     ORDER BY o.rootid, s.visit, j.priority DESC
                  ) subq
                  WHERE psfflux >= 2291
                  GROUP BY rootid, band
               ) n23 ON d0.rootid=n23.rootid AND d0.band=n23.band
               LEFT JOIN (
                  SELECT rootid, band, COUNT(diasourceid) AS ndets
                  FROM (
                     SELECT DISTINCT ON(o.rootid, s.visit) o.rootid, s.band, s.diasourceid, s.psfflux
                     FROM diasource s
                     INNER JOIN diaobject o ON s.diaobjectid=o.diaobjectid
                     INNER JOIN base_procver_of_procver j ON s.base_procver_id=j.base_procver_id
                                                         AND j.procver_id={pvid}
                     ORDER BY o.rootid, s.visit, j.priority DESC
                  ) subq
                  WHERE psfflux >= 5754
                  GROUP BY rootid, band
               ) n22 ON d0.rootid=n22.rootid AND d0.band=n22.band
               LEFT JOIN (
                  SELECT rootid, band, COUNT(diasourceid) AS ndets
                  FROM (
                     SELECT DISTINCT ON(o.rootid, s.visit) o.rootid, s.band, s.diasourceid, s.psfflux
                     FROM diasource s
                     INNER JOIN diaobject o ON s.diaobjectid=o.diaobjectid
                     INNER JOIN base_procver_of_procver j ON s.base_procver_id=j.base_procver_id
                                                         AND j.procver_id={pvid}
                     ORDER BY o.rootid, s.visit, j.priority DESC
                  ) subq
                  WHERE psfflux >= 14454
                  GROUP BY rootid, band
               ) n21 ON d0.rootid=n21.rootid AND d0.band=n21.band
               LEFT JOIN (
                  SELECT rootid, band, COUNT(diasourceid) AS ndets
                  FROM (
                     SELECT DISTINCT ON(o.rootid, s.visit) o.rootid, s.band, s.diasourceid, s.psfflux, s.psffluxerr
                     FROM diasource s
                     INNER JOIN diaobject o ON s.diaobjectid=o.diaobjectid
                     INNER JOIN base_procver_of_procver j ON s.base_procver_id=j.base_procver_id
                                                         AND j.procver_id={pvid}
                     ORDER BY o.rootid, s.visit, j.priority DESC
                  ) subq
                  WHERE psfflux / psffluxerr >= 10
                  GROUP BY rootid, band
               ) sn10 ON d0.rootid=sn10.rootid AND d0.band=sn10.band
               LEFT JOIN (
                  SELECT rootid, band, COUNT(diasourceid) AS ndets
                  FROM (
                     SELECT DISTINCT ON(o.rootid, s.visit) o.rootid, s.band, s.diasourceid, s.psfflux, s.psffluxerr
                     FROM diasource s
                     INNER JOIN diaobject o ON s.diaobjectid=o.diaobjectid
                     INNER JOIN base_procver_of_procver j ON s.base_procver_id=j.base_procver_id
                                                         AND j.procver_id={pvid}
                     ORDER BY o.rootid, s.visit, j.priority DESC
                  ) subq
                  WHERE psfflux / psffluxerr >= 7
                  GROUP BY rootid, band
               ) sn7 ON d0.rootid=sn7.rootid AND d0.band=sn7.band
               LEFT JOIN (
                  SELECT rootid, band, COUNT(diasourceid) AS ndets
                  FROM (
                     SELECT DISTINCT ON(o.rootid, s.visit) o.rootid, s.band, s.diasourceid, s.psfflux, s.psffluxerr
                     FROM diasource s
                     INNER JOIN diaobject o ON s.diaobjectid=o.diaobjectid
                     INNER JOIN base_procver_of_procver j ON s.base_procver_id=j.base_procver_id
                                                         AND j.procver_id={pvid}
                     ORDER BY o.rootid, s.visit, j.priority DESC
                  ) subq
                  WHERE psfflux / psffluxerr >= 5
                  GROUP BY rootid, band
               ) sn5 ON d0.rootid=sn5.rootid AND d0.band=sn5.band
            )
            """
        ) ).format( viewname=sql.Identifier( f'objstats_{procver}' ), pvid=pvid )

        dbcon.execute_nofetch( q, explain=False )

        indexcols = [ 'rootid', 'firstdet_mjd', 'lastdet_mjd', 'maxdet_mjd',
                     'firstdet_flux', 'lastdet_flux', 'maxdet_flux',
                     'ndets', 'ndets24', 'ndets23', 'ndets22', 'ndets21', 'nsn10', 'nsn7', 'nsn5' ]
        for col in indexcols:
            q = sql.SQL( 'CREATE INDEX {idxname} ON {viewname}({col})'
                        ).format( idxname=sql.Identifier( f'idx_obstats_{procver}_{col}' ),
                                  viewname=sql.Identifier( f'objstats_{procver}' ),
                                  col=sql.Identifier( col ) )
            dbcon.execute( q, explain=False )

        q = sql.SQL( 'CREATE INDEX {idxname} ON {viewname}(band)',
                    ).format( idxname=sql.Identifier( f'idx_obstats_{procver}_band' ),
                              viewname=sql.Identifier( f'objstats_{procver}' ) )

        q = sql.SQL( 'CREATE INDEX {idxname} ON {viewname}(q3c_ang2ipix(ra, dec))'
                    ).format( idxname=sql.Identifier( f'idx_objstats_{procver}_q3c' ),
                              viewname=sql.Identifier( f'objstats_{procver}' ) )
        dbcon.execute( q, explain=False )

        # Now create the view that combines all the bands together
        q = sql.SQL( textwrap.dedent(
            """
            CREATE MATERIALIZED VIEW {combviewname} AS (
              SELECT s.rootid, s.ra, s.dec,
                     fd.mjd AS firstdet_mjd, fd.flux AS firstdet_flux, fd.fluxerr AS firstdet_fluxerr,
                     ld.mjd AS lastdet_mjd, ld.flux AS lastdet_flux, ld.fluxerr AS lastdet_fluxerr,
                     xd.mjd AS maxdet_mjd, xd.flux AS maxdet_flux, xd.fluxerr AS maxdet_fluxerr,
                     s.ndets AS ndets, s.ndets24 AS ndets24, s.ndets23 AS ndets23, s.ndets22 AS ndets22,
                     s.ndets21 AS ndets21, s.nsn10 AS nsn10, s.nsn7 AS nsn7, s.nsn5 AS nsn5
              FROM (
                SELECT rootid, ra, dec, SUM(ndets) AS ndets, SUM(ndets24) AS ndets24, SUM(ndets23) AS ndets23,
                       SUM(ndets22) AS ndets22, SUM(ndets21) AS ndets21, SUM(nsn10) AS nsn10,
                       SUM(nsn7) AS nsn7, SUM(nsn5) AS nsn5
                FROM {viewname}
                GROUP BY rootid, ra, dec
              ) s
              INNER JOIN (
                SELECT DISTINCT ON(rootid) rootid, firstdet_mjd AS mjd, firstdet_flux AS flux,
                                           firstdet_fluxerr AS fluxerr
                FROM {viewname}
                ORDER BY rootid, firstdet_mjd
              ) fd ON s.rootid=fd.rootid
              INNER JOIN (
                SELECT DISTINCT ON(rootid) rootid, lastdet_mjd AS mjd, lastdet_flux AS flux, lastdet_fluxerr AS fluxerr
                FROM {viewname}
                ORDER BY rootid, lastdet_mjd DESC
              ) ld ON s.rootid=ld.rootid
              INNER JOIN (
                SELECT DISTINCT ON(rootid) rootid, maxdet_mjd AS mjd, maxdet_flux AS flux, maxdet_fluxerr AS fluxerr
                FROM {viewname}
                ORDER BY rootid, maxdet_flux DESC
              ) xd ON s.rootid=xd.rootid
            )
            """ ) ).format( viewname=sql.Identifier(f'objstats_{procver}'),
                            combviewname=sql.Identifier(f'objstatscomb_{procver}') )
        dbcon.execute( q, explain=False )

        for col in indexcols:
            q = sql.SQL( 'CREATE INDEX {idxname} ON {viewname}({col})'
                        ).format( idxname=sql.Identifier( f'idx_obstatscomb_{procver}_{col}' ),
                                  viewname=sql.Identifier( f'objstatscomb_{procver}' ),
                                  col=sql.Identifier( col ) )
            dbcon.execute( q, explain=False )

        q = sql.SQL( 'CREATE INDEX {idxname} ON {viewname}(q3c_ang2ipix(ra, dec))'
                    ).format( idxname=sql.Identifier( f'idx_objstatscomb_{procver}_q3c' ),
                              viewname=sql.Identifier( f'objstatscomb_{procver}' ) )
        dbcon.execute( q, explain=False )

        dbcon.commit()
        FDBLogger.info( f"Done creating materialized view objstats_{procver}" )
