import datetime
import numbers
import json   # noqa: F401

import numpy
import pandas
import astropy.time

import db
import util


def procver_int( processing_version, dbcon=None ):
    if isinstance( processing_version, numbers.Integral ):
        return processing_version
    try:
        ipv = int( processing_version )
        return ipv
    except Exception:
        pass
    with db.DB( dbcon ) as con:
        cursor = con.cursor()
        cursor.execute( "SELECT id FROM processing_version WHERE description=%(pv)s", { 'pv': processing_version } )
        row = cursor.fetchone()
        if row is not None:
            return row[0]
        cursor.execute( "SELECT id FROM processing_version_alias WHERE description=%(pv)s",
                        { 'pv': processing_version } )
        row = cursor.fetchone()
        if row is None:
            raise ValueError( f"Unknown processing version {processing_version}" )
        return row[0]


def object_ltcv( processing_version, diaobjectid, return_format='json', bands=None, which='patch', dbcon=None ):
    """Get the lightcurve for an object

    Parameters
    ----------
       processing_version : int or str
          The processing verson (or alias) to search

       diaobjectid : int
          The object id

       return_format : str, default 'json'
          'json' or 'pandas'

       bands : list of str or None
          If not None, only include the bands in this list.

       which : str, default 'patch'
          forced : get forced photometry (i.e. diaforcedsource)
          detections : get detections (i.e. diasource)
          patch : get forced photometry, but patch in detections where forced photometry is missing

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

    if which not in ( 'detections', 'forced', 'patch' ):
        raise ValueError( f"Unknown value of which for object_ltcv: {which}" )

    if return_format not in ( 'json', 'pandas' ):
        raise ValueError( f"Unknown return_format {return_format}" )

    # Just pull down all sources and forced sources, and do
    # post-processing in python.  This is going to be hundreds or
    # thousands of points for a single object, so it's not really
    # necessary to try to do lots of processing SQL-side to filter stuff
    # out like we do in get_hot_ltcvs.
    sources = []
    forced = []
    with db.DB( dbcon ) as dbcon:
        pv = procver_int( processing_version, dbcon=dbcon )
        cursor = dbcon.cursor()
        q =  ( "SELECT midpointmjdtai AS mjd, band, psfflux, psffluxerr "
               "FROM diasource "
               "WHERE diaobjectid=%(id)s AND processing_version=%(pv)s " )
        if bands is not None:
            q += "AND band=ANY(%(bands)s) "
        q += "ORDER BY mjd "
        cursor.execute( q, { 'id': diaobjectid, 'pv': pv, 'bands': bands } )
        columns = [ d[0] for d in cursor.description ]
        sources = pandas.DataFrame( cursor.fetchall(), columns=columns )
        if which in ( 'forced', 'patch' ):
            q = ( "SELECT midpointmjdtai AS mjd, band, psfflux, psffluxerr "
                  "FROM diaforcedsource "
                  "WHERE diaobjectid=%(id)s AND processing_version=%(pv)s " )
            if bands is not None:
                q += "AND band=ANY(%(bands)s) "
            q += "ORDER BY mjd "
            cursor.execute( q, { 'id': diaobjectid, 'pv': pv, 'bands': bands } )
            columns = [ d[0] for d in cursor.description ]
            forced = pandas.DataFrame( cursor.fetchall(), columns=columns )

    if which == 'detections':
        # If we're only asking for detections, this is easy
        retframe = sources
        retframe['isdet'] = True

    else:
        # Otherwise, we have to think a lot.  We need to find
        # corresponences between forced points and source points.  I
        # don't trust the floating-point MJDs from corresponding rows of
        # the two tables to be identical (because you should never trust
        # floating point numbers to be identical), so multiply them by
        # 10⁴ and convert to bigints for matching purposes.  That gives
        # a resolution of ~10 seconds; we're making the implicit
        # assumption that no two exposures will have been taken less
        # than 10 seconds apart.  Stick my head in the sand re: the edge
        # case of two corresponding things flooring in different
        # directions because of floating point underflow.  Going to 4
        # decimal places = 10 digits of precision (for 5 digits in the
        # intger part of MJD), and doubles have 15 or 16 digits of
        # precision, so presumably we're OK, but there may still be a
        # rare 99999 vs 000000 edge case.
        #
        # FUTURE NOTE : we should probably filter on visit and detector
        # with actual LSST data!  Presumably those will be properly
        # unique.  And, I think I populate those with SNANA, so maybe
        # we should just do that now.  That would save us from thinking
        # about anything floating point for joining.

        forced['mjde4'] = ( forced.mjd * 10000 ).astype( numpy.int64 )
        sources['mjde4'] = ( sources.mjd * 10000 ).astype( numpy.int64 )
        forced.set_index( [ 'mjde4', 'band' ], inplace=True )
        sources.set_index( [ 'mjde4', 'band' ], inplace=True )
        joined = forced.join( sources, on=[ 'mjde4', 'band' ], how='outer',
                              lsuffix='_f', rsuffix='_s' ).reset_index()
        joined['isdet'] = ~joined.mjd_s.isna()
        joined['ispatch'] = joined.mjd_f.isna()

        if which == 'patch':
            # Patch in the detections where there is no forced photometry
            # (Pandas is mysterious; have to use ".loc" to set columns, can't
            # use the simpler indexing you'd use to read columns.)
            joined.loc[ joined['ispatch'], 'mjd_f' ] = joined[ joined['ispatch'] ].mjd_s
            joined.loc[ joined['ispatch'], 'psfflux_f' ] = joined[ joined['ispatch'] ].psfflux_s
            joined.loc[ joined['ispatch'], 'psffluxerr_f' ] = joined[ joined['ispatch'] ].psffluxerr_s

        # Remove columns that don't have forced or patched photometry
        joined = joined[ ~joined.mjd_f.isna() ]

        # Remove columns we don't want to return
        joined.drop( columns=[ 'mjd_s', 'psfflux_s', 'psffluxerr_s', 'mjde4' ], inplace=True )
        joined.rename( inplace=True, columns={ 'mjd_f': 'mjd',
                                               'psfflux_f': 'psfflux',
                                               'psffluxerr_f': 'psffluxerr' } )
        retframe = joined

    retframe.sort_values( [ 'mjd', 'band' ], inplace=True )
    if return_format == 'pandas':
        return retframe
    elif return_format == 'json':
        retval = { c: list( joined[c].values ) for c in joined.columns }
        # Gotta de-bool the bool columns since JSON, sadly, can't handle it
        retval['isdet'] = [ 1 if r else 0 for r in retval['isdet'] ]
        if ( 'ispatch' in retval ):
            retval['ispatch'] = [ 1 if r else 0 for r in retval['ispatch'] ]
        return retval
    else:
        raise RuntimeError( "This should never happen." )


def debug_count_temp_table( con, table ):
    res = con.execute( f"SELECT COUNT(*) FROM {table}" )
    util.logger.debug( f"Table {table} has {res[0][0]} rows" )


def object_search( processing_version, return_format='json', just_objids=False, **kwargs ):
    """Search for objects.

    For parameters that define the search, if they are None, they are
    not considered in the search.  (I.e. that filter will be skipped.)

    Parameters
    ----------
      processing_version : string or int
         The processing version you're looking at

      return format : string
         Either "json" or "pandas".  (TODO: pyarrow? polars?)

      just_objids : bool, default False
         See "Returns" below.

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

      # relwindow_t0, relwindow_t1 : float, default None
      #    NOT IMPLEMENTED.  Intended to be a time window around maximum-flux detection.


      mint_firsdtdetection : float, default None
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
      #    Only return objects that have been detected in at least this many different  bnads.
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

         This parameter also effects what is included in the returned
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
                  'window_t0', 'window_t1', 'min_window_numdetections',
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

    # Parse out statbands, allowing either a single string or a list of strings
    statbands = None
    if 'statbands' in kwargs:
        if ( ( ( isinstance( kwargs['statbands'], str ) ) and ( len(kwargs['statbands'].strip()) == 0 ) )
             or
             ( ( isinstance( kwargs['statbands'], list ) ) and ( len(kwargs['statbands']) == 0 ) ) ):
            statbands = None
        else:
            if isinstance( kwargs['statbands'], list ):
                statbands = kwargs['statbands']
            else:
                statbands = [ kwargs['statbands'] ]
            if not all( isinstance(b, str) for b in statbands ):
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

    mint_firstdetection = util.float_or_none_from_dict( kwargs, 'mint_firstdetection' )
    maxt_firstdetection = util.float_or_none_from_dict( kwargs, 'maxt_firstdetection' )
    minmag_firstdetection = util.float_or_none_from_dict( kwargs, 'minmag_firstdetection' )
    maxmag_firstdetection = util.float_or_none_from_dict( kwargs, 'maxmag_firstdetection' )

    mint_lastdetection = util.float_or_none_from_dict( kwargs, 'mint_lastdetection' )
    maxt_lastdetection = util.float_or_none_from_dict( kwargs, 'maxt_lastdetection' )
    minmag_lastdetection = util.float_or_none_from_dict( kwargs, 'minmag_lastdetection' )
    maxmag_lastdetection = util.float_or_none_from_dict( kwargs, 'maxmag_lastdetection' )

    mint_maxdetection = util.float_or_none_from_dict( kwargs, 'mint_maxdetection' )
    maxt_maxdetection = util.float_or_none_from_dict( kwargs, 'maxt_maxdetection' )
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


    with db.DBCon() as con:
        # Figure out processing version
        try:
            procver = int( processing_version)
        except Exception:
            rows, _ = con.execute( "SELECT id FROM processing_version WHERE description=%(procver)s",
                                   { 'procver': processing_version } )
            if len(rows) == 0:
                rows, _ = con.execute( "SELECT id FROM processing_version_alias WHERE description=%(procver)s",
                                       { 'procver': processing_version } )
                if len(rows) == 0:
                    raise ValueError( f"Unknown processing version {processing_version}" )
            procver = rows[0][0]


        # Search criteria consistency checks
        if ( any( [ ( ra is None ), ( dec is None ), ( radius is None ) ] )
             and not all( [ ( ra is None ), ( dec is None ), ( radius is None ) ] ) ):
            raise ValueError( "Must give either all or none of ra dec, radius, not just one or two" )

        if ( window_t0 is None ) != ( window_t1 is None ):
            raise ValueError( "Must give both or neither of window_t0, window_t1, not just one" )
        if ( min_window_numdetections is not None ) and ( window_t0 is None ):
            raise ValueError( "min_window_numdetections requires window_t0 and window_t1" )
        if ( ( window_t0 is not None ) and ( maxt_lastdetection is not None )
             and ( maxt_lastdetection < window_t0 ) ):
            raise ValueError( f"window_t0={window_t0} and maxt_lastdetection={maxt_lastdetection} inconsistent" )
        if ( ( window_t1 is not None ) and ( mint_firstdetection is not None )
             and ( mint_firstdetection > window_t1 ) ):
            raise ValueError( f"window_t1={window_t1} and mint_firstdetection={mint_firstdetection} inconsistent" )

        # TODO : compare max detection time to first and last detection time

        nexttable = 'diaobject'

        # Filter by ra and dec if given
        if ra is not None:
            radius = util.float_or_none_from_dict( kwargs, 'radius' )
            radius = radius if radius is not None else 10.
            q = ( "SELECT diaobjectid,ra,dec INTO TEMP TABLE objsearch_radeccut\n"
                  "FROM diaobject\n"
                  "WHERE processing_version=%(pv)s\n"
                  "AND q3c_radial_query( ra, dec, %(ra)s, %(dec)s, %(rad)s )" )
            subdict = { 'pv': procver, 'ra': ra, 'dec': dec, 'rad': radius/3600. }
            util.logger.debug( f"Sending query: {q} with subdict {subdict}" )
            con.execute_nofetch( q, subdict )
            debug_count_temp_table( con, 'objsearch_radeccut' )
            nexttable = 'objsearch_radeccut'

        # Count (and maybe filter) by number of detections within the time window
        # ROB TODO : use processing version index
        if window_t0 is not None:
            if nexttable != 'diaobject':
                # Make a primary key so we can group by
                con.execute_nofetch( f"ALTER TABLE {nexttable} ADD PRIMARY KEY (diaobjectid)" )
            subdict = { 'pv': procver, 't0': window_t0, 't1': window_t1 }
            q = ( f"/*+ IndexScan(s idx_diasource_diaobjectid) */\n"
                  f"SELECT * INTO TEMP TABLE objsearch_windowdet FROM (\n"
                  f"  SELECT o.diaobjectid, o.ra, o.dec, COUNT(s.midpointmjdtai) AS numdetinwindow\n"
                  f"  FROM {nexttable} o\n"
                  f"  INNER JOIN diasource s ON o.diaobjectid=s.diaobjectid\n"
                  f"         AND s.processing_version=%(pv)s\n"
                  f"  WHERE s.midpointmjdtai>=%(t0)s AND s.midpointmjdtai<=%(t1)s\n" )
            if statbands is not None:
                q += "     AND s.band=ANY(%(bands)s)\n"
            q += ( "  GROUP BY o.diaobjectid, o.ra, o.dec\n"
                   ") subq\n" )
            if min_window_numdetections is not None:
                q += "WHERE numdetinwindow>=%(n)s\n"
                subdict['n'] = min_window_numdetections
            con.execute_nofetch( q, subdict )
            debug_count_temp_table( con, 'objsearch_windowdet' )
            nexttable = 'objsearch_windowdet'


        # First pass cut that has any detection with (min(minfirst,minlast) < t < max(maxfirst,maxlast)
        #   to try to cut down the total size of stuff to think about in our next big join
        # TODO : also thing about adding magnitude cuts here!  May not
        #   be worth it since we don't have indexes on fluxes.  (Maybe we should?)
        if any( i is not None for i in [ mint_firstdetection, maxt_firstdetection,
                                         mint_lastdetection, maxt_lastdetection ] ):
            if ( ( maxt_lastdetection is not None ) and ( mint_firstdetection is not None ) and
                 ( mint_firstdetection < maxt_lastdetection ) ):
                raise RuntimeError( "maxt_lastdetection > mint_firstdetection, which makes no sense." )
            subdict = { 'pv': procver }
            q = ( f"/*+ IndexScan(s idx_diasource_diaobjectid) */\n"
                  f"SELECT * INTO TEMP TABLE objsearch_detcut FROM (\n"
                  f"  SELECT DISTINCT ON (o.diaobjectid) o.diaobjectid,o.ra,o.dec FROM {nexttable} o\n"
                  f"  INNER JOIN diasource s ON s.diaobjectid=o.diaobjectid\n"
                  f"                        AND s.processing_version=%(pv)s\n" )
            if ( mint_firstdetection is not None ) or ( mint_lastdetection is not None ):
                q += "    AND midpointmjdtai>=%(mint)s\n"
                subdict['mint'] = ( mint_firstdetection if mint_lastdetection is None
                                    else mint_lastdetection if mint_firstdetection is None
                                    else min( mint_firstdetection, mint_lastdetection ) )
            if ( maxt_firstdetection is not None ) or ( maxt_lastdetection is not None ):
                q += "    AND midpointmjdtai<=%(maxt)s\n"
                subdict['maxt'] = ( maxt_firstdetection if maxt_lastdetection is None
                                    else maxt_lastdetection if maxt_firstdetection is None
                                    else max( maxt_firstdetection, maxt_lastdetection ) )
            if statbands is not None:
                q += "    AND s.band=ANY(%(bands)s) "
                subdict['bands'] = statbands
            q += ") subq"
            con.execute_nofetch( q, subdict )
            debug_count_temp_table( con, 'objsearch_detcut' )
            nexttable = "objsearch_detcut"

        # Make a temp table that has number of detections, and first, last, and max detections
        # NOTE.  We're being cavalier here with INNER JOIN.  The assumption is that
        #   there will ALWAYS be at least one diasource for any diaobject, otherwise
        #   the diaobject would never have been defined in the first place.
        # TODO THINK : what about when statbands is given?  ROB THINK A LOT.
        subdict = { 'pv': procver }
        q = ( f"/*+ IndexScan(s idx_diasource_diaobjectid) */\n"
              f"SELECT * INTO TEMP TABLE objsearch_stattab FROM (\n"
              f"  SELECT DISTINCT ON (diaobjectid)\n"
              f"         o.*, NULL::integer as numdet,\n"
              f"         s.midpointmjdtai AS firstdetmjd, s.band AS firstdetband,\n"
              f"         s.psfflux AS firstdetflux, s.psffluxerr AS firstdetfluxerr,\n"
              f"         NULL::double precision as lastdetmjd, NULL::text as lastdetband,\n"
              f"         NULL::double precision as lastdetflux, NULL::double precision as lastdetfluxerr,\n"
              f"         NULL::double precision as maxdetmjd, NULL::text as maxdetband,\n"
              f"         NULL::double precision as maxdetflux, NULL::double precision as maxdetfluxerr\n"
              f"  FROM {nexttable} o\n"
              f"  INNER JOIN diasource s ON s.diaobjectid=o.diaobjectid\n"
              f"                        AND s.processing_version=%(pv)s\n" )
        if statbands is not None:
            subdict['bands'] = statbands
            q += ( "  WHERE s.band=ANY(%(bands)s)\n"
                   "  ORDER BY o.diaobjectid, s.midpointmjdtai\n" )
        q += ") subq"
        con.execute_nofetch( q, subdict )
        q = ( f"/*+ IndexScan(s idx_diasource_diaobjectid) */\n"
              f"UPDATE objsearch_stattab o\n"
              f"SET lastdetmjd=midpointmjdtai, lastdetband=band,\n"
              f"    lastdetflux=psfflux, lastdetfluxerr=psffluxerr\n"
              f"FROM ( SELECT DISTINCT ON (o.diaobjectid) o.diaobjectid,\n"
              f"                                          s.midpointmjdtai, s.band,\n"
              f"                                          s.psfflux, s.psffluxerr\n"
              f"       FROM {nexttable} o\n"
              f"       INNER JOIN diasource s ON s.diaobjectid=o.diaobjectid\n"
              f"                             AND s.processing_version=%(pv)s\n" )
        if statbands is not None:
            q += "               AND s.band=ANY(%(bands)s)\n"
        q += ( "      ORDER BY o.diaobjectid, s.midpointmjdtai DESC\n"
               "     ) subq\n"
               "WHERE subq.diaobjectid=o.diaobjectid" )
        con.execute_nofetch( q, subdict )
        q = ( f"/*+ IndexScan(s idx_diasource_diobjectid) */\n"
              f"UPDATE objsearch_stattab o\n"
              f"SET maxdetmjd=midpointmjdtai, maxdetband=band,\n"
              f"    maxdetflux=psfflux, maxdetfluxerr=psffluxerr\n"
              f"FROM ( SELECT DISTINCT ON (o.diaobjectid) o.diaobjectid,\n"
              f"                                          s.midpointmjdtai, s.band,\n"
              f"                                          s.psfflux, s.psffluxerr\n"
              f"       FROM {nexttable} o\n"
              f"       INNER JOIN diasource s ON s.diaobjectid=o.diaobjectid\n"
              f"                             AND s.processing_version=%(pv)s\n" )
        if statbands is not None:
            q += "               AND s.band=ANY(%(bands)s)\n"
        q += ( "       ORDER BY o.diaobjectid, s.psfflux DESC\n"
               "      ) subq\n"
               "WHERE subq.diaobjectid=o.diaobjectid" )
        con.execute_nofetch( q, subdict )
        q = ( f"/*+ IndexScan(s idx_diasource_diaobjectid) */\n"
              f"UPDATE objsearch_stattab o\n"
              f"SET numdet=n "
              f"FROM ( SELECT o.diaobjectid, COUNT(s.midpointmjdtai) AS n\n"
              f"       FROM {nexttable} o\n"
              f"       INNER JOIN diasource s ON s.diaobjectid=o.diaobjectid\n"
              f"                             AND s.processing_version=%(pv)s\n" )
        if statbands is not None:
            q += "               AND s.band=ANY(%(bands)s)\n"
        q += ( "       GROUP BY o.diaobjectid "
               "     ) subq\n"
               "WHERE subq.diaobjectid=o.diaobjectid" )
        con.execute_nofetch( q, subdict )
        debug_count_temp_table( con, 'objsearch_stattab' )

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

        # Get the last forced source
        # For some reason, Postgres was deciding not to use the index on this next query, which
        #   raised the runtime by two orders of magnitude.  Hint fixed it.
        subdict = { 'pv': procver }
        q = ( f"/*+ IndexScan(f idx_diaforcedsource_diaobjectid ) */\n"
              f"SELECT * INTO TEMP TABLE objsearch_final FROM (\n"
              f"  SELECT DISTINCT ON (t.diaobjectid) t.*,\n"
              f"      f.psfflux AS lastforcedflux, f.psffluxerr AS lastforcedfluxerr,\n"
              f"      f.midpointmjdtai AS lastforcedmjd, f.band AS lastforcedband\n"
              f"  FROM {nexttable} t\n"
              f"  INNER JOIN diaforcedsource f ON t.diaobjectid=f.diaobjectid AND f.processing_version=%(pv)s\n" )
        if statbands is not None:
            q += "  WHERE f.band=ANY(%(bands)s)\n"
            subdict['bands'] = statbands
        q += ( "  ORDER BY t.diaobjectid, f.midpointmjdtai DESC\n"
               ") subq" )
        con.execute_nofetch( q, subdict )
        debug_count_temp_table( con, 'objsearch_final' )

        # Filter baesd on last magnitude
        if min_lastmag is not None:
            con.execute_nofetch( "DELETE FROM objsearch_final WHERE lastforcedflux>%(f)s",
                                 { 'f': 10**((min_lastmag-zp)/-2.5) } )
            debug_count_temp_table( con, 'objsearch_final' )
        if max_lastmag is not None:
            con.execute_nofetch( "DELETE FROM objsearch_final WHERE lastforcedflux<%(f)s",
                                 { 'f': 10**((max_lastmag-zp)/-2.5) } )
            debug_count_temp_table( con, 'objsearch_final' )

        # Pull down the results
        rows, columns = con.execute( "SELECT * FROM objsearch_final" )
        colummap = { columns[i]: i for i in range(len(columns)) }
        util.logger.debug( f"object_search returning {len(rows)} objects in format {return_format}" )

    if return_format == 'json':
        rval = { c: [ r[colummap[c]] for r in rows ] for c in columns }
        if 'numdetinwindow' not in rval:
            rval['numdetinwindow'] = [ None for r in rows ]
        # util.logger.debug( f"returning json\n{json.dumps(rval,indent=4)}" )
        return rval

    elif return_format == 'pandas':
        df = pandas.DataFrame( rows, columns=columns )
        if 'numdetinwindow' not in df.columns:
            df['numdetinwindow'] = None
        # util.logger.debug( f"object_search pandas dataframe: {df}" )
        return df

    else:
        raise RuntimeError( "This should never happen." )


def get_hot_ltcvs( processing_version, detected_since_mjd=None, detected_in_last_days=None,
                   mjd_now=None, source_patch=False, include_hostinfo=False ):
    """Get lightcurves of objects with a recent detection.

    Parameters
    ----------
      processing_version: string
        The description of the processing version, or processing version
        alias, to use for searching all tables.

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

      source_path : bool, default Fals
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

      Returns
      -------
        ( pandas.DataFrame, pandas.DataFrame or None )

        A 2-element tuple.  The first will be a pandas DataFrame, the
        second will either be another DataFrame or None.  No indexes
        will have been set in the dataframes.

        The first one has the lightcurves.  It has columns:
           rootid -- the object root ID from the database.  (TODO: document.)
           ra -- the ra of the *object* (interpretation complicated).
                   Will be the same for all rows with the same rootid.
                   Decimal degrees, J2000.
           dec -- the dec of the *object* (goes with ra).  Decmial degrees, J2000.
           visit -- the visit number
           detector -- the detector number
           midpointmjdtai -- the MJD of the obervation
           band -- the filter (u, g, r, i, z, or Y)
           psfflux -- the PSF flux in nJy
           psffluxerr --- uncertaintly on psfflux in nJy
           is_source -- bool.  If you specified source_aptch=False, this
                          will be False for all rows.  Otherwise, it's
                          True for rows pulled from the diasource table,
                          and False for rows pulled from the
                          diaforcedsource table.

       The second member of the tuple will be None unless you specified
       include_hostinfo.  If include_hostinfo is true, then it's a
       dataframe with the following columns.  Note that the root id does
       *not* uniquely specify the host properties!  The
       processing_version you gave will affect which rows were actually
       pulled from the diaobject table.  (And it's potentially more
       complicated than that....)  These are mostly defined based on
       looking at the Object table as defined in the 2023-07-10 version
       of the DPDD in Table 4.3.1, with some columns coming from the
       DiaObject table defined by https://sdm-schemas.lsst.io/apdb.html
       (accessed on 2024-04-30).

           rootid --- the object root ID from the database.  Use this to
                        match to the lightcurve data frame.
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

    with db.DB() as con:
        with con.cursor() as cursor:
            # Figure out the processing version
            cursor.execute( "SELECT id FROM processing_version WHERE description=%(procver)s",
                            { 'procver': processing_version } )
            rows = cursor.fetchall()
            if len(rows) == 0:
                cursor.execute( "SELECT id FROM processing_version_alias WHERE description=%(procver)s",
                                { 'procver': processing_version } )
                rows = cursor.fetchall()
            if len(rows) == 0:
                raise ValueError( f"Could not find processing version '{processing_version}'" )
            procver = rows[0][0]

            # First : get a table of all the object ids (root object ids)
            #   that have a detection (i.e. a diasource) in the
            #   desired time period.

            q = ( "/*+ NoBitmapScan(elasticc2_diasource)\n"
                  "*/\n"
                  "SELECT DISTINCT ON(o.rootid) rootid "
                  "INTO TEMP TABLE tmp_objids "
                  "FROM diaobject o "
                  "INNER JOIN diasource s ON s.diaobjectid=o.diaobjectid "
                  "WHERE s.processing_version=%(procver)s AND s.midpointmjdtai>=%(t0)s" )
            if mjd_now is not None:
                q += "  AND midpointmjdtai<=%(t1)s"
            cursor.execute( q, { 'procver': procver, 't0': mjd0, 't1': mjd_now } )

            # Second : pull out host info for those objects if requested
            # TODO : right now it just pulls out nearby extended object 1.
            # make it configurable to get up to all three.
            hostdf = None
            if include_hostinfo:
                q = "SELECT DISTINCT ON (o.rootid) o.rootid,"
                for bandi in range( len(bands)-1 ):
                    q += ( f"h.stdcolor_{bands[bandi]}_{bands[bandi+1]},"
                           f"h.stdcolor_{bands[bandi]}_{bands[bandi+1]}_err," )
                q += ( "h.petroflux_r,h.petroflux_r_err,o.nearbyextobj1sep,h.pzmean,h.pzstd "
                       "FROM diaobject o "
                       "INNER JOIN host_galaxy h ON o.nearbyextobj1id=h.id "
                       "WHERE o.rootid IN (SELECT rootid FROM tmp_objids) "
                       "  AND o.processing_version=%(procver)s "
                       "ORDER BY o.rootid" )
                cursor.execute( q, { 'procver': procver} )
                columns = [ cursor.description[i][0] for i in range( len(cursor.description) ) ]
                hostdf = pandas.DataFrame( cursor.fetchall(), columns=columns )


            # Third : pull out all the forced photometry
            # THOUGHT REQUIRED : do we want midmpointmjdtai to stop at mjd_now-1 rather
            #   than mjd_now?  It depends what you mean.  If you want mjd_now to mean
            #   "data through this date" then don't stop a day early.  If you mean
            #   "simulate what we knew on this date"), then do stop a day early, because
            #   forced photometry will be coming out with a delay of a ~day.
            q = ( "/*+ IndexScan(f idx_diaforcedsource_diaobjectid)\n"
                  "    IndexScan(o)\n"
                  "*/\n"
                  "SELECT o.rootid AS rootid, o.ra AS ra, o.dec AS dec,"
                   "     f.visit,f.detector,f.midpointmjdtai,f.band,"
                   "     f.psfflux,f.psffluxerr "
                   "FROM diaforcedsource f "
                   "INNER JOIN diaobject o ON f.diaobjectid=o.diaobjectid  "
                   "WHERE o.rootid IN (SELECT rootid FROM tmp_objids) "
                   "  AND f.processing_version=%(procver)s" )
            if mjd_now is not None:
                q += "  AND f.midpointmjdtai<=%(t1)s "
            q += "ORDER BY o.rootid,f.midpointmjdtai"
            cursor.execute( q, { "procver": procver, "t1": mjd_now } )
            columns = [ cursor.description[i][0] for i in range( len(cursor.description) ) ]
            forceddf = pandas.DataFrame( cursor.fetchall(), columns=columns )
            forceddf['is_source'] = False

            # Fourth: if we've been asked to patch in sources where forced sources are
            #   missing, pull those down and concatenate them into the dataframe.
            # TODO : figure out the right hints to give when these tables
            #   are big!
            sourcedf = None
            if source_patch:
                q = ( "/*+ IndexScan(s idx_diasource_diaobjectid)\n"
                      "    IndexScan(f idx_diaforcedsource_diaobjectid)\n"
                      "    IndexScan(o)\n"
                      "*/\n"
                      "SELECT o.rootid,o.ra,o.dec,s.visit,s.detector,"
                      "       s.midpointmjdtai,s.band,s.psfflux,s.psffluxerr "
                      "FROM diasource s "
                      "INNER JOIN diaobject o ON s.diaobjectid=o.diaobjectid "
                      "LEFT JOIN diaforcedsource f ON (f.diaobjectid=s.diaobjectid AND "
                      "                                f.processing_version=s.processing_version AND "
                      "                                f.visit=s.visit) "
                      "WHERE o.rootid IN (SELECT rootid FROM tmp_objids) "
                      "  AND s.processing_version=%(procver)s "
                      "  AND f.diaobjectid IS NULL " )
                if mjd_now is not None:
                    q += "  AND s.midpointmjdtai<=%(t1)s "
                q += "ORDER BY o.rootid,s.midpointmjdtai"
                cursor.execute( q, { "procver": procver, "t1": mjd_now } )
                columns = [ cursor.description[i][0] for i in range( len(cursor.description) ) ]
                sourcedf = pandas.DataFrame( cursor.fetchall(), columns=columns )
                sourcedf['is_source'] = True
                forceddf = pandas.concat( [ forceddf, sourcedf ], axis='index' )
                forceddf.sort_values( [ 'rootid', 'midpointmjdtai' ], inplace=True )

    return forceddf, hostdf
