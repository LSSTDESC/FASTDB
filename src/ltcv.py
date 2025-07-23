import datetime
import numbers
import json

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


def object_search( processing_version, return_format='json', **kwargs ):
    util.logger.debug( f"In object_search : kwargs = {kwargs}" )
    knownargs = { 'ra', 'dec', 'radius',
                  'mint_firstdetection', 'maxt_firstdetection',
                  'mint_lastdetection', 'maxt_lastdetection',
                  'min_numdetections', 'mindt_firstlastdetection','maxdt_firstlastdetection',
                  'min_bandsdetected', 'min_lastmag', 'max_lastmag',
                  'statbands' }
    unknownargs = set( kwargs.keys() ) - knownargs
    if len( unknownargs ) != 0:
        raise ValueError( f"Unknown search keywords: {unknownargs}" )

    if return_format not in [ 'json', 'pandas' ]:
        raise ValueError( f"Unknown return format {return_format}" )

    statbands = None
    if 'statbands' in kwargs:
        if ( isinstance( kwargs['statbands'], str ) ) and ( len(kwargs['statbands'].strip()) == 0 ):
            statbands = None
        else:
            if isinstance( kwargs['statbands'], list ):
                statbands = kwargs['statbands']
            else:
                statbands = [ kwargs['statbands'] ]
            if not all( isinstance(b, str) for b in statbands ):
                return TypeError( 'statbands must be a str or a list of str' )

    with db.DB() as con:
        cursor = con.cursor()

        # Figure out processing version
        try:
            procver = int( processing_version)
        except Exception:
            cursor.execute( "SELECT id FROM processing_version WHERE description=%(procver)s",
                            { 'procver': processing_version } )
            rows = cursor.fetchall()
            if len(rows) == 0:
                cursor.execute( "SELECT id FROM processing_version_alias WHERE description=%(procver)s",
                                { 'procver': processing_version } )
                rows = cursor.fetchall()
                if len(rows) == 0:
                    raise ValueError( f"Unknown processing version {processing_version}" )
            procver = rows[0][0]

        # Filter by ra and dec if given
        ra = util.float_or_none_from_dict_float_or_hms( kwargs, 'ra' )
        dec = util.float_or_none_from_dict_float_or_dms( kwargs, 'dec' )
        if ( ra is None ) != ( dec is None ):
            raise ValueError( "Must give either both or neither of ra and dec, not just one." )

        nexttable = 'diaobject'
        if ra is not None:
            radius = util.float_or_none_from_dict( kwargs, 'radius' )
            radius = radius if radius is not None else 10.
            q = ( "SELECT * INTO TEMP TABLE objsearch_tmp1 "
                  "FROM diaobject "
                  "WHERE processing_version=%(pv)s "
                  "AND q3c_radial_query( ra, dec, %(ra)s, %(dec)s, %(rad)s )" )
            subdict = { 'pv': procver, 'ra': ra, 'dec': dec, 'rad': radius/3600. }
            util.logger.debug( f"Sending query: {q} with subdict {subdict}" )
            cursor.execute( q, subdict )
            nexttable = 'objsearch_tmp1'

        mint_firstdet = util.mjd_or_none_from_dict_mjd_or_timestring( kwargs, 'mint_firstdetection' )
        maxt_firstdet = util.mjd_or_none_from_dict_mjd_or_timestring( kwargs, 'maxt_firstdetection' )
        mint_lastdet = util.mjd_or_none_from_dict_mjd_or_timestring( kwargs, 'mint_lastdetection' )
        maxt_lastdet = util.mjd_or_none_from_dict_mjd_or_timestring( kwargs, 'maxt_lastdetection' )
        if any( i is not None for i in [ mint_firstdet, maxt_firstdet, mint_lastdet, maxt_lastdet ] ):
            raise NotImplementedError( "Filtering by detection times not yet implemented" )

        min_numdets = util.int_or_none_from_dict( kwargs, 'min_numdetections' )
        if min_numdets is not None:
            raise NotImplementedError( "Filtering by number of detections not yet implemented" )

        mindt = util.float_or_none_from_dict( kwargs, 'mindt_firstlastdetection' )
        maxdt = util.float_or_none_from_dict( kwargs, 'maxdt_firstlastdetection' )
        if ( mindt is not None ) or ( maxdt is not None ):
            raise NotImplementedError( "Filtering by time between first and last detection not yet implemented" )

        min_bands = util.int_or_none_from_dict( kwargs, 'min_bandsdetected' )
        if min_bands is not None:
            raise NotImplementedError( "Filtering by number of bands detected is not yet implemented" )

        min_lastmag = util.float_or_none_from_dict( kwargs, 'min_lastmag' )
        max_lastmag = util.float_or_none_from_dict( kwargs, 'max_lastmag' )
        if ( min_lastmag is not None ) or ( max_lastmag is not None ):
            raise NotImplementedError( "Filtering by last magnitude not yet implemented" )


        if nexttable == 'diaobject':
            raise RuntimeError( "Error, no search criterion given" )

        q = ( f"SELECT o.diaobjectid, o.ra, o.dec, s.psfflux AS srcflux, s.psffluxerr AS srcdflux, "
              f"       s.midpointmjdtai AS srct, s.band AS srcband "
              f"INTO TEMP TABLE objsearch_sources "
              f"FROM {nexttable} o "
              f"INNER JOIN diasource s ON o.diaobjectid=s.diaobjectid AND s.processing_version=%(pv)s " )
        if statbands is not None:
            q += "WHERE s.band=ANY(%(bands)s) "
        q += "ORDER BY diaobjectid, srct"
        subdict = { 'pv': procver, 'bands': statbands }
        util.logger.debug( f"Sending query: {q} with subdict {subdict}" )
        cursor.execute( q, subdict )
        q = ( "SELECT diaobjectid, ra, dec, COUNT(srcflux) AS ndet, "
              "        NULL::real AS maxflux, NULL::real AS maxdflux, "
              "        NULL::double precision AS maxfluxt, NULL::character(1) AS maxfluxband, "
              "        NULL::real as lastflux, NULL::real AS lastdflux, NULL::character(1) as lastfluxband, "
              "        NULL::double precision as lastfluxt "
              "INTO TEMP TABLE objsearch_srcstats "
              "FROM objsearch_sources "
              "GROUP BY diaobjectid, ra, dec" )
        util.logger.debug( f"Sending query {q}" )
        cursor.execute( q )
        q = ( "UPDATE objsearch_srcstats oss "
              "SET maxflux=subq.srcflux, maxdflux=subq.srcdflux, maxfluxt=subq.srct, "
              "    maxfluxband=subq.srcband "
              "FROM ( SELECT DISTINCT ON (diaobjectid) diaobjectid, srcflux, srcdflux, srct, srcband "
              "       FROM objsearch_sources "
              "       ORDER BY diaobjectid, srcflux DESC ) subq "
              "WHERE oss.diaobjectid=subq.diaobjectid" )
        util.logger.debug( f"Sending query {q}" )
        cursor.execute( q )
        q = ( "UPDATE objsearch_srcstats oss "
              "SET lastflux=subq.srcflux, lastdflux=subq.srcdflux, lastfluxt=subq.srct, "
              "    lastfluxband=subq.srcband "
              "FROM ( SELECT DISTINCT ON (diaobjectid) diaobjectid, srcflux, srcdflux, srct, srcband "
              "       FROM objsearch_sources "
              "       ORDER BY diaobjectid, srct DESC ) subq "
              "WHERE oss.diaobjectid=subq.diaobjectid" )
        util.logger.debug( f"Sending query {q}" )
        cursor.execute( q )

        # For some reason, Postgres was deciding not to use the index on this next query, which
        #   raised the runtime by two orders of magnitude.  Hint fixed it.
        q = ( "/*+ IndexScan(f idx_diaforcedsource_diaobjectidpv ) */ "
              "SELECT DISTINCT ON (t.diaobjectid) t.diaobjectid, t.ra, t.dec, t.ndet, "
              "    t.maxflux AS maxdetflux, t.maxdflux AS maxdetfluxerr, t.maxfluxt AS maxdetfluxmjd, "
              "    t.maxfluxband as maxdetfluxband, "
              "    t.lastflux AS lastdetflux, t.lastdflux AS lastdetfluxerr, t.lastfluxt AS lastdetfluxmjd, "
              "    t.lastfluxband AS lastdetfluxband, "
              "    f.psfflux AS lastforcedflux, f.psffluxerr AS lastforcedfluxerr, "
              "    f.midpointmjdtai AS lastforcedfluxmjd, f.band AS lastforcedfluxband "
              "FROM objsearch_srcstats t "
              "INNER JOIN diaforcedsource f ON t.diaobjectid=f.diaobjectid AND f.processing_version=%(pv)s " )
        if statbands is not None:
            q += "WHERE f.band=ANY(%(bands)s) "
        q += "ORDER BY t.diaobjectid, f.midpointmjdtai DESC"
        subdict = { 'pv': procver, 'bands': statbands }
        util.logger.debug( f"Sending query: {q} with subdict {subdict}" )
        cursor.execute( q, subdict )
        columns = [ d[0] for d in cursor.description ]
        colummap = { cursor.description[i][0]: i for i in range( len(cursor.description) ) }
        rows = cursor.fetchall()

        util.logger.debug( f"object_search returning {len(rows)} objects in format {return_format}" )

    if return_format == 'json':
        rval = { c: [ r[colummap[c]] for r in rows ] for c in columns }
        util.logger.debug( f"returning json\n{json.dumps(rval,indent=4)}" )
        return rval

    elif return_format == 'pandas':
        df = pandas.DataFrame( rows, columns=columns )
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
            q = ( "/*+ IndexScan(f idx_diaforcedsource_diaobjectidpv)\n"
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
                q = ( "/*+ IndexScan(s idx_diasource_diaobjectidpv)\n"
                      "    IndexScan(f idx_diaforcedsource_diaobjectidpv)\n"
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
