import numbers

from psycopg import sql
import flask

import db
import ltcv
import util
from webserver.baseview import BaseView


# ======================================================================
#

class GetManyLtcvs( BaseView ):
    def get_ltcvs( self, procver, objids, dbcon=None ):
        """Return lightcurves of objects as json.

        Reads parameters 'bands', 'which', and 'mjd_now' from
        flask.request.json.  'bands' is a list of bands to include in
        the lightcurve.  'which' is one of 'detections', 'forced', or
        'patch'.  See ltcv.object_ltcv

        Parameters
        ----------
          procver : uuid or str
            The processing version to pull lightcurves from.

          objids : int, uuid, list of int, or list of uuid
            The object IDs to pull lightcurves for.

          dbcon : db.DBCon or psycopg.Connection, default None
            Database connection.  If None, opens a new one and closes it
            when done.

        Returns
        -------
          result: dict
            Same dict as is returned from ltcv.many_object_ltcvs()

        """

        if not util.isSequence( objids ):
            objids = [ objids ]
        if all( isinstance( o, numbers.Integral ) for o in objids ):
            objids_are_root = False
        else:
            objids_are_root = True
            try:
                objids = [ util.asUUID( o ) for o in objids ]
            except ValueError:
                raise ValueError( "objids must be a list of integers or a list of uuids" )
        if len( objids ) == 0:
            raise ValueError( "no objids requested" )

        bands = None
        which = 'patch'
        mjd_now = None
        if flask.request.is_json:
            data = flask.request.json
            unknown = set( data.keys() ) - { 'bands', 'which', 'mjd_now' }
            if len(unknown) > 0:
                raise ValueError( f"Unknown data parameters: {unknown}" )
            if 'bands' in data:
                bands = data['bands']
            if 'which' in data:
                if data['which'] not in ( 'detections', 'forced', 'patch' ):
                    raise ValueError( f"Unknown value of which: {which}" )
                which = data['which']
            if 'mjd_now' in data:
                mjd_now = float( data['mjd_now'] )

        return ltcv.many_object_ltcvs( procver, objids, bands=bands, which=which,
                                       return_format='json', mjd_now=mjd_now, dbcon=dbcon )

    def do_the_things( self, procver='default' ):
        if ( not flask.is_json ) or ( 'objids' not in flask.request.json ):
            raise ValueError( f"Must pass POST data as a json dict with at least objids as a key" )
        return self.get_ltcvs( procver, flask.request.json['objids'] )
    


# ======================================================================
# /ltcv/getltcv
#
# Returns a dict with keys mjd, band, flux, fluxerr, isdet, and maybe ispatch

class GetLtcv( GetManyLtcvs ):
    def do_the_things( self, procver, objid=None ):
        if objid is None:
            objid = procver
            procver = 'default'

        mess = self.get_ltcvs( procver, [ objid ] )
        key0 = list( mess.keys() )[0]
        rval = mess[ key0 ]
        del rval['rootid']
        return rval


# ======================================================================
# /ltcv/getrandomltcv

class GetRandomLtcv( GetLtcv ):
    def do_the_things( self, procver ):
        with db.DBCon() as dbcon:
            pv = db.ProcessingVersion.procver_id( procver, dbcon=dbcon  )

            q = sql.SQL(
                """
                SELECT diaobjectid
                FROM (
                  SELECT DISTINCT ON (o.diaobjectid) o.diaobjectid
                  FROM diaobject o
                  INNER JOIN base_procver_of_procver pv ON o.base_procver_id=pv.base_procver_id
                                                       AND pv.procver_id={procver}
                  ORDER BY o.diaobjectid, pv.priority DESC
                ) subq
                ORDER BY random() LIMIT 1
                """
            ).format( procver=pv )
            rows, _cols = dbcon.execute( q )

            mess = self.get_ltcvs( pv, [ rows[0][0] ], dbcon=dbcon )
            key0 = list( mess.keys() )[0]
            rval = mess[ key0 ]
            del rval['rootid']
            
            
            
            # THINK ; this may be slow, as it may sort the entire object table!  Or at least the index.
            # TABLESAMPLE may be a solution, but it doesn't interact with WHERE
            #   the way I'd want it to.  (Empirically, doesn't seem too bad with a 4M object table.)
            cursor.execute( "SELECT diaobjectid FROM diaobject WHERE processing_version=%(pv)s "
                            "ORDER BY random() LIMIT 1", { 'pv': pv } )
            objid = cursor.fetchone()[0]
            return self.get_ltcv( procver, pv, objid, dbcon=dbcon )


# ======================================================================
# /ltcv/gethottransients

class GetHotTransients( BaseView ):
    """Get lightcurves of recently-detected transients.  URL endpoint /ltcv/gethottransients

    Calling
    -------

    Hit this entpoint with a POST request.  The POST payload should be a JSON
    dictionary.  The dictionary can include the following keys (though all are
    optional):

       processing_version : str
         The processing version or alias.  If not given, assumes "default"

       return fromat : int
         Specifies the format of the data returned; see below.  If not given,
         assumes 0.

       detected_since_mjd : float
         If given, gets all transients detected since this mjd.

       detected_in_last_days : float
         Get all transients detected in this many days.  Can't give both this
         and detected_since_mjd.  If neither are given, assumes
         detected_in_last_days=30

       mjd_now : float
         Pass a value here to make the server pretend that this is the current
         mjd.  Normally, it just uses the current time.  (Useful for tests and
         development.)

       source_patch : bool
         Defaults to False, in which case only forced photometry will be
         returned.  If True, then return detections where forced photometry is
         not available.  See "WARNING re: forced photometry and detctions"
         below.

       include_hostinfo : bool
         Defaults to False.  If True, additional information will be returned
         with the first-listed possible host of each transient.

    Returns
    -------
      application/json   (utf-8 encoded, which I believe is required for json)

         The format of the returned JSOn depends on the return_format paremeter.

         return_format = 0:
            Returns a list of dictionaries.  Each row corresponds to a single
            detected transients, and will have keys:
               objectid : string UUID
               ra : float, ra of the object
               dec : float, dec of the object
               zp : float, always 31.4
               redshift : float, currently always -99  (not implemented!)
               sncode : int, currently always -99  (not implemented!)
               photometry : dict with four keys, each of which is a list
                    mjd : float, mjd of point
                    band : str, one if u, g, r, i, z, or Y
                    flux : float, psf flux in nJy
                    fluxerr : uncertainty on flux
                    is_source : bool; if False, this is forced photometry, if true it's a detection

               If include_hostinfo was True, then each row also includes the following fields:

               hostgal_stdcolor_u_g : float, color in magnitudes
               hostgal_stdcolor_g_r : float
               hostgal_stdcolor_r_i : float
               hostgal_stdcolor_i_z : float
               hostgal_stdcolor_z_y : float
               hostgal_stdcolor_u_g_err : float, uncertainty on color in magnitudes
               hostgal_stdcolor_g_r_err : float
               hostgal_stdcolor_r_i_err : float
               hostgal_stdcolor_i_z_err : float
               hostgal_stdcolor_z_y_err : float
               hostgal_petroflux_r : float, the flux within a defined radius in nJy (use zeropoint=31.4)
               hostgal_petroflux_r_err : float, uncertainty on petroflux_r
               hostgal_snsep : float, a number that's currently poorly defined and that will change
               hostgal_pzmean : float, estimate of mean photometric redshift
               hostgal_pzstd : float, estimate of std deviation of photometric redshift

                NOTE : it's possible that more host galaxy fields will be added

         return_format = 1:
            Returns a list of dictionaries.  Similar to return_format 0,
            except instead of having the key "photometry" pointing to a
            dictionary, the dictionary in each row of the return has four
            additional keys mjd, band, flux, fluxerr, and is_source.  Each
            element of those five lists are themselves a list, holding what
            would have been in the elements of the 'photometry' dictionary in
            return_format 1.

         return_format = 2:
            Returns a dict.  Each value of the dict is a list, and all lists
            have the same number of values.  Each element of each list corresponds
            to a single transient, and they're all ordered the same.  The keys of
            the top-level dictionary are the same as the keys of each row in
            return_format 1.

         Both return formats 1 and 2 can be loaded directly into a pandas data
         frame, though polars might work better because it has better direct
         support for embedded lists.  (Return format 0 can probably also be
         loaded into both.  Cleanest will be using return format 2 with polars.)

    WARNING re: forced photometry and detections

    The most consistent lightcurve will be based entirely on forced
    photometry.  In that case, forced photometry has been performed on
    difference images at... I THINK... the same RA/Dec on all images.  (This
    depends on exactly how LSST does things, and may well be different for
    PPDB data than for DR data.)  However, especially when dealing with
    real-time data, forced photometry may not yet be available.  Detections
    happen in near-real-time, but forced photometry will be delayed by
    somethign like 24 hours.  (TODO: figure out the project specs on this.)
    For real-time analysis, you probably want the latest data.  In that case,
    set source_patch to True.  Lightcurves you get back will be heterogeneous.
    Most of each lightcurve will be based on forced photometry, but for
    detections that do not yet have corresponding forced photometry in our
    database, you will get the detection fluxes.

    """

    def do_the_things( self ):
        logger = flask.current_app.logger
        bands = [ 'u', 'g', 'r', 'i', 'z', 'y' ]

        if not flask.request.is_json:
            raise TypeError( "POST data was not JSON" )
        data = flask.request.json

        kwargs = {}
        if 'procesing_version' not in data:
            kwargs['processing_version'] = 'default'
        kwargs.update( data )
        if 'return_format' in kwargs:
            return_format = kwargs['return_format']
            del kwargs['return_format']
        else:
            return_format = 0

        df, hostdf = ltcv.get_hot_ltcvs( **kwargs )

        if ( return_format == 0 ) or ( return_format == 1 ):
            sne = []
        elif ( return_format == 2 ):
            sne = { 'objectid': [],
                    'ra': [],
                    'dec': [],
                    'mjd': [],
                    'band': [],
                    'flux': [],
                    'fluxerr': [],
                    'is_source': [],
                    'zp': [],
                    'redshift': [],
                    'sncode': [] }
            if hostdf is not None:
                sne[ 'hostgal_petroflux_r' ] = []
                sne[ 'hostgal_petroflux_r_err' ] = []
                sne[ 'hostgal_snsep' ] = []
                sne[ 'hostgal_pzmean' ] = []
                sne[ 'hostgal_pzstd' ] = []
                for bandi in range( len(bands)-1 ):
                    sne[ f'hostgal_stdcolor_{bands[bandi]}_{bands[bandi+1]}' ] = []
                    sne[ f'hostgal_stdcolor_{bands[bandi]}_{bands[bandi+1]}_err' ] = []
        else:
            raise RuntimeError( "This should never happen." )

        # ZEROPOINT
        #
        # https://sdm-schemas.lsst.io/apdb.html claims that all fluxes are in nJy.
        # Wikipedia tells me that mAB = -2.5log_10(f_ν) + 8.90
        #   with f_ν in Jy, or, better stated, since arguments of logs should not have units:
        #     mAB = -2.5 log_10( f_ν / 1 Jy ) + 8.90
        # Converting units:
        #    mAB = -2.5 log_10( f_ν / 1 Jy * ( 1 Jy / 10⁹ nJy ) ) + 8.90
        #        = -2.5 log_10( f_ν / nJy * 10⁻⁹ ) +  8.90
        #        = -2.5 ( log_10( f_ν / nJy ) - 9 ) + 8.90
        #        = -2.5 log_10( f_ν / nJy ) + 31.4

        if len(df) > 0:
            objids = df['rootid'].unique()
            logger.debug( f"GetHotSNEView: got {len(objids)} objects in a df of length {len(df)}" )
            df.set_index( [ 'rootid', 'visit' ], inplace=True )
            if hostdf is not None:
                hostdf.set_index( 'rootid', inplace=True )

            for objid in objids:
                subdf = df.xs( objid, level='rootid' )
                if hostdf is not None:
                    subhostdf = hostdf.xs( objid )
                if ( return_format == 0 ) or ( return_format == 1 ):
                    toadd = { 'objectid': str(objid),
                              'ra': subdf.ra.values[0],
                              'dec': subdf.dec.values[0],
                              'zp': 31.4,
                              'redshift': -99.,
                              'sncode': -99 }
                    if hostdf is not None:
                        toadd[ 'hostgal_petroflux_r' ] = subhostdf.petroflux_r
                        toadd[ 'hostgal_petroflux_r_err' ] = subhostdf.petroflux_r_err
                        toadd[ 'hostgal_snsep' ] = subhostdf.nearbyextobj1sep
                        toadd[ 'hostgal_pzmean' ] = subhostdf.pzmean
                        toadd[ 'hostgal_pzstd' ] = subhostdf.pzstd
                        for bandi in range( len(bands)-1 ):
                            toadd[ f'hostgal_stdcolor_{bands[bandi]}_{bands[bandi+1]}' ] = (
                                subhostdf[ f'stdcolor_{bands[bandi]}_{bands[bandi+1]}' ] )
                            toadd[ f'hostgal_stdcolor_{bands[bandi]}_{bands[bandi+1]}_err' ] = (
                                subhostdf[ f'stdcolor_{bands[bandi]}_{bands[bandi+1]}_err' ] )

                    if return_format == 0:
                        toadd['photometry'] = { 'mjd': list( subdf['midpointmjdtai'] ),
                                                'band': list( subdf['band'] ),
                                                'flux': list( subdf['psfflux'] ),
                                                'fluxerr': list( subdf['psffluxerr'] ),
                                                'is_source': list( subdf['is_source'] ) }
                    else:
                        toadd['mjd'] = list( subdf['midpointmjdtai'] )
                        toadd['band'] = list( subdf['band'] )
                        toadd['flux'] = list( subdf['psfflux'] )
                        toadd['fluxerr'] = list( subdf['psffluxerr'] )
                        toadd['is_source'] = list( subdf['is_source'] )
                    sne.append( toadd )
                elif return_format == 2:
                    sne['objectid'].append( str(objid) )
                    sne['ra'].append( subdf.ra.values[0] )
                    sne['dec'].append( subdf.dec.values[0] )
                    sne['mjd'].append( list( subdf['midpointmjdtai'] ) )
                    sne['band'].append( list( subdf['band'] ) )
                    sne['flux'].append( list( subdf['psfflux'] ) )
                    sne['fluxerr'].append( list( subdf['psffluxerr'] ) )
                    sne['is_source'].append( list( subdf['is_source'] ) )
                    sne['zp'].append( 31.4 )
                    sne['redshift'].append( -99 )
                    sne['sncode'].append( -99 )
                    if hostdf is not None:
                        sne[ 'hostgal_petroflux_r' ].append( subhostdf['petroflux_r'] )
                        sne[ 'hostgal_petroflux_r_err'] .append( subhostdf['petroflux_r_err'] )
                        sne[ 'hostgal_snsep' ].append( subhostdf['nearbyextobj1sep'] )
                        sne[ 'hostgal_pzmean' ].append( subhostdf['pzmean'] )
                        sne[ 'hostgal_pzstd' ].append( subhostdf['pzstd'] )
                        for bandin in range( len(bands) ):
                            sne[ f'hostgal_stdcolor_{bands[bandi]}_{bands[bandi+1]}' ].append(
                                subhostdf[f'stdcolor_{bands[bandi]}_{bands[bandi+1]}'] )
                            sne[ f'hostgal_stdcolor_{bands[bandi]}_{bands[bandi+1]}_err' ].append(
                                subhostdf[f'stdcolor_{bands[bandi]}_{bands[bandi+1]}_err'] )
                else:
                    raise RuntimeError( "This should never happen." )


        # logger.info( "GetHotTransients; returning" )
        return sne




# **********************************************************************
# **********************************************************************
# **********************************************************************

bp = flask.Blueprint( 'ltcvapp', __name__, url_prefix='/ltcv' )

urls = {
    "/getltcv/<procver>/<objid>": GetLtcv,
    "/getrandomltcv/<procver>": GetRandomLtcv,
    "/gethottransients": GetHotTransients
}

usedurls = {}
for url, cls in urls.items():
    if url not in usedurls.keys():
        usedurls[ url ] = 0
        name = url
    else:
        usedurls[ url ] += 1
        name = f'{url}.{usedurls[url]}'

    bp.add_url_rule (url, view_func=cls.as_view(name), methods=['POST'], strict_slashes=False )
