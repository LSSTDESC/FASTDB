import textwrap

from psycopg import sql
import flask

import db
import ltcv
import util
from util import FDBLogger
from webserver.baseview import BaseView, FASTDBWebException


# ======================================================================
# /getmanyltcvs
# /getmanyltcvs/<procver>
#
# POST body must be json, must include objids.  May include bands, which, mjd_now

class GetManyLtcvs( BaseView ):
    def get_ltcvs( self, procver, objids, dbcon=None ):
        """Return lightcurves of objects as json.

        Reads the following parameters from the POST data, which must be
        a json dictionary; they are all passed on as-is tp
        ltcv.py::many_object_ltcvs:
           'bands', 'which', 'include_base_procver', 'include_source_positions',
           'use_weighted_source_positions', 'always_use_weighted_source_positions',
           'return_object_info', 'include_object_positions',
           'position_processing_version', 'mjd_now'


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
          application/json

          If return_object_info is True, then this is a dict with keys 'ltcvs' and 'obinfo'.  ROB DOCUMENT.


        """

        if not util.isSequence( objids ):
            objids = [ objids ]
        try:
            objids = [ int( o ) for o in objids ]
        except ValueError:
            try:
                objids = [ util.asUUID( o ) for o in objids ]
            except ValueError:
                raise FASTDBWebException( f"objids must be a list of integers or a list of uuids, got {objids}" )
        if len( objids ) == 0:
            raise FASTDBWebException( "no objids requested" )

        if flask.request.is_json:
            kwargs = flask.request.json
            unknown = set( kwargs.keys() ) - { 'bands', 'which', 'include_base_procver', 'include_source_positions',
                                               'use_weighted_source_positions', 'always_use_weighted_source_positions',
                                               'return_object_info', 'include_object_positions',
                                               'position_processing_version', 'mjd_now' }
            if len(unknown) > 0:
                raise FASTDBWebException( f"Unknown data parameters: {unknown}" )
        else:
            kwargs = {}

        try:
            rval = ltcv.many_object_ltcvs( processing_version=procver, objids=objids, return_format='json', **kwargs )
        except Exception as ex:
            FDBLogger.exception( ex )
            raise FASTDBWebException( f"Error trying to get lightcurves: {ex}" )

        if ( 'return_object_info' in kwargs ) and ( kwargs['return_object_info'] ):
            return { 'ltcvs': rval[0], 'objinfo': rval[1] }
        else:
            return rval


    def do_the_things( self, procver='default' ):
        if ( not flask.request.is_json ) or ( 'objids' not in flask.request.json ):
            raise FASTDBWebException( "Must pass POST data as a json dict with at least objids as a key" )
        objids = flask.request.json['objids']
        del flask.request.json['objids']
        return self.get_ltcvs( procver, objids )



# ======================================================================
# /ltcv/getltcv

class GetLtcv( GetManyLtcvs ):
    def do_the_things( self, procver, objid=None ):
        if objid is None:
            objid = procver
            procver = 'default'

        mess = self.get_ltcvs( procver, [ objid ] )
        if isinstance( mess, dict ):
            # This means we returned ltcvs and objinfo
            if len(mess['ltcvs']) == 0:
                raise FASTDBWebException( f"Could not find lightcurve for {objid} in processing version {procver}" )
            return { 'ltcv': mess['ltcvs'][0], 'objinfo': mess['objinfo'] }
        else:
            if len(mess) == 0:
                raise FASTDBWebException( f"Could not find lightcurve for {objid} in processing version {procver}" )
            if len(mess) > 1:
                raise FASTDBWebException( f"Got {len(mess)} lightcurves for {objid} in processing version {procver}; "
                                          f"this is suprising, and something is wrong somewhere." )
            return mess[0]


# ======================================================================
# /ltcv/getrandomltcv
#
# NOTE : I'm not happy with this one, as it requires an object processing
#   version, but those are more opaque than photometry processing versions.

class GetRandomLtcv( GetLtcv ):
    def do_the_things( self, procver="default" ):
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
        return mess[ key0 ]


# ======================================================================
# /ltcv/gethottransients

class GetHotTransients( BaseView ):
    """Get lightcurves of recently-detected transients.  URL endpoint /ltcv/gethottransients

    ROB DOCUMENT.

    OLD:

         return_format = 0:
            Returns a list of dictionaries.  Each row corresponds to a single
            detected transient, and will have keys:
               rootid : string UUID.  This is the thing you should use to keep
                        track of the object.
               diaobjectid : list of bigint; diaobjectids in the desired
                             processing version associated with this rootid.
                             There may be multiple
               ra : float (*see below)
               dec : float (*see below)
               ra_err: float (*see below)
               dec_err: float (*see below)
               ra_dec_cov: float (*see below)
               zp : float, always 31.4
               redshift : float, currently always -99  (not implemented!)
               photometry : dict with several keys, the value of each of which is a list
                    mjd : float, mjd of point
                    band : str, one if u, g, r, i, z, or Y
                    flux : float, psf flux in nJy
                    fluxerr : uncertainty on flux
                    isdet : bool; if True, this point was detected (i.e. a source exists)
                    ispatch : bool; if False, the flux is forced photometry, if True, the flux is
                              from the detection.  Will only be present if source_patch is True.

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

            WARNING NOT TESTED.

         return_format = 2:
            Returns a dict.  Each value of the dict is a list, and all lists
            have the same number of values.  Each element of each list corresponds
            to a single transient, and they're all ordered the same.  The keys of
            the top-level dictionary are the same as the keys of each row in
            return_format 1.

            WARNING NOT TESTED.

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

    def do_the_things( self, procver='realtime' ):
        known_keys = { 'position_processing_version', 'include_object_positions',
                       'include_source_positions', 'include_base_procver',
                       'use_weighted_source_positions', 'always_use_weighted_source_positions',
                       'detected_since_mjd', 'detected_in_last_days', 'mjd_now',
                       'source_patch' }

        if not flask.request.is_json:
            kwargs = dict()
        else:
            kwargs = flask.request.json

        unknown = set( kwargs.keys() ) - known_keys
        if len(unknown) > 0:
            raise FASTDBWebException( f"Unknown data parameters: {unknown}" )

        try:
            rval = ltcv.get_hot_ltcvs( procver, return_format='json', **kwargs )
        except Exception as ex:
            FDBLogger.exception( ex )
            raise FASTDBWebException( f"Error trying to get hot transients: {ex}" )

        return { 'ltcvs': rval[0], 'objinfo': rval[1] }



# **********************************************************************


class GetBrokerInfo( BaseView ):
    def do_the_things( self, processing_version='realtime' ):
        global app
        if ( not flask.request.is_json ) or ( not isinstance( flask.request.json, dict ) ):
            raise FASTDBWebException( "Post data was not a JSON dict, expected a dict as JSON post data." )
        jsondata = flask.request.json
        if 'diasourceids' not in jsondata:
            raise FASTDBWebException( "Post data dict must include key diasourceids with list of source ids." )
        srcids = jsondata['diasourceids']
        srcids = list( srcids ) if util.isSequence(srcids) else [ srcids ]
        brokername = None if 'brokername' not in jsondata else jsondata['brokername']
        topic = None if 'topic' not in jsondata else jsondata[ 'topic' ]

        with db.DBCon() as con:
            try:
                pvid = db.ProcessingVersion.procver_id( processing_version, dbcon=con )
            except Exception:
                raise FASTDBWebException( f"Unknown processing version {processing_version}" )
            q = sql.SQL( textwrap.dedent(
                """\
                SELECT DISTINCT ON (b.diasourceid, b.brokername, b.topic)
                   b.diasourceid, b.brokername, b.topic, b.info
                FROM diasource_brokerinfo b
                INNER JOIN base_procver_of_procver pv ON b.base_procver_id=pv.base_procver_id
                                                     AND pv.procver_id={pvid}
                WHERE b.diasourceid=ANY({srcids})
                """
            ) ).format( pvid=pvid, srcids=srcids )
            if brokername is not None:
                q += sql.SQL( "  AND b.brokername={brokername}\n" ).format( brokername=brokername )
            if topic is not None:
                q += sql.SQL("   AND b.topic={topic}\n" ).format( topic=topic )
            q += sql.SQL( textwrap.dedent(
                """\
                ORDER BY b.diasourceid, b.brokername, b.topic
                """
            ) )
            rows, _cols = con.execute( q )

        rval = {}
        curdiasourceid = None
        for row in rows:
            if row[0] != curdiasourceid:
                curdiasourceid = row[0]
                rval[ curdiasourceid ] = []
            rval[ curdiasourceid ].append( { 'brokername': row[1],
                                             'topic': row[2],
                                             'info': row[3] } )

        return rval






# **********************************************************************
# **********************************************************************
# **********************************************************************

bp = flask.Blueprint( 'ltcvapp', __name__, url_prefix='/ltcv' )

urls = {
    "/getmanyltcvs": GetManyLtcvs,
    "/getmanyltcvs/<procver>": GetManyLtcvs,
    "/getltcv/<procver>": GetLtcv,             # <procver> is really <objid> in this case
    "/getltcv/<procver>/<objid>": GetLtcv,
    "/getrandomltcv": GetRandomLtcv,
    "/getrandomltcv/<procver>": GetRandomLtcv,
    "/gethottransients": GetHotTransients,
    "/gethottransients/<procver>": GetHotTransients,
    "/getbrokerinfo": GetBrokerInfo,
    "/getbrokerinfo/<processing_version>": GetBrokerInfo
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
