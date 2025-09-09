import datetime
import pytz
import flask
import uuid

import astropy.time

import db
import spectrum
from webserver.baseview import BaseView

# Want this to be False except when
#  doing deep-in-the-weeds debugging
_show_way_too_much_debug_info = False


# ======================================================================
# /spectrum/askforspectrum

class AskForSpectrum( BaseView ):
    def do_the_things( self ):
        # logger = flask.current_app.logger
        userid = flask.session['useruuid']

        data = flask.request.json
        if ( ( 'requester' not in data ) or
             ( 'objectids' not in data ) or
             ( 'priorities' not in data ) or
             ( not isinstance( data['objectids'], list ) ) or
             ( not isinstance( data['priorities'], list ) ) or
             ( len( data['objectids'] ) != len( data['priorities'] ) ) ):
            return "Mal-formed data for askforspectrum", 500

        now = datetime.datetime.now( tz=datetime.UTC )
        tocreate = [ { 'requester': data['requester'],
                       'root_diaobject_id': data['objectids'][i],
                       'wantspec_id': f"{data['objectids'][i]} ; {data['requester']}",
                       'user_id': userid,
                       'priority': ( 0 if int(data['priorities'][i]) < 0
                                     else 5 if int(data['priorities'][i]) > 5
                                     else int(data['priorities'][i] )),
                       'wanttime': now }
                       for i in range(len(data['objectids'])) ]

        n = db.WantedSpectra.bulk_insert_or_upsert( tocreate, upsert=True )

        return { 'status': 'ok',
                 'message': 'wanted spectra created',
                 'num': n }


# ======================================================================
# /spectrum/spectrawanted
#
# TODO : database hints

class WhatSpectraAreWanted( BaseView ):
    def do_the_things( self ):
        data = flask.request.json

        procver = data['processing_version'] if 'processing_version' in data.keys() else 'realtime'

        lim_mag_band = data['lim_mag_band'] if 'lim_mag_band' in data else None
        lim_mag = float( data['lim_mag'] ) if 'lim_mag' in data else None
        requester = data['requester'] if 'requester' in data else None

        if 'requested_since' in data.keys():
            try:
                wantsince = datetime.datetime.fromisoformat( data['requested_since'] )
                if wantsince.tzinfo is None:
                    wantsince = pytz.utc.localize( wantsince )
                else:
                    wantsince = wantsince.astimezone( datetime.UTC )
            except (TypeError, ValueError):
                return f"Failed to parse YYYY-MM-DD HH:MM:SS from {data['requested_since']}", 500
        else:
            wantsince = None

        now = datetime.datetime.now( tz=datetime.UTC )
        if 'mjd_now' in data:
            mjdnow = float( data['mjd_now'] )
            now = datetime.datetime.utcfromtimestamp( astropy.time.Time( mjdnow, format='mjd', scale='tai' ).unix_tai )
            now = pytz.utc.localize( now )

        notclaimedindays = 7 if 'not_claimed_in_last_days' not in data.keys() else data['not_claimed_in_last_days']
        notclaimsince = now - datetime.timedelta( days=int(notclaimedindays) ) if notclaimedindays is not None else None

        nospecindays = 7 if 'no_spectra_in_last_days' not in data.keys() else data['no_spectra_in_last_days']
        nospecsince = ( astropy.time.Time( now - datetime.timedelta( days=int(nospecindays) ) ).mjd
                        if nospecindays is not None else None )

        if 'detected_since_mjd' in data.keys():
            detsince = None if data['detected_since_mjd'] is None else float( data['detected_since_mjd'] )
        else:
            if 'detected_in_last_days' in data.keys():
                detected_in_last_days = int( data['detected_in_last_days' ] )
            else:
                detected_in_last_days = 14
            detsince = astropy.time.Time( now - datetime.timedelta( days=detected_in_last_days ) ).mjd

        df = spectrum.what_spectra_are_wanted( procver=procver, wantsince=wantsince, requester=requester,
                                               notclaimsince=notclaimsince, nospecsince=nospecsince,
                                               detsince=detsince, lim_mag=lim_mag, lim_mag_band=lim_mag_band,
                                               mjdnow=mjdnow, logger=flask.current_app.logger )

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


# ======================================================================
# /spectrum/planspectrum

class PlanSpectrum( BaseView ):
    def do_the_things( self ):
        data = flask.request.json

        if not all( i in data for i in ['oid', 'facility', 'plantime'] ):
            return "JSON payload must include keys oid, facility, plantime", 500

        try:
            plantime = datetime.datetime.fromisoformat( data['plantime'] )
            if plantime.tzinfo is None:
                plantime = pytz.utc.localize( plantime )
            else:
                plantime = plantime.astimezone( datetime.UTC )
        except (TypeError, ValueError):
            return f"Failed to parse YYYY-MM-DD HH:MM:SS from {data['plantime']}", 500

        kwargs = { 'root_diaobject_id': uuid.UUID( data['oid'] ),
                   'facility': str( data['facility'] ),
                   'plantime': plantime,
                   'comment': data['comment'] if 'comment' in data else None
                  }
        plansp = db.PlannedSpectra( **kwargs )
        plansp.insert( refresh=False )

        return { "status": "ok" }


# ======================================================================
# /spectrum/removespectrumplan

class RemoveSpectrumPlan( BaseView ):
    def do_the_things( self ):
        data = flask.request.json

        if ( 'oid' not in data ) or ( 'facility' not in data ):
            return "JSON payload must include keys oid and facility", 500

        with db.DB() as con:
            cursor = con.cursor()
            cursor.execute( "DELETE FROM plannedspectra WHERE root_diaobject_id=%(id)s "
                            "  AND facility=%(fac)s",
                            { 'id': data['oid'],
                              'fac': data['facility'] } )
            nrows = cursor.rowcount
            con.commit()

        return { 'status': 'ok', 'ndel': nrows }


# ======================================================================
# /spectrum/reportspectruminfo

class ReportSpectrumInfo( BaseView ):
    def do_the_things( self ):
        data = flask.request.json

        if not all( i in data for i in [ 'oid', 'facility', 'mjd', 'z', 'classid' ] ):
            return "JSON payload must include keys oid, facility, mjd, z, and classid", 500

        specinfo = db.SpectrumInfo( root_diaobject_id=uuid.UUID( data['oid'] ),
                                    facility=str( data['facility'] ),
                                    inserted_at=datetime.datetime.now( tz=datetime.UTC ),
                                    mjd=float( data['mjd'] ),
                                    z=( None if ( ( 'z' not in data ) or ( data['z'] is None ) or
                                                  ( str(data['z']).strip()=="" ) )
                                        else float( data['z'] ) ),
                                    classid=int( data['classid'] ) )
        specinfo.insert( refresh=False )

        return { 'status': 'ok' }


# ======================================================================
# /spectrum/getknownspectruminfo

class GetSpectrumInfo( BaseView ):
    def do_the_things( self ):
        data = flask.request.json.copy()

        if 'oid' in data:
            data['rootids'] = data['oid']
            del data['oid']

        df = spectrum.get_spectrum_info( logger=flask.current_app.logger, **data )
        df.rename( columns={ 'root_diaobject_id': 'oid' }, inplace=True )
        df['inserted_at'] = df['inserted_at'].apply( lambda x: x.isoformat() )
        return df.to_dict( 'records' )


# **********************************************************************
# **********************************************************************
# **********************************************************************

bp = flask.Blueprint( 'spectrumapp', __name__, url_prefix='/spectrum' )

urls = {
    "/askforspectrum": AskForSpectrum,
    "/spectrawanted": WhatSpectraAreWanted,
    "/planspectrum": PlanSpectrum,
    "/removespectrumplan": RemoveSpectrumPlan,
    "/reportspectruminfo": ReportSpectrumInfo,
    "/getknownspectruminfo": GetSpectrumInfo,
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
