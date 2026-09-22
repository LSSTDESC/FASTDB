import re
import copy
import logging

from psycopg import sql
import flask
import flask_session

import db
import ltcv
from util import FDBLogger
import webserver.rkauth_flask as rkauth_flask
import webserver.dbapp as dbapp
import webserver.ltcvapp as ltcvapp
import webserver.spectrumapp as spectrumapp
from webserver.baseview import BaseView, FASTDBWebException

# ======================================================================
# Global config

import config
with open( config.secretkeyfile ) as ifp:
    _flask_session_secret_key = ifp.readline().strip()


# ======================================================================

class MainPage( BaseView ):
    def dispatch_request( self ):
        return flask.render_template( "fastdb_webap.html" )


# ======================================================================

class GetProcVers( BaseView ):
    def do_the_things( self ):
        # global app

        with db.DBCon() as con:
            pvrows, _ = con.execute( "SELECT description FROM processing_version" )
            alrows, _ = con.execute( "SELECT description FROM processing_version_alias" )

        rows = [ r[0] for r in ( pvrows + alrows ) ]
        rows.sort()

        # app.logger.debug( f"GetProcVers: rows is {rows}" )

        return { 'status': 'ok',
                 'procvers': rows
                }


# ======================================================================

class ProcVer( BaseView ):
    def do_the_things( self, procver ):
        # global app
        # app.logger.debug( f"In ProcVer with procver={procver}" )

        with db.DBCon() as con:
            try:
                pvid = db.ProcessingVersion.procver_id( procver, dbcon=con )
                if pvid is None:
                    return f"Unknown processing version {procver}", 422
            except Exception as ex:
                raise FASTDBWebException( str(ex) )

            retval = { 'status': 'ok', 'id': None, 'description': None, 'aliases': [], 'base_procvers': [] }
            row, _ = con.execute( "SELECT id,description FROM processing_version WHERE id=%(pv)s", { 'pv': pvid } )
            retval['id'] = row[0][0]
            retval['description'] = row[0][1]

            rows, _ = con.execute( "SELECT description FROM processing_version_alias WHERE procver_id=%(pv)s",
                                   { 'pv': pvid } )
            retval['aliases'] = [ r[0] for r in rows ]

            rows, _ = con.execute( "SELECT _table, ARRAY_AGG(description), ARRAY_AGG(priority)\n"
                                   "FROM (\n"
                                   "  SELECT b.description,b._table,j.priority\n"
                                   "  FROM base_processing_version b\n"
                                   "  INNER JOIN base_procver_of_procver j ON b.id=j.base_procver_id\n"
                                   "  WHERE j.procver_id=%(pv)s\n"
                                   "  ORDER BY b._table,j.priority DESC\n"
                                   ") subq\n"
                                   "GROUP BY _table",
                                   { 'pv': pvid } )
            retval['base_procvers'] = { r[0]: [ [ d, p ] for d, p in zip(r[1], r[2]) ] for r in rows }

            return retval


# ======================================================================

class BaseProcVer( BaseView ):
    def do_the_things( self, procver, table=None ):
        with db.DBCon() as con:
            try:
                pvid = db.BaseProcessingVersion.base_procver_id( procver, table )
            except Exception as ex:
                raise FASTDBWebException( str(ex) )

            row, _ = con.execute( "SELECT id,description,_table FROM base_processing_version WHERE id=%(pv)s",
                                  { 'pv': pvid } )
            if len(row) == 0:
                return f"Unknown base processing version {procver}", 422

            retval = { 'status': 'ok',
                       'id': row[0][0],
                       'description': row[0][1],
                       'table': row[0][2]
                      }
            rows, _ = con.execute( "SELECT description FROM processing_version p "
                                   "INNER JOIN base_procver_of_procver j ON p.id=j.procver_id "
                                   "WHERE j.base_procver_id=%(pv)s "
                                   "ORDER BY p.description",
                                   { 'pv': pvid } )
            retval['procvers'] = [ r[0] for r in rows ]

            return retval


# ======================================================================

class CountThings( BaseView ):
    def do_the_things( self, which, procver='default' ):
        global app

        synonyms = { 'rootid': [ 'root', 'rootid', 'rootobject', 'rootdiaobject' ],
                     'diaobject': [ 'object', 'diaobject'],
                     'diasource': [ 'source', 'diasource' ],
                     'diaforcedsource': [ 'forced', 'forcedsource' ,'diaforcedsource' ]
                    }

        thingtocount = None
        for k, v in synonyms.items():
            if which in v:
                thingtocount = k
                break
        if thingtocount is None:
            return f"Unknown thing to count: {which}", 422
        if thingtocount == 'diaobject':
            return "Counting diaobject not supported, count rootid", 422
        if not re.search( r'^[A-Za-z0-9_\-]+$', procver ):
            # Let's really make sure not to bobby tables
            return f"Invalid processing version {procver}, can only contain A-Z, a-z, 0-9, _, and -", 422

        estimate = False
        if flask.request.is_json:
            data = flask.request.json
            estimate = ( 'estimate' in data ) and ( data['estimate'] )
            if estimate:
                return "Estimate not supported", 422

        with db.DBCon() as dbcon:
            try:
                # Just make sure this is a know processing version, even though
                #   we aren't actually going to use it.
                # This is a necessary but not sufficient condition; the
                #   objstatscomb materialized view for this processing
                #   version must also exist.  TODO: think about whether
                #   I want to interpret aliases for finding the
                #   objstatscomb table.  Temptation: yes.
                _pvid = db.ProcessingVersion.procver_id( procver )
            except Exception as ex:
                raise FASTDBWebException( str(ex) )

            table = sql.Identifier( f"objstatscomb_{procver}" )

            if thingtocount == 'rootid':
                rows, _cols = dbcon.execute( sql.SQL("SELECT COUNT(rootid) FROM {table}").format( table=table ) )
            elif thingtocount == 'diasource':
                rows, _cols = dbcon.execute( sql.SQL("SELECT SUM(ndets) FROM {table}").format( table=table ) )
            elif thingtocount == 'diaforcedsource':
                rows, _cols = dbcon.execute( sql.SQL("SELECT SUM(nfrc) FROM {table}").format( table=table ) )
            else:
                return "This should never happen", 422
            count = rows[0][0]

            return { 'status': 'ok',
                     'table': thingtocount,
                     'isestimate': estimate,
                     'count': count }


# ======================================================================
# /getdiaobjectinfo
# /getdiaobjectinfo/<procver>
# /getdiaobjectinfo/<procver>/<objid>

class GetDiaObjectInfo( BaseView ):
    def do_the_things( self, procver=None, objid=None ):
        if flask.request.is_json:
            data = flask.request.json
            if not isinstance( data, dict ):
                raise FASTDBWebException( "POST data must be a JSON dict" )
            kwargs = copy.deepcopy( data )

            known_kwargs = { 'objectids', 'processing_version', 'position_processing_version',
                             'base_procvers', 'return_diaobject_positions' }
            unknown = set( kwargs.keys() ) - known_kwargs
            if len( unknown ) > 0:
                raise FASTDBWebException( f"Unknown data parameters: {unknown}" )
        else:
            kwargs = {}

        if 'objectids' in kwargs:
            if objid is not None:
                raise FASTDBWebException( "Error, object id given in both URL and body.  Only do one." )
            objid = kwargs['objectids']
            procver = ( procver if procver is not None
                        else kwargs['processing_version'] if 'processing_version' in kwargs
                        else 'default' )
            del kwargs['objectids']
        elif objid is None:
            # OK, calling semantics are kinda complicated here.  If no objids were specified
            #   in the data, and there was only one REST argument, then we actually assume
            #   it's an objid rather than a procver.
            objid = procver
            procver = 'default' if 'processing_version' not in kwargs else kwargs['processing_version']

        if 'processing_version' in kwargs:
            if ( procver is not None ) and ( kwargs['processing_version'] != procver ):
                raise FASTDBWebException( f"Conflicting processing versions; {procver} specified in the URL, "
                                          f"but {kwargs['processing_version']} passed in the body!" )
            else:
                procver = kwargs['processing_version']

        kwargs['processing_version'] = procver if procver is not None else 'default'

        try:
            return ltcv.get_object_infos( objid, return_format='json', **kwargs )
        except Exception as ex:
            raise FASTDBWebException( str(ex) )


# ======================================================================

class ObjectSearch( BaseView ):
    def do_the_things( self, processing_version='default' ):
        global app
        if not flask.request.is_json:
            raise FASTDBWebException( "POST data was not JSON; send search criteria as a JSON dict" )
        searchdata = flask.request.json

        FDBLogger.debug( f"ObjectSearch on processing version {processing_version} with search data {searchdata}" )
        try:
            return ltcv.object_search( processing_version, **searchdata )
        except Exception as ex:
            raise FASTDBWebException( str(ex) )


# **********************************************************************
# **********************************************************************
# **********************************************************************
# Configure and create the web app in global variable "app"

FDBLogger.multiprocessing_replace( pid=True )

app = flask.Flask(  __name__ )
# loglevel = logging.INFO
loglevel = logging.DEBUG
app.logger.setLevel( loglevel )
FDBLogger.setLevel( loglevel )

app.config.from_mapping(
    SECRET_KEY=_flask_session_secret_key,
    SESSION_COOKIE_PATH='/',
    SESSION_TYPE='filesystem',
    SESSION_PERMANENT=True,
    SESSION_USE_SIGNER=True,
    SESSION_FILE_DIR=config.sessionstore,
    SESSION_FILE_THRESHOLD=1000,
)

server_session = flask_session.Session( app )

rkauth_flask.RKAuthConfig.setdbparams(
    db_host=db.dbhost,
    db_port=db.dbport,
    db_name=db.dbname,
    db_user=db.dbuser,
    db_password=db.dbpasswd,
    email_from = config.emailfrom,
    email_subject = 'fastdb password reset',
    email_system_name = 'fastdb',
    smtp_server = config.smtpserver,
    smtp_port = config.smtpport,
    smtp_use_ssl = config.smtpusessl,
    smtp_username = config.smtpusername,
    smtp_password = config.smtppassword
)
app.register_blueprint( rkauth_flask.bp )

app.register_blueprint( dbapp.bp )
app.register_blueprint( ltcvapp.bp )
app.register_blueprint( spectrumapp.bp )


urls = {
    "/": MainPage,
    "/getprocvers": GetProcVers,
    "/procver/<procver>": ProcVer,
    "/baseprocver/<procver>": BaseProcVer,
    "/baseprocver/<procver>/<table>": BaseProcVer,
    "/count/<which>": CountThings,
    "/count/<which>/<procver>": CountThings,
    "/getdiaobjectinfo": GetDiaObjectInfo,
    "/getdiaobjectinfo/<procver>": GetDiaObjectInfo,
    "/getdiaobjectinfo/<procver>/<objid>": GetDiaObjectInfo,
    "/objectsearch": ObjectSearch,
    "/objectsearch/<processing_version>": ObjectSearch,
}

usedurls = {}
for url, cls in urls.items():
    if url not in usedurls.keys():
        usedurls[ url ] = 0
        name = url
    else:
        usedurls[ url ] += 1
        name = f'{url}.{usedurls[url]}'

    app.add_url_rule (url, view_func=cls.as_view(name), methods=['GET', 'POST'], strict_slashes=False )
