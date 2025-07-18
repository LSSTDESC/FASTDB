import logging

import flask
import flask_session

import db
import ltcv
import webserver.rkauth_flask as rkauth_flask
import webserver.dbapp as dbapp
import webserver.ltcvapp as ltcvapp
import webserver.spectrumapp as spectrumapp
from webserver.baseview import BaseView

# ======================================================================
# Global config

import config
with open( config.secretkeyfile ) as ifp:
    _flask_session_secret_key = ifp.readline().strip()


# ======================================================================

class MainPage( BaseView ):
    def dispatch_request( self ):
        app.logger.error( "Hello error." )
        app.logger.warning( "Hello warning." )
        app.logger.info( "Hello info." )
        app.logger.debug( "Hello debug." )
        return flask.render_template( "fastdb_webap.html" )


# ======================================================================

class GetProcVers( BaseView ):
    def do_the_things( self ):
        # global app

        with db.DB() as con:
            cursor = con.cursor()
            cursor.execute( "SELECT description FROM processing_version" )
            pvrows = cursor.fetchall()
            cursor.execute( "SELECT description FROM processing_version_alias" )
            alrows = cursor.fetchall()

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

        pvid = None
        with db.DB() as con:
            cursor = con.cursor()
            try:
                intpv = int(procver)
                cursor.execute( "SELECT id FROM processing_version WHERE id=%(pv)s", { 'pv': intpv } )
                rows = cursor.fetchall()
                if len(rows) > 0:
                    pvid = rows[0][0]
            except Exception:
                pass
            if pvid is None:
                cursor.execute( "SELECT id FROM processing_version WHERE description=%(pv)s", { 'pv': procver } )
                row = cursor.fetchone()
                if row is not None:
                    pvid = row[0]
            if pvid is None:
                cursor.execute( "SELECT id FROM processing_version_alias "
                                "WHERE description=%(pv)s ORDER BY description ",
                                { 'pv': procver } )
                row = cursor.fetchone()
                if row is not None:
                    pvid = row[0]

            if pvid is None:
                return f"Unknonw processing version {procver}", 500

            retval = { 'status': 'ok', 'id': None, 'description': None, 'aliases': [] }
            cursor.execute( "SELECT id,description FROM processing_version WHERE id=%(pv)s", { 'pv': pvid } )
            row = cursor.fetchone()
            retval['id'] = row[0]
            retval['description'] = row[1]
            cursor.execute( "SELECT description FROM processing_version_alias WHERE id=%(pv)s", { 'pv': pvid } )
            rows = cursor.fetchall()
            retval['aliases'] = [ r[0] for r in rows ]
            return retval




# ======================================================================

class CountThings( BaseView ):
    def do_the_things( self, which, procver ):
        global app

        tablemap = { 'object': 'diaobject',
                     'source': 'diasource',
                     'forced': 'diaforcedsource' }
        if which not in tablemap:
            return f"Unknown thing to count: {which}", 500
        table = tablemap[ which ]

        with db.DB() as dbcon:
            cursor = dbcon.cursor()
            cursor.execute( "SELECT id FROM processing_version WHERE description=%(pv)s",
                            { 'pv': procver } )
            rows = cursor.fetchall()
            if len(rows) == 0:
                cursor.execute( "SELECT id FROM processing_version_alias WHERE description=%(pv)s",
                                { 'pv': procver } )
                rows = cursor.fetchall()
                if len(rows) == 0:
                    return f"Unknown processing version {procver}", 500
            pvid = rows[0][0]

            cursor.execute( f"SELECT COUNT(*) FROM {table} WHERE processing_version=%(pv)s",
                            { 'pv': pvid } )
            rows = cursor.fetchall()

        return { 'status': 'ok',
                 'which': which,
                 'count': rows[0][0]
                }


# ======================================================================

class ObjectSearch( BaseView ):
    def do_the_things( self, processing_version ):
        global app
        if not flask.request.is_json:
            raise TypeError( "POST data was not JSON; send search criteria as a JSON dict" )
        searchdata = flask.request.json

        rval = ltcv.object_search( processing_version, return_format='json', **searchdata )

        # JSON dysfunctionality... convert to strings and back,
        # javascript may decide to interpret bigints as doubles, thereby
        # losing necessary precision.  Convert all bigints to strings.
        # Right now, that means listing the possible columns here.  There
        # must be a better way... but if I want to interpret it in javascriptlk
        # there probably isn't.
        bigints = [ 'diaobjectid' ]
        for k in bigints:
            rval[k] = [ str(v) for v in rval[k] ]
        
        return rval


# **********************************************************************
# **********************************************************************
# **********************************************************************
# Configure and create the web app in global variable "app"


app = flask.Flask(  __name__ )
# app.logger.setLevel( logging.INFO )
app.logger.setLevel( logging.DEBUG )

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
    "/count/<which>/<procver>": CountThings,
    "/objectsearch/<processing_version>": ObjectSearch
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
