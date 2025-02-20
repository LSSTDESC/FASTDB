import logging

import flask
import flask_session

from baseview import BaseView
import rkauth_flask
import dbapp
import db

# ======================================================================
# Global config

import apconfig
with open( apconfig.secretkeyfile ) as ifp:
    _flask_session_secret_key = ifp.readline().strip()


# ======================================================================

class MainPage( BaseView ):
    def dispatch_request( self ):
        return flask.render_template( "fastdb_webap.html" )


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
    SESSION_FILE_DIR=apconfig.sessionstore,
    SESSION_FILE_THRESHOLD=1000,
)

server_session = flask_session.Session( app )

rkauth_flask.RKAuthConfig.setdbparams(
    db_host=db.dbhost,
    db_port=db.dbport,
    db_name=db.dbname,
    db_user=db.dbuser,
    db_password=db.dbpasswd,
    email_from = apconfig.emailfrom,
    email_subject = 'fastdb password reset',
    email_system_name = 'fastdb',
    smtp_server = apconfig.smtpserver,
    smtp_port = apconfig.smtpport,
    smtp_use_ssl = apconfig.smtpusessl,
    smtp_username = apconfig.smtpusername,
    smtp_password = apconfig.smtppassword
)
app.register_blueprint( rkauth_flask.bp )

app.register_blueprint( dbapp.bp )

urls = {
    "/": MainPage,
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
