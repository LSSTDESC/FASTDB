from types import SimpleNamespace
import simplejson
import copy
import re

import flask
import flask.views

from db import DB
import util
from util import FDBLogger


# ======================================================================

class FASTDBWebException( RuntimeError ):
    def __init__( self, *args, returncode=422, **kwargs ):
        FDBLogger.error( f"Creating a FASTDBWebException with returncode {422}, args {args}, kwargs {kwargs}" )
        self.returncode = returncode
        super().__init__( *args, **kwargs )


# ======================================================================

class BaseView( flask.views.View ):
    """A BaseView that all other views can be based on.

    If the view doesn't override dispatch_request, then it must define a
    function do_the_things.  That should return a dict, list, string,
    tuple, or ...something else.

    If it returns a dict or a list, the web server will send to the
    client application/json with status 200. If the result is a string,
    it the web server will send to the client text/plain with status
    200.  If it's a tuple, just let Flask deal with that tuple to figure
    out what the web server should send to the client.  Otherwise, the
    web server will sendn to the client application/octet-stream with
    status 200.

    Subclasses that do not override dispatch_request do not need to call
    check_auth.  However, if they do override it, they should call that
    if the results shouldn't be sent back to an unauthenticated user.

    """

    _admin_required = False

    def __init__( self, *args, **kwargs ):
        super().__init__( *args, **kwargs )

    def argstr_to_args( self, argstr, initargs={} ):
        """Parse argstr as a bunch of /kw=val to a dictionary, update with request body if it's json."""

        args = copy.deepcopy( initargs )
        if argstr is not None:
            for arg in argstr.split("/"):
                match = re.search( '^(?P<k>[^=]+)=(?P<v>.*)$', arg )
                if match is None:
                    FDBLogger( f"error parsing url argument {arg}, must be key=value" )
                    raise FASTDBWebException( f'error parsing url argument {arg}, must be key=value' )
                args[ match.group('k') ] = match.group('v')
        if flask.request.is_json:
            args.update( flask.request.json )
        return args

    def check_auth( self ):
        self.username = flask.session['username'] if 'username' in flask.session else '(None)'
        self.displayname = flask.session['userdisplayname'] if 'userdisplayname' in flask.session else '(None)'
        self.authenticated = ( 'authenticated' in flask.session ) and flask.session['authenticated']
        self.user = None
        if self.authenticated:
            with DB() as conn:
                cursor = conn.cursor()
                cursor.execute( "SELECT id,username,displayname,email FROM authuser WHERE username=%(username)s",
                                {'username': self.username } )
                rows = cursor.fetchall()
                if len(rows) > 1:
                    self.authenticated = False
                    raise RuntimeError( f"Error, more than one {self.username} in database, "
                                        f"this should never happen." )
                if len(rows) == 0:
                    self.authenticated = False
                    raise ValueError( f"Error, failed to find user {self.username} in database" )
                row = rows[0]
                self.user = SimpleNamespace( id=row[0], username=row[1], displayname=row[2], email=row[3] )
                # Verify that session displayname and database displayname match?  Eh.  Whatevs.
        return self.authenticated

    def dispatch_request( self, *args, **kwargs ):
        if not self.check_auth():
            return "Not logged in", 500
        if ( self._admin_required ) and ( not self.user.isadmin ):
            return "Action requires admin", 500
        try:
            retval = self.do_the_things( *args, **kwargs )
            rethdrs = None
            httpcode = None
            if isinstance( retval, tuple ):
                if len(retval) >= 3:
                    rethdrs = retval[2]
                    if not isinstance( rethdrs, dict ):
                        raise TypeError( f"Invalid return headers {rethdrs}, should be a dict." )
                if len(retval) >= 2:
                    httpcode = int( retval[1] )
                retval = retval[0]

            httpcode = 200 if httpcode is None else httpcode

            if isinstance( retval, dict ) or isinstance( retval, list ):
                rethdrs = {} if rethdrs is None else rethdrs
                if 'Content-Type' in rethdrs:
                    if rethdrs['Content-Type'] != 'application/json':
                        raise FASTDBWebException( f"Server error, tried to return a dict or list with "
                                                  f"Content-Type {rethdrs['Content-Type']}, but it should be "
                                                  f"application/json" )
                else:
                    rethdrs['Content-Type'] = 'application/json'

                # I don't like this whole "it's a global variable, but, hey, you're good, it's
                #   what you want inside your object" thing, but whatever, it's what flask does.
                if flask.request.headers.get( 'Fastdb-Stringifyints' ) is not None:
                    # ...this is for Javascript, which will read JSON and turn all integers
                    #   into doubles... thereby destroying 64-bit integers.  The fastdb
                    #   javascript code sets the Fastdb-Stringifyints header to tell us
                    #   to send integers back as strings so they won't get destroyed.
                    FDBLogger.warning( "Stringifying integers" )
                    retval = util.stringify_integers( retval )
                # Can't just use the default JSON handling, because it
                #   writes out NaN which is not standard JSON and which
                #   the javascript JSON parser chokes on.  simplejson
                #   provides ignore_nan to convert NaN and inf to null,
                #   which is more strict JSON compliant.  Also take the
                #   opportunity to convert numpy types and UUIDs into
                #   types JSON can handle, so that we don't have to do
                #   that in every handler.
                # Ponder if perhaps we should have the option to return
                #   BSON or something similar that's
                #   binary-encoded... might be useful for python apps
                #   calling the api.
                retval = simplejson.dumps( retval, ignore_nan=True, default=util.fastdb_json_default )

            elif rethdrs is None:
                if isinstance( retval, str ):
                    rethdrs = { 'Content-Type': 'text/plain; charset=utf-8' }
                else:
                    rethdrs = { 'Content-Type': 'application/octet-stream' }

            return ( retval, httpcode, rethdrs )

        except Exception as ex:
            # sio = io.StringIO()
            # traceback.print_exc( file=sio )
            # FDBLogger.debug( sio.getvalue() )
            FDBLogger.exception( str(ex) )
            if isinstance( ex, FASTDBWebException ):
                return str(ex), ex.returncode
            else:
                return str(ex), 500
