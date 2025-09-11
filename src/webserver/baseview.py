import uuid
from types import SimpleNamespace
import simplejson
import numbers

import flask
import flask.views

from db import DB
from util import FDBLogger


# ======================================================================
# Encoder for simplejson
#
# Handels UUID (which is why it was named this, as it originally only did this),
#   floats, and integers, converting numpy types to regular floats and ints

class UUIDJSONEncoder( simplejson.JSONEncoder ):
    def default( self, obj ):
        if isinstance( obj, uuid.UUID ):
            return str(obj)
        elif isinstance( obj, numbers.Integral ):
            return int(obj)
        elif isinstance( obj, numbers.Real ):
            return float(obj)
        else:
            return super().default( obj )


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
            # Can't just use the default JSON handling, because it
            #   writes out NaN which is not standard JSON and which
            #   the javascript JSON parser chokes on.  Sigh.
            if isinstance( retval, dict ) or isinstance( retval, list ):
                return ( simplejson.dumps( retval, ignore_nan=True, cls=UUIDJSONEncoder ),
                         200, { 'Content-Type': 'application/json' } )
            elif isinstance( retval, str ):
                return retval, 200, { 'Content-Type': 'text/plain; charset=utf-8' }
            elif isinstance( retval, tuple ):
                return retval
            else:
                return retval, 200, { 'Content-Type': 'application/octet-stream' }
        except Exception as ex:
            # sio = io.StringIO()
            # traceback.print_exc( file=sio )
            # FDBLogger.debug( sio.getvalue() )
            FDBLogger.exception( str(ex) )
            return str(ex), 500
