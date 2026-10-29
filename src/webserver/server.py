import re
import copy
import logging
import uuid
import textwrap

from psycopg import sql
import flask
import flask_session

import db
import ltcv
from util import FDBLogger, asUUID
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
                # Get the processing version
                procver = db.ProcessingVersion.get_procver( procver )
                # Bobby table check.  One that's not valid should never have passed in the first place,
                #  but be paranoid.
                if not re.search( r'^[A-Za-z0-9_\-]+$', procver.description ):
                    raise ValueError( f"Invalid processing version name \"{procver.description}\" for "
                                      f"processing version {procver.id}; should only include "
                                      f"A-Z, a-z, 0-9, _, and -" )
            except Exception as ex:
                raise FASTDBWebException( str(ex) )

            table = sql.Identifier( f"objstatscomb_{procver.description}" )

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


# ======================================================================

class CreateTag( BaseView ):
    def do_the_things( self, tag, description=None ):
        self.check_groups( [ 'admin', 'tag_create' ] )

        args = { 'tag': tag, 'description': description }
        if flask.request.is_json:
            args.update( flask.request.json )
        if set( args.keys() ) != { 'tag', 'description' }:
            return f"Unknown arguments: {set(args.keys()) - {'tag', 'description'}}", 422

        try:
            _ = asUUID( tag )
            return f"Error, {tag} is an invalid tag name, it can't look like a uuid.", 422
        except Exception:
            pass

        with db.DBCon() as dbcon:
            rows, _cols = dbcon.execute( sql.SQL( "SELECT id, tag FROM tags WHERE tag={tag}" )
                                         .format( tag=args['tag'] ) )
            if len(rows) > 0:
                return f"Tag {tag} already exists.", 422

            tagid = uuid.uuid4()
            dbcon.execute( sql.SQL( "INSERT INTO tags(id, tag, description) "
                                    "VALUES ({tagid}, {tag}, {description})" )
                           .format( tagid=tagid, tag=args['tag'], description=args['description'] ) )
            dbcon.commit()
            return { "status": "ok",
                     "id": tagid,
                     "tag": args['tag'],
                     "description": args['description']
                    }



# ======================================================================

class GetTag( BaseView ):
    def do_the_things( self, tag ):
        try:
            tagid = asUUID( tag )
            q = sql.SQL( "SELECT id, tag, description FROM tags WHERE id={tagid}" ).format( tagid=tagid )
        except Exception:
            q = sql.SQL( "SELECT id, tag, description FROM tags WHERE tag={tag}" ).format( tag=tag )

        with db.DBCon() as dbcon:
            rows, _cols = dbcon.execute( q )
            if len(rows) == 0:
                return f"Tag {tag} is not known.", 422
            elif len(rows) > 1:
                return f"Tag {tag} is multiply defined; this should never happen.", 422
            else:
                return { 'status': 'ok',
                         'id': str(rows[0][0]),
                         'tag': rows[0][1],
                         'description': rows[0][2]
                        }


# ======================================================================

class TagObjects( BaseView ):
    def do_the_things( self, tag ):
        self.check_groups( [ 'admin', 'tag_create' ] )

        if not flask.request.is_json:
            return "Error, must send JSON POST body with objects=list", 422
        data = flask.request.json
        if ( 'objects' not in data ) or ( not isinstance( data['objects'], list ) ):
            return "Error, must send JSON POST body with objects=list", 422

        ids_are_diaobjectid = ( 'ids_are_diaobjectid' in data ) and ( data['ids_are_diaobjectid'] )

        if ids_are_diaobjectid:
            q = sql.SQL( "SELECT rootid, diaobjectid FROM diaobject WHERE diaobjectid=ANY(ARRAY[{objids}])"
                        ).format( objids=sql.SQL(",").join( data['objects'] ) )
        else:
            q = sql.SQL( "SELECT id FROM root_diaobject WHERE id=ANY(ARRAY[{objids}]::uuid[])"
                         ).format( objids=sql.SQL(",").join( data['objects'] ) )

        with db.DBCon() as dbcon:
            tagobj = db.Tag.get( tag )
            if tagobj is None:
                return f"Unknown tag {tag}", 422

            rows, _cols = dbcon.execute( q )
            if len(rows) != len( data['objects'] ):
                return ( f"You passed {len(data['objects'])} object ids, but I only found "
                         f"{len(rows)} of those in the database" ), 422

            if ids_are_diaobjectid:
                rootids = list( set( row[0] for row in rows ) )
            else:
                rootids = [ row[0] for row in rows ]

            data = { 'tagid': [ tagobj.id ] * len(rootids), 'rootid': rootids }
            db.RootDiaObjectTag.bulk_insert_or_upsert( data, dbcon=dbcon )
            # That will have committed

        return { "status": "ok" }


# =====================================================================

class TagsForObject( BaseView ):
    def do_the_things( self, objid ):
        with db.DBCon() as dbcon:
            try:
                rootobj = db.RootDiaObject.get( objid, dbcon=dbcon )
            except Exception as ex:
                raise FASTDBWebException( str(ex) )
            if rootobj is None:
                return f"Unknown object {objid}", 422

            q = sql.SQL( "SELECT t.tag FROM tags t "
                         "INNER JOIN root_diaobject_tag r ON t.id=r.tagid "
                         "WHERE r.rootid={rootid}" ).format( rootid=rootobj.id )
            rows, _cols = dbcon.execute( q )

            return { "rootid": rootobj.id,
                     "tags": [ r[0] for r in rows ]
                    }


# ======================================================================

class ObjectsForTag( BaseView ):
    def do_the_things( self, tagid ):
        with db.DBCon() as dbcon:
            tagobj = db.Tag.get( tagid, dbcon=dbcon )
            if tagobj is None:
                return f"Unknown tag {tagid}", 422

            q = sql.SQL( "SELECT rootid FROM root_diaobject_tag WHERE tagid={tagid}" ).format( tagid=tagobj.id )
            rows, _cols = dbcon.execute( q )

            return { "tagid": tagobj.id,
                     "tag": tagobj.tag,
                     "rootids": [ r[0] for r in rows ]
                    }


# ======================================================================
class DeleteTag( BaseView ):
    def do_the_things( self, tag ):
        self.check_groups( [ 'admin', 'tag_create' ] )

        if ( ( not flask.request.is_json ) or ( not isinstance( flask.request.json, dict ) ) or
             ( 'I know what I am doing' not in flask.request.json) or
             ( not flask.request.json['I know what I am doing'] )
            ):
            return "Only do this if you know what you are doing", 422

        with db.DBCon() as dbcon:
            try:
                tagid = asUUID( tag )
                q = sql.SQL( "SELECT id, tag FROM tags WHERE id={tag}" ).format( tag=tagid )
            except Exception:
                q = sql.SQL( "SELECT id, tag FROM tags WHERE tag={tag}" ).format( tag=tag )

            rows, _cols = dbcon.execute( q )
            if len(rows) == 0:
                return f"Unknown tag {tag}", 422

            tagid = rows[0][0]
            tag = rows[0][1]

            dbcon.execute( sql.SQL( "DELETE FROM tags WHERE id={tagid} " ).format( tagid=tagid ) )
            dbcon.commit()

            return { "status": "tag deleted",
                     "tag": tag,
                     "tagid": tagid
                    }


# ======================================================================

class UntagObjects( BaseView ):
    def do_the_things( self, tag ):
        self.check_groups( [ 'admin', 'tag_create' ] )

        if not flask.request.is_json:
            return "Must POST JSON data with key objects, value list of object ids", 422

        data = flask.request.json
        if ( ( not isinstance( data, dict ) ) or ( 'objects' not in data ) or
             ( not isinstance( data['objects'], list ) ) ):
            return "Must POST JSON data with key objects, value list of object ids", 422

        if ( 'I know what I am doing' not in data ) or ( not data['I know what I am doing'] ):
            return "Only do this if you know what you are doing.", 422

        ids_are_diaobjectid = ( 'ids_are_diaobjectid' in data ) and ( data['ids_are_diaobjectid'] )
        ok_missing = ( 'ok_missing' in data ) and ( data['ok_missing'] )

        with db.DBCon() as dbcon:
            tagobj = db.Tag.get( tag, dbcon=dbcon )
            if tagobj is None:
                return f"Unknown tag {tag}", 422

            if ( not ok_missing ) or ids_are_diaobjectid:
                if ids_are_diaobjectid:
                    compdex = 1
                    q = sql.SQL( textwrap.dedent(
                        """\
                        SELECT o.rootid, o.diaobjectid FROM root_diaobject_tag rdt
                        INNER JOIN diaobject o ON o.rootid=rdt.rootid
                        WHERE o.diaobjectid=ANY(ARRAY[{objids}])
                          AND rdt.tagid={tagid}
                        """
                    ) ).format( tagid=tagobj.id, objids=sql.SQL(",").join( data['objects'] ) )
                else:
                    compdex = 0
                    q = sql.SQL( textwrap.dedent(
                        """\
                        SELECT rootid FROM root_diaobject_tag
                        WHERE tagid={tagid}
                          AND rootid=ANY(ARRAY[{objids}]::uuid[])
                        """
                    ) ).format( tagid=tagobj.id, objids=sql.SQL(",").join( data['objects'] ) )

                try:
                    rows, _cols = dbcon.execute( q )
                except Exception as ex:
                    return f"Database query error: {ex}", 422

                if not ok_missing:
                    foundobjs = set( row[compdex] for row in rows )
                    wantedobjs = set( asUUID( data['objects'] ) )
                    if ( foundobjs != wantedobjs ):
                        return f"The following objects weren't in tag {tag}: {wantedobjs-foundobjs}", 422

                rootids = [ row[0] for row in rows ]

            else:
                rootids = asUUID( data['objects'] )

            if len(rootids) == 0:
                if ( len( data['objects'] ) > 0 ) and not ok_missing:
                    return "None of the objects you specified for were in tag {tag}", 422
                else:
                    return { "status": "ok" }

            dbcon.execute( sql.SQL( textwrap.dedent(
                """\
                DELETE FROM root_diaobject_tag
                WHERE tagid={tagid}
                  AND rootid=ANY(ARRAY[{rootids}])
                """
            ) ).format( tagid=tagobj.id, rootids=sql.SQL(",").join(rootids) ) )
            dbcon.commit()

            return { "status": "ok" }



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
    usegroups = True,
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
    "/createtag/<tag>": CreateTag,
    "/createtag/<tag>/<description>": CreateTag,
    "/tagobjects/<tag>": TagObjects,
    "/tagsforobject/<objid>": TagsForObject,
    "/objectsfortag/<tagid>": ObjectsForTag,
    "/deletetag/<tag>": DeleteTag,
    "/untagobjects/<tag>": UntagObjects
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
