import io
import uuid
import datetime

import flask
import psycopg

import db
from util import asUUID
from webserver.baseview import BaseView


# ======================================================================]
# Utility functions used by multiple views

def _extract_queries( data ):
    if 'query' not in data:
        raise ValueError( "POST data must include 'query'" )

    if isinstance( data['query'], list ):
        queries = data['query']
    elif not isinstance( data['query'], str ):
        raise TypeError( "query must be either a list of strings or a string" )
    else:
        queries = [ data['query'] ]
    if not all( [ isinstance(q, str) for q in queries  ] ):
        raise TypeError( "queries must all be strings" )

    if 'subdict' not in data:
        subdicts = [ {} for i in range(len(queries)) ]
    else:
        if isinstance( data['subdict'], list ):
            subdicts = data['subdict']
        elif not isinstance( data['subdict'], dict ):
            raise TypeError( "subdict must be either a list of dicts or a dict" )
        else:
            subdicts = [ data['subdict'] ]
        if len(subdicts) != len(queries):
            raise ValueError( "number of queries and subdicts must match" )
        if not all( [ isinstance(s, dict) for s in subdicts ] ):
            raise TypeError( "subdicts must all be dicts" )

    # Have to convert lists to tuples in the substitution dictionaries
    for subdict in subdicts:
        for key in subdict.keys():
            if isinstance( subdict[key], list ):
                subdict[key] = tuple( subdict[key] )

    # Look for a return format
    return_format = 0
    if 'return_format' in data:
        return_format = data['return_format']

    return queries, subdicts, return_format


def _dbcon():
    # TODO : make these configurable?
    dbuser = "postgres_ro"
    pwfile = "/secrets/postgres_ro_password"
    with open( pwfile ) as ifp:
        password = ifp.readline().strip()

    conn = psycopg.connect( dbname=db.dbname, host=db.dbhost, port=db.dbport, user=dbuser, password=password )

    return conn


# ======================================================================
# Interface for short SQL queries that return results directly.

class RunSQLQuery( BaseView ):
    def do_the_things( self ):
        logger = flask.current_app.logger

        if not flask.request.is_json:
            raise TypeError( "POST data was not JSON" )
        data = flask.request.json

        try:
            queries, subdicts, return_format = _extract_queries( data )

            try:
                conn = _dbcon()
                cursor = conn.cursor()

                logger.debug( "Starting query sequence" )
                logger.debug( f"queries={queries}" )
                logger.debug( f"subdicts={subdicts}" )

                for query, subdict in zip( queries, subdicts ):
                    logger.debug( f"Query is {query}, subdict is {subdict}, "
                                  f"user is {flask.session['useruuid']} ({flask.session['username']})" )
                    cursor.execute( query, subdict )
                    logger.debug( 'Query done' )

                logger.debug( "Fetching" )
                columns = [ c.name for c in cursor.description ]
                rows = cursor.fetchall()

            finally:
                conn.rollback()
                conn.close()

            if return_format == 0:
                retval = { 'status': 'ok',
                           'rows': [ { c: r[i] for i, c in enumerate(columns) } for r in rows ]
                          }
            elif return_format == 1:
                retval = { 'status': 'ok',
                           'data': { c: [ r[i] for r in rows ] for i, c in enumerate(columns) }
                          }
            else:
                raise ValueError( f"Unknown return format {return_format}" )

            logger.debug( f"Returning {len(rows)} rows from query sequence." )
            return retval

        except Exception as ex:
            logger.exception( ex )
            return { 'status': 'error', 'error': str(ex) }



# ======================================================================
# Submit a long SQL query for background running

class SubmitLongSQLQuery( BaseView ):
    def do_the_things( self ):
        logger = flask.current_app.logger

        if not flask.request.is_json:
            raise TypeError( "POST data was not JSON" )
        data = flask.request.json

        try:
            queries, subdicts, return_format = _extract_queries( data )

            if return_format == 0:
                return_format = 'csv'
            if return_format not in [ 'csv', 'pandas', 'numpy' ]:
                raise ValueError( f"Unknown format {return_format}" )

            queryid = uuid.uuid4()
            strio = io.StringIO()
            strio.write( f"Queueing query {queryid} with {len(queries)} queries "
                         f"for user {flask.session['useruuid']} ({flask.session['username']})\n" )
            for q, s in zip( queries, subdicts ):
                strio.write( f"  ====> query={q}   ;   subdict={s}\n" )
            logger.debug( strio.getvalue() )

            qq = db.QueryQueue( queryid = queryid,
                                userid = asUUID( flask.session['useruuid'] ),
                                submitted = datetime.datetime.now( tz=datetime.UTC ),
                                queries = queries,
                                subdicts = subdicts,
                                format = return_format )
            qq.insert()

            return { 'status': 'ok', 'queryid': str(queryid) }

        except Exception as ex:
            logger.exception( ex )
            return { 'status': 'error', 'error': str(ex) }


# ======================================================================
# Check status of long running SQL query

class CheckLongSQLQuery( BaseView ):
    def do_the_things( self, queryid ):
        logger = flask.current_app.logger
        try:
            qq = db.QueryQueue.get( queryid )
            if qq is None:
                raise ValueError( f"Unknown query {queryid}" )

            response = { 'queryid': queryid,
                         'queries': qq.queries,
                         'subdicts': qq.subdicts,
                         'submitted': qq.submitted.isoformat() }
            if qq.error:
                response.update( { 'status': 'error',
                                   'error': qq.errortext } )
                if qq.finished is not None:
                    response['finished'] == qq.finished.isoformat()
                if qq.started is not None:
                    response['started'] = qq.started.isoformat()

            elif qq.finished is not None:
                response.update( { 'status': 'finished',
                                   'started': qq.started.isoformat(),
                                   'finished': qq.finished.isoformat() } )

            elif qq.started is not None:
                response.update( { 'status': 'started',
                                   'started': qq.started.isoformat() } )

            else:
                response.update( { 'status': 'queued' } )

            return response

        except Exception as ex:
            logger.exception( ex )
            return { 'status': 'error', 'error': str(ex) }


# ======================================================================
# Get results of long SQL query

class GetLongSQLQueryResults( BaseView ):
    def do_the_things( self, queryid ):
        logger = flask.current_app.logger
        try:
            qq = db.QueryQueue.get( queryid )
            if qq is None:
                raise ValueError( f"Unknown query {queryid}" )
            if qq.error:
                raise RuntimeError( f"Query {queryid} errored out: {qq.errortext}" )
            if qq.finished is None:
                if qq.started is None:
                    raise RuntimeError( f"Query {queryid} hasn't started yet" )
                else:
                    raise RuntimeError( f"Query {queryid} hasn't finished yet" )

            if ( qq.format == "numpy" ) or ( qq.format == "pandas" ):
                with open( f"/query_results/{str(qq.queryid)}", "rb" ) as ifp:
                    return ifp.read()
            elif qq.format == "csv":
                with open( f"/query_results/{str(qq.queryid)}", "r" ) as ifp:
                    return ifp.read(), 200, { 'Content-Type': 'text/csv; charset=utf-8' }
            else:
                raise ValueError( f"Query {queryid} is finished, but results are in an unknown format {qq.format}" )

        except Exception as ex:
            logger.exception( ex )
            raise


# ======================================================================
# ======================================================================
# ======================================================================

bp = flask.Blueprint( 'dbapp', __name__, url_prefix='/db' )

urls = {
    "runsqlquery": RunSQLQuery,
    "submitsqlquery": SubmitLongSQLQuery,
    "checksqlquery/<queryid>": CheckLongSQLQuery,
    "getsqlqueryresults/<queryid>": GetLongSQLQueryResults,
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
