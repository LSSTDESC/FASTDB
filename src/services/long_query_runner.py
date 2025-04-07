import sys
import io
import logging
import datetime
import pathlib
import time
import json
import argparse
import multiprocessing
from contextlib import contextmanager

import pandas
import psycopg
import psycopg.rows

import config

_loglevel = logging.DEBUG


class QueryRunner:
    def __init__( self ):
        self.outdir = pathlib.Path( "/query_results" )
        self.logger = logging.getLogger( "long_query_runner" )
        _logout = logging.StreamHandler( sys.stderr )
        self.logger.addHandler( _logout )
        _formatter = logging.Formatter( '[%(asctime)s - %(levelname)s] - %(message)s', datefmt='%Y-%m-%d %H:%M:%S' )
        _logout.setFormatter( _formatter )
        self.logger.propagate = False
        self.logger.setLevel( _loglevel )

        self.sleeptime = 10

        # Database settings.
        self.dbname = config.dbdatabase
        self.dbhost = config.dbhost
        self.dbport = config.dbport
        self.dbuser = config.dbuser
        with open( config.dbpasswdfile, "r" ) as ifp:
            self.dbpswd = ifp.readline().strip()
        # If the database was set up properly, the user
        #   postgres_ro has readonly access to all the
        #   tables, with the password at the file below:
        self.rodbuser = 'postgres_ro'
        with open( "/secrets/postgres_ro_password" ) as ifp:
            self.rodbpasswd = ifp.readline().strip()

    @contextmanager
    def rwconn( self ):
        try:
            conn = psycopg.connect( host=self.dbhost, port=self.dbport, dbname=self.dbname,
                                    user=self.dbuser, password=self.dbpswd )
            yield conn
        finally:
            conn.rollback()
            conn.close()

    @contextmanager
    def conn( self ):
        try:
            conn = psycopg.connect( host=self.dbhost, port=self.dbport, dbname=self.dbname,
                                    user=self.rodbuser, password=self.rodbpasswd )
            yield conn
        finally:
            conn.rollback()
            conn.close()


    def prune_old_query_results( self, days=7, purgedb=True, errored=True ):
        """Delete query results that are more than days old."""

        since = datetime.datetime.now( tz=datetime.UTC ) - datetime.timedelta( days=days )
        with self.rwconn() as conn:
            cursor = conn.cursor()
            q = "SELECT queryid FROM query_queue WHERE finished<%(since)s"
            if errored:
                q += " OR (error AND started<%(since)s)"
            cursor.execute( q, { 'since':since } )
            rows = cursor.fetchall()
            for row in rows:
                qid = row[0]
                self.logger.info( f"Pruning query {qid}" )
                outf = self.outdir / str(qid)
                if outf.is_file():
                    outf.unlink()
                if purgedb:
                    cursor.execute( "DELETE FROM query_queue WHERE queryid=%(id)s", { 'id': qid } )

            conn.commit()


    def get_queued_query( self ):
        with self.rwconn() as conn:
            cursor = conn.cursor( row_factory=psycopg.rows.dict_row )
            cursor.execute( "LOCK TABLE query_queue" )
            cursor.execute( "SELECT * FROM query_queue WHERE started IS NULL ORDER BY submitted" )
            rows = cursor.fetchall()
            if len(rows) == 0:
                return None

            self.logger.info( f"Claiming query request {rows[0]['queryid']}" )
            cursor.execute( "UPDATE query_queue SET started=%(t)s WHERE queryid=%(id)s",
                            { 't': datetime.datetime.now( tz=datetime.UTC ),
                              'id': rows[0]['queryid'] } )
            conn.commit()

            cursor.execute( "SELECT * FROM query_queue WHERE queryid=%(id)s", { 'id': rows[0]['queryid'] } )
            rows = cursor.fetchall()
            return dict( rows[0] )


    def run_query( self, queryinfo ):
        queryid = queryinfo['queryid']
        try:
            # Convert the subdict text entries into dictionaries.
            # Convert any lists in queryinfo subdict to tuples, because
            #   psycopg is very particular.
            queries = queryinfo['queries']
            subdicts = []
            for i, subdict in enumerate( queryinfo['subdicts'] ):
                if isinstance( subdict, str ):
                    subdict = json.loads( queryinfo['subdicts'][i] )
                if not isinstance( subdict, dict ):
                    raise TypeError( f"Subdict {i} is not a dictionary, it's a {type(subdict)}" )
                for key in subdict.keys():
                    if isinstance( subdict[key], list ):
                        subdict[key] = tuple( subdict[key] )
                subdicts.append( subdict )

            with self.rwconn() as conn:
                cursor = conn.cursor()
                cursor.execute( "UPDATE query_queue SET started=%(t)s WHERE queryid=%(id)s",
                                { 'id': queryid, 't': datetime.datetime.now(tz=datetime.UTC) } )
                conn.commit()

            # Want to use a readonly connection to the database because
            #   this function will be running queries submitted by users
            #   over the wide scary Internet.
            with self.conn() as conn:
                self.logger.info( f"Starting {len(queries)} queries for {queryid}" )
                strio = io.StringIO()
                for i, (query, subdict) in enumerate( zip( queries, subdicts ) ):
                    strio.write( f"   {i:3d}: {query}   ;   subdict={subdict}\n" )
                self.logger.debug( f"Queries:\n{strio.getvalue()}" )

                cursor = conn.cursor()
                for i, (query, subdict_text) in enumerate( zip( queries, subdicts ) ):
                    try:
                        self.logger.debug( f"Starting query {i} of {len(queries)}" )
                        cursor.execute( query, subdict )
                    except Exception as e:
                        self.logger.exception( f"Exception running query {i} of {queryid}: {e}" )
                        raise

                self.logger.info( f"Done with queries for {queryid}, fetching." )
                columns = [ d.name for d in cursor.description ]
                rows = cursor.fetchall()

            self.logger.info( f"Done fetching for {queryid}, saving" )
            if ( queryinfo['format'] == 'csv' ) or ( queryinfo['format'] == 'pandas' ):
                df = pandas.DataFrame( rows, columns=columns )
                if queryinfo['format'] == 'pandas':
                    df.to_pickle( self.outdir / str(queryid) )
                else:
                    df.to_csv( self.outdir / str(queryid) )

            elif ( queryinfo['format'] == 'numpy' ):
                raise NotImplementedError( "numpy return format isn't implemented yet" )

            self.logger.info( f"Done saving {queryid}" )
            with self.rwconn() as conn:
                cursor = conn.cursor()
                cursor.execute( "UPDATE query_queue SET finished=%(t)s WHERE queryid=%(id)s",
                                { 'id': queryid, 't': datetime.datetime.now(tz=datetime.UTC) } )
                conn.commit()

        except Exception as ex:
            with self.rwconn() as conn:
                cursor = conn.cursor()
                cursor.execute( "UPDATE query_queue SET error=TRUE, errortext=%(txt)s WHERE queryid=%(id)s",
                                { 'id': queryid, 'txt': str(ex) } )
                conn.commit()
            return


    def query_loop( self, sleeptime ):
        me = multiprocessing.current_process()
        self.logger = logging.getLogger( me.name )
        logout = logging.StreamHandler( sys.stderr )
        self.logger.addHandler( logout )
        formatter = logging.Formatter( f'[%(asctime)s - {me.name} - %(levelname)s] - %(message)s',
                                       datefmt='%Y-%m-%d %H:%M:%S' )
        logout.setFormatter( formatter )
        self.logger.propagate = False
        self.logger.setLevel( _loglevel )

        self.logger.info( f"Process {me.name} ({me.pid}) starting." )
        while True:
            queryinfo = self.get_queued_query()
            if queryinfo is None:
                time.sleep( sleeptime )
            else:
                self.run_query( queryinfo )


    def __call__( self ):
        parser = argparse.ArgumentParser( 'long_query_runner', description="Run long queryies from query_queue",
                                          formatter_class=argparse.ArgumentDefaultsHelpFormatter )
        parser.add_argument( '-o', '--once', default=False, action='store_true',
                             help="Just run at most one query" )
        parser.add_argument( '-l', '--loop', default=False, action='store_true',
                             help="Run the check/run query loop" )
        parser.add_argument( '-s', '--sleep-time', default=10, type=int,
                             help=( "How many seconds to sleep between looking to see if there are queries to do "
                                    "(default 10)" ) )
        parser.add_argument( '-n', '--num-runners', default=10, type=int,
                             help=( "How many queries to run simutalenously (default 10)" ) )
        parser.add_argument( '-p', '--prune', default=None, type=float,
                             help=( "Prune queries older than this many days.  It probably doesn't "
                                    "make sense to use this with --loop" ) )
        args = parser.parse_args()

        if args.prune is not None:
            self.prune_old_query_results( args.prune )

        if args.once:
            if args.loop:
                self.logger.warning( "Both --once and --loop given, only running one query (ignoring --loop)." )
            queryinfo = self.get_queued_query()
            if queryinfo is None:
                self.logger.info( "No queries to run." )
            else:
                self.run_query( queryinfo )
            return

        elif args.loop:
            if ( args.num_runners < 1 ) or ( args.num_runners > 20 ):
                raise ValueError( "num-runners must be >=1 and <= 20." )
            if args.num_runners == 1:
                self.logger.info( "Starting infinite loop to look for and run queries." )
                self.query_loop( args.sleep_time )
            else:
                self.logger.info( f"Starting {args.num_runners} processes to go into infinite loops "
                                  f"looking for and running queries." )
                pool = multiprocessing.Pool( args.num_runners )
                for i in range( args.num_runners ):
                    self.logger.info( f"...starting job {i}...." )
                    pool.apply_async( self.query_loop, [ args.num_runners ] )
                pool.close()
                self.logger.info( "Done starting query runners, joining pool." )
                pool.join()

        else:
            if args.prune is None:
                raise ValueError( "Doing nothing; specify either --prune, --once, or --loop )" )


# ======================================================================

def main():
    qr = QueryRunner()
    qr()


if __name__ == "__main__":
    main()
