# A client for connecting to a FASTDB server from python
#
# free software, available under the LBNL variant 3-clause BSD license ; see LICENSE
#
# Based on rkauthclient from https://github.com/rknop/rkwebutil/blob/master/rkauth_client.py

import sys
import os
import io
import pathlib
import time
import uuid
import requests
import binascii
import logging
import configparser

from Crypto.Protocol.KDF import PBKDF2
from Crypto.Hash import SHA256
from Crypto.Cipher import AES, PKCS1_OAEP
from Crypto.PublicKey import RSA


class FASTDBClient:
    short_query_url = 'db/runsqlquery/'
    submit_long_query_url = 'db/submitsqlquery/'
    check_long_sql_query_url = 'db/checksqlquery/'
    get_long_sql_query_results_url = 'db/getsqlqueryresults/'

    def __init__( self, server, username=None, password=None, login=True,
                  verify=None, retries=None, retrysleep=None, retrysleepinc=None,
                  logger=None, info=True, debug=False, verify_ini_permissions=True ):
        """Create a client to connect to a fastdb server,

        After making an object, use its various methods (ROB LIST
        EXAMPLES) to communicate.  You can also get the logged-in python
        requests object directly via the .req property after calling
        .verify_logged_in().

        Parameters
        ----------
          server: str
            If this starts with "http:" or "https:", then it is base url
            of the server's webap.  Otherwise, it's the name of a block
            in ~/.fastdb.ini

          username: str
            Username; required if server is a URL.  Otherwise, if not
            None, overrides what's read from ~/.fastdb.ini.

          password: str
            Password; requried if server is a URL.  Otherwise, if not
            None, overrides what's read from ~/.fastdb.ini.

          login: bool, default True
            By default, will call verify_logged_in() when initializing a
            FASTDBClient object in order to log you into the server.  If
            for some reason you want to do this manually, set
            login=False.

          verify: bool, default True (or what's in ~/.fastdb.ini)
            Verify SSL certs?  Passed on to requests functions via verify=

          retries: int, default 5 (or what's in ~/.fastdb.ini)
            When sending a request, will look at the response.  If it's
            not 200, will sleep and retry this many times before finally
            failing.  Useful for dealing with servers that have flaky
            connections for whatever reason.

          retrysleep: float, default 1 (or what's in ~/.fastdb.ini)
            How long to sleep (in seconds) after the first connection
            failure.

          retrysleepinc: float, default 2 (or what's in ~/.fastdb.ini)
            After each retry failure, increment the sleep time by this
            many seconds.  So, with repeated failures to connect, by
            default it will sleep 1, then 3, 5, 7, and 9 seconds after
            each try before finally failing.

          logger: logging.Logger or None
            A logger log object.  If not passed, the FASTDBClient will
            make its own.  Either way, it is accessible via the logger
            property.

          info: bool, default True
            Ignored if logger is not None or if debug is True.  Set the
            logger's log level to INFO.  If both this and debug are
            False, will set the log level to WARNING.

          debug: bool, default False
            Ignored if logger is not None.  Set the logger's log level
            to DEBUG.

        """

        self.logger = logger
        if self.logger is None:
            # ARGH.  Logger can be frustrating.  I don't want to keep adding the handlers
            #   over and over again, but sometimes self.logger.hasHandlers() was coming up True
            #   even though there were no handlers.  (It may have to do with propagation,
            #   and a logger that exists in pytest?)
            # So that we don't get multiple-logging, we don't want to add a handler every
            #   time.
            # The solution: just make a new logger for every fastdb_client instance.
            self.logger = logging.getLogger( str(uuid.uuid4()) )
            logout = logging.StreamHandler( sys.stderr )
            self.logger.addHandler( logout )
            formatter = logging.Formatter( '[%(asctime)s - FASTDB - %(levelname)s] - %(message)s',
                                           datefmt='%Y-%m-%d %H:%M:%S' )
            logout.setFormatter( formatter )
            self.logger.setLevel( logging.DEBUG if debug else logging.INFO if info else logging.WARNING )
        elif not isinstance( self.logger, logging.Logger ):
            raise TypeError( f"logger must be a logging.Logger object, not a {type(logger)}" )

        self.verify = True
        self.retries = 5
        self.retrysleep = 1.
        self.retrysleepinc = 2.

        if ( server[:5] == "http:" ) or ( server[:6] == "https:" ):
            self.url = server
            if ( username is None ) or ( password is None ):
                raise ValueError( "When server is a url, must pass username and password" )
            self.username = username
            self.password = password
        else:
            fastdbini = pathlib.Path( os.getenv("HOME") ) / ".fastdb.ini"
            if verify_ini_permissions:
                stat = fastdbini.stat()
                if stat.st_mode & 0o077 != 0:
                    raise RuntimeError( "Permissions on ~/.fastdb.ini incorrect; must not be accesible "
                                        "by group or world." )
            config = configparser.ConfigParser()
            config.read( fastdbini )
            if server not in config:
                raise ValueError( f"Config for {server} not found in ~/.fastdb.ini" )
            if ( ( 'url' not in config[server] )
                 or ( 'username' not in config[server] )
                 or ( 'password' not in config[server] )
                ):
                raise ValueError( f"Config for {server} in ~/.fastdb.ini must have all of url, username, password." )
            self.url = config[server]['url']
            self.username = username if username is not None else config[server]['username']
            self.password = password if password is not None else config[server]['password']
            if 'verify' in config[server]:
                self.verify = bool( config[server]['verify'] )
            if 'retries' in config[server]:
                self.retries = int( config[server]['retries'] )
            if 'retrysleep' in config[server]:
                self.retrysleep = float( config[server]['retrysleep'] )
            if 'retrysleepinc' in config[server]:
                self.retrysleepinc = float( config[server]['retrysleepinc'] )

        if verify is not None:
            self.verify = bool( verify )
        if retries is not None:
            self.retries = int( retries )
        if retrysleep is not None:
            self.retrysleep = float( retrysleep )
        if retrysleepinc is not None:
            self.retrysleepinc = float( retrysleepinc )

        self.req = None
        if login:
            self.verify_logged_in()


    def retry_send( self, url, data=None, json=None, method="post" ):
        """Send a python requests POST or GET to a web server with retries.

        You usually want to use .post(), or one of the more
        specific methods rather than calling this directly.

        Parameters
        ----------
          url : str
            The url to connect to

          data : (something), default None
            Passed to python requests via data=

          json : dict or list, default None
            Passed to python requests via json=

          method : "get" or "post", default "post"
            Connection m ethod

        Returns
        -------
          Python requests Response object.  Will raise an exception if
          the connection failes after the retries configured in
          fastdb_client instantiation.

        """

        sleeptime = self.retrysleep
        previous_fail = False
        t0 = time.perf_counter()
        for tries in range( self.retries + 1 ):
            res = None
            try:
                if method == 'post':
                    res = self.req.post( url, data=data, json=json, verify=self.verify )
                elif method == 'get':
                    res = self.req.get( url, data=data, json=json, verify=self.verify )
                else:
                    raise ValueError( f"Unknown method {method}, must be get or post" )
                if res.status_code != 200:
                    errmsg = f"Got status {res.status_code} trying to connect to {url}"
                    if tries == self.retries:
                        self.logger.error( errmsg )
                    else:
                        self.logger.debug( errmsg )
                    raise RuntimeError( errmsg )
                if previous_fail:
                    dt = time.perf_counter() - t0
                    self.logger.info( f"Connection to {url} succeeded after {tries} retries "
                                      f"over {dt:.2f} seconds." )
                return res
            except Exception:
                previous_fail = True
                dt = time.perf_counter() - t0
                if tries < self.retries:
                    status_code = "<unknown>" if res is None else res.status_code
                    self.logger.warning( f"Failed to connect to {url} after {tries+1} tries "
                                         f"Over {dt:.2f} seconds, got status {status_code}; "
                                         f"sleeping {sleeptime} seconds and retrying" )
                    time.sleep( sleeptime )
                    sleeptime += self.retrysleepinc
                else:
                    self.logger.error( f"Failed to connect to {url} after {self.retries} tries "
                                       f"over {dt:.2f} seconds.  Giving up." )
                    if ( res is not None ) and ( res.status_code == 500 ):
                        self.logger.debug( f"Body of 500 return: {res.text}" )
                    raise


    def verify_logged_in( self ):
        """Log into the server if necessary.

        Raises an exception if logging in fails for whatever reason.

        """

        must_log_in = False
        if self.req is None:
            must_log_in = True
        else:
            res = self.retry_send( f'{self.url}/auth/isauth' )
            data = res.json()
            if not data['status']:
                must_log_in = True
            else:
                if data['username'] != self.username:
                    res = self.retry_send( f"{self.url}/auth/logout" )
                    data = res.json()
                    if ( 'status' not in data ) or ( data['status'] != 'Logged out' ):
                        raise RuntimeError( f"Unexpected response logging out: {res.text}" )
                    must_log_in = True

        if must_log_in:
            self.req = requests.session()
            res = self.retry_send( f'{self.url}/auth/getchallenge', json={ 'username': self.username } )
            try:
                data = res.json()
                challenge = binascii.a2b_base64( data['challenge'] )
                enc_privkey = binascii.a2b_base64( data['privkey'] )
                salt = binascii.a2b_base64( data['salt'] )
                iv = binascii.a2b_base64( data['iv'] )
                aeskey = PBKDF2( self.password.encode('utf-8'), salt, 32, count=100000,
                                 hmac_hash_module=SHA256 )
                aescipher = AES.new( aeskey, AES.MODE_GCM, nonce=iv )
                # When javascript created the encrypted AES key, it appended
                #   a 16-byte auth tag to the end of the ciphertext. (Python's
                #   Crypto AES-GCM handling treates this as a separate thing.)
                privkeybytes = aescipher.decrypt_and_verify( enc_privkey[:-16], enc_privkey[-16:] )
                privkey = RSA.import_key( privkeybytes )
                rsacipher = PKCS1_OAEP.new( privkey, hashAlgo=SHA256 )
                decrypted_challenge = rsacipher.decrypt( challenge ).decode( 'utf-8' )
            except Exception:
                raise RuntimeError( "Failed to log in, probably incorrect password" )

            res = self.retry_send( f'{self.url}/auth/respondchallenge',
                                   json= { 'username': self.username, 'response': decrypted_challenge } )
            data = res.json()
            if ( ( data['status'] != 'ok' ) or ( data['username'] != self.username ) ):
                raise RuntimeError( f"Unexpected response logging in: {res.text}" )


    # ======================================================================
    # The generic method for communcating with a fastdb API endpoint

    def post( self, relative_url, json=None, data=None, verifyloggedin=True, return_format='json' ):
        """Send a POST query to the server.

        Parameters
        ----------
          relative_url: str
            URL relative to the base webap URL.

          json: dict, default None
            Parameters to pass to the API endpoint.  This will usually
            be in the form of a dictionary.  Which keywords are
            expected, and what the values can be, will differ from API
            endpoint to endpoint.  (Some endpoints won't expect
            anything, in which case you can just not specify this
            argument.)  See the FASTDB API documentation for what you should
            send.  (For the technical-minded, this is passed on to a
            python requests session object's post method via the
            parameter 'json').

          data: (something), default None
            Raw data to upload to the API endpoint.  Most endpoints will
            not use this, but will use the json parameter instead.  If
            there is ever an endpoint that does something like file
            upload, that's where you'd use json.  (For the
            technical-minded, this is passed on to a python requests
            session object's post method via the parmeter 'data'.)

          verifyloggedin : bool, default True
            If True, verify that we're logged into the server, and if
            not, log in using the url, username, and password passed to
            the object constructor.  This causes at least one extra
            initial connection to the server, which has some overhead.
            If you don't want this, set verifyloggedin to False to have
            it assume you're logged in, and just fail if you're not.
            (If you're doing a lot of rapid posts to the server, you may
            well want to set this to False for performance reasons.  If
            you're doing only a post every few seconds or less often,
            the overhead will probably not be too significant.)

          return_format : str, default 'json'
            What do you want returned from the call to this function?
            Can be one of 'json', 'csv', or 'raw'.  'raw' should always
            work; other options will only work if the api endpoint
            you're hitting is compatible.


        Returns
        -------
          Something

          What you get back depends on what you passed to "return_format":

              raw : a python requests.Response object.  Do whatever you want with it.

              json : a dictionary or a list.  If you ask for return_format
                     'json', you are telling the function that it should
                     expect the server to return json which can be
                     parsed to a python datastructure.

              csv : You will get the text of a csv file, assuming that
                    the api endpoint returns csv.  Note that you do not
                    get the filename; you get the actual text.  Write it
                    to a file if you want a file.  [NOT CURRENTLY IMPLEMENTED]

        """

        if verifyloggedin:
            self.verify_logged_in()

        slash = '/' if ( ( self.url[-1] != '/' ) and ( relative_url[0] != '/' ) ) else ''
        res = self.retry_send( f'{self.url}{slash}{relative_url}', json=json )

        if return_format == 'raw':
            return res

        if return_format == 'json':
            if res.headers.get('Content-Type')[:16] != 'application/json':
                raise RuntimeError( f"Expected json back from fastdb server, but got "
                                    f"{res.headers.get('Content_Type')}" )
            return res.json()

        if return_format == 'csv':
            raise NotImplementedError( "CSV return format not yet implemented." )


    # ======================================================================
    # Methods for communicating with the db/ api for direct sql queries

    def _parse_query( self, query, subdict, return_format=0 ):
        if subdict is not None:
            if isinstance( query, list ):
                if not isinstance( subdict, list ):
                    raise TypeError( "If query is a list, subdict must be a list" )
                if len( subdict ) != len( query ):
                    raise ValueError( "If query and subdict are lists, they must have the same length" )
            elif isinstance( query, str ):
                if not isinstance( subdict, dict ):
                    raise TypeError( "If query is a string, subdict must be a dict" )
            else:
                raise TypeError( f"Query must be a str or list, not a {type(query)}" )
            retval = { 'query': query, 'subdict': subdict, 'return_format': return_format }

        else:
            if not ( isinstance( query, list ) or isinstance( query, str ) ):
                raise TypeError( f"Query must be a str or list, not a {type(query)}" )
            retval = { 'query': query, 'return_format': return_format }

        return retval


    def submit_short_sql_query( self, query, subdict=None, return_format=0 ):
        """Get the results of a SQL query to FASDB that will take less than 5 minutes.

        Parameters
        ----------
          query : str or list of str
            The PostgreSQL query to send.  Will do standard psycopg
            cursor.execute() parameter substitution of strings like
            "%(var)s" using the "var" entry in subdict.  (Of course,
            "var" can be anything.)

            If a list, then is a sequence of queries to be run in order
            as part of the same transaction.  This functionality exists
            so that you could e.g. create and use temp tables.

          subdict : dict or list of dict
            The substitution dictionary to use with query. For each
            instance of "%(var)s" in query, there must be an entry in
            this dictionary with key var and what it should be
            substituted for.  (Of course, you can use any alphanumeric
            key in place of "var".)

            If query is a list, then this must be a list of dictionaries
            with the same length as query.


          return_format : int, default 0
            See "Returns" below.

        Returns
        -------
          Depending on the value return_format:

          0 (default) : a list of dictionaries.  Each element in the
            list is a dictionary of {column: value} resulting from the
            query, or from the last query in the list if a list of
            queries was sent.

          1 : a dictionary of {column: list}.  Each value will have the
            same length.  The elements of each list represent the values
            for that column in all of the rows returned from the the
            query, or fromt he last query in the list if a list of
            queries was sent.

        """

        json = self._parse_query( query, subdict, return_format )
        data = self.post( self.short_query_url, json=json )

        if 'status' not in data.keys():
            raise ValueError( "Unexpected response, no 'status' in return value" )
        elif data['status'] == 'error':
            raise RuntimeError( f"Got an error from the server: {data['error']}" )
        elif data['status'] != 'ok':
            raise RuntimeError( f"status is {data['status']} and I don't know how to cope" )
        else:
            return data['rows'] if return_format == 0  else data['data']


    def submit_long_sql_query( self, query, subdict=None, return_format='csv' ):
        """Submit a long SQL query to FASTDB

        FASTDB will queue the query and run it sometime.  Run
        self.check_long_sql_query(querid) to see if it's done, and, if
        it is, self.get_long_sql_query(queryid) to get the results.

        Parameters
        ----------
          query, subdict : Same as what's passed to submit_short_sql_query

          return_format : str, default 'csv'
            The format of the returned data.  Right now, must be 'csv'.

        Returns
        -------
          queryid: str

          An opaque string (really, a string-encoded UUID, but you don't
          need to know that) that you use to get the results of the
          query later.

        """

        json = self._parse_query( query, subdict, return_format )
        data = self.post( self.submit_long_query_url, json=json )

        if 'status' not in data.keys():
            raise ValueError( "Unexpected response, no 'status' in return value" )
        elif data['status'] == 'error':
            raise RuntimeError( f"Got an error from the server: {data['error']}" )
        elif data['status'] != 'ok':
            raise RuntimeError( f"status is {data['status']} and I don't know how to cope" )
        else:
            self.logger.info( f"Submitted query {data['queryid']}" )
            return data['queryid']


    def check_long_sql_query( self, queryid ):
        """Check on the status of a long SQL query to FASTDB

        Parameters
        ----------
          queryid : str
            The string returned by submit_long_sql_query

        Returns
        -------
          dict
            Will always have a key "status", which is one of "queued",
            "started", "finished", or "error".

            May have additional keys based on status:
              If status is 'started':
                started: the time the query started

              If status is 'finished':
                started: the time the query started
                finished: the time the query finished

              If status is 'error':
                finished: the time the queyr errored out
                error: an error message
                ...there may also, but won't always, be a key 'started' with the time the query started

        """

        result = self.post( f"{self.check_long_sql_query_url}{queryid}/" )
        if 'status' not in result.keys():
            raise ValueError( "Unexpected response, no 'status' in return value" )
        return result


    def get_long_sql_query_result( self, queryid ):
        """Get the results of a finished long sql query.

        Only call this after you've called check_long_sql_query on this
        queryid and have received a status of 'sinished'

        Parameters
        ----------
          queryid : str
            The string returned by submit_long_sql_query

        Returns
        -------
          str or binary blob
            The results of the query.  The nature of these results
            depend on the on the return_format parameter you passed to
            submit_long_sql_query.

            If you passed a sequence of queries to submit_long_sql_query,
            this will be the result of the last query in the list.

        """

        result = self.post( f"{self.get_long_sql_query_results_url}{queryid}/", return_format='raw' )
        ctype = result.headers[ 'content-type' ]
        if ctype == 'text/csv; charset=utf-8':
            return result.text
        elif ctype == 'application/octet-stream':
            return result.content
        else:
            raise TypeError( f"Got unknown type {ctype}, expected 'text/csv; charset=utf-8' "
                             f"or 'application/octet-stream'" )


    def synchronous_long_sql_query( self, query, subdict=None, return_format='csv',
                                    checkeach=300, maxwait=3600 ):
        """Get the result of an SQL query to FASDB.

        If the query will take less than 5 minutes, use submit_short_sql_query() instead.

        This combines together submit_long_sql_query(),
        check_long_sql_query(), and get_long_sql_query_result() into a
        single call.

        Parameters
        ----------
          query, subdict, return_format : same as what's passed to submit_long_sql_query()

          checkeach: int, default 300
            After submitting the query, wait this many seconds before
            checking to see if it's done.  If it's not, wait again,
            check again, etc.

          maxwait: int, default 3600
            Wait at most this many seconds for the query to finish
            before giving up.  Note that the server may well still
            complete the query after this wait time, but you won't have
            a way of getting the result.

        Returns
        -------
          Same as what get_long_sql_query_result() returns

        """

        queryid = self.submit_long_sql_query( query, subdict, return_format )

        t0 = time.perf_counter()
        done = False
        totwait = 0
        while ( not done ) and ( totwait < maxwait ):
            time.sleep( checkeach )
            data = self.check_long_sql_query( queryid )

            if data['status'] == 'error':
                strio = io.StringIO()
                strio.write( "Long query failed" )
                if 'finished' in data:
                    strio.write( f" at {data['finished']}" )
                strio.write( f" with error: {data['error']}" )
                raise RuntimeError( strio.getvalue() )

            elif data['status'] == 'finished':
                self.logger.info( f"Long query started at {data['started']} "
                                  f"and finished at {data['finished']}" )
                done = True

            elif data['status'] == 'started':
                self.logger.info( f"Long query started at {data['started']} and is still in progress." )

            elif data['status'] == 'queued':
                self.logger.info( "Long query is queued, not yet started." )

            else:
                raise ValueError( f'Unexpected value of data["status"]: "{data["status"]}"' )

            totwait = time.perf_counter() - t0

        if not done:
            raise RuntimeError( f"Query failed to complete within {totwait} seconds." )

        self.logger.info( f"Got long query result after {totwait} seconds." )
        return self.get_long_sql_query_result( queryid )
