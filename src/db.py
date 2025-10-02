# IMPORTANT : make sure that everything in here stays synced with the
#   database schema managed by migrations in ../db
#
# WARNING : code here will drop the table "temp_bulk_upsert" if you make it, so don't make that table.
#
# WARNING : code assumes all column names are lowercase.  Don't mix case in column names.

# import sys
import os
import uuid
import collections
import types
import logging

from contextlib import contextmanager

import numpy as np
import psycopg
import psycopg.rows
import psycopg.sql
import psycopg.types.json
import pymongo

import util
from util import FDBLogger

# These next three are for debugging.  They are used in DBCon.__init__.
# For normal use, they should be False, as they bloat the logs a lot.
# They will be ignored if the logging level of FGDBLogger is not DEBUG.
#
# ALSO : use of alwaysexplain and alwaysanalyze makes us susceptible to
# SQL injection attacks, so we REALLY don't want these set in production
# databases!!!!!!!  They are for debugging purposes only.  Do not try
# this at home.  Do not eat, multilate, or spindle.  Consult your doctor
# if you do not feel extreme anxiety when using these.
#
# explaining can slow down queries as sometimes it seems that
# postgres really wants to think about what it's doing before giving you
# a query plan (I don't know why; is it a pg_hint_plan thing?)
#
# We should replace them with configurable options.
_echoqueries = True
_alwaysexplain = True
_alwaysanalyze = True

# The tables here should be in the order they safe to drop.
# (Insofar as it's safe to drop all your tables....)
all_table_names = [ 'query_queue',
                    'spectruminfo', 'plannedspectra', 'wantedspectra',
                    'ppdb_alerts_sent', 'ppdb_diaforcedsource', 'ppdb_diasource', 'ppdb_diaobject', 'ppdb_host_galaxy',
                    'diaforcedsource', 'diasource', 'diaobject', 'root_diaobject','host_galaxy',
                    'diasource_import_time',
                    'processing_version_alias', 'base_procver_of_procver',
                    'processing_version', 'base_processing_version',
                    'passwordlink', 'authuser',
                    'migrations_applied' ]

# ======================================================================
# Global config

import config
with open( config.dbpasswdfile ) as ifp:
    dbpasswd = ifp.readline().strip()
dbhost = config.dbhost
dbport = config.dbport
dbuser = config.dbuser
dbname = config.dbdatabase

# For multiprcoessing debugging
# import pdb
# class ForkablePdb(pdb.Pdb):
#     _original_stdin_fd = sys.stdin.fileno()
#     _original_stdin = None

#     def __init__(self):
#         pdb.Pdb.__init__(self, nosigint=True)

#     def _cmdloop(self):
#         current_stdin = sys.stdin
#         try:
#             if not self._original_stdin:
#                 self._original_stdin = os.fdopen(self._original_stdin_fd)
#             sys.stdin = self._original_stdin
#             self.cmdloop()
#         finally:
#             sys.stdin = current_stdin


# ======================================================================

def get_dbcon():
    """Get a database connection.

    It's your responsibility to roll it back, close it, etc!

    Consider using the DB or DBCon context managers instead of this.
    """

    global dbuser, dbpasswd, dbhost, dbport, dbname
    conn = psycopg.connect( dbname=dbname, user=dbuser, password=dbpasswd, host=dbhost, port=dbport )
    return conn


@contextmanager
def DB( dbcon=None ):
    """Get a psycopg.connection in a context manager.

    Always call this as "with DB() as ..."

    Parameters
    ----------
       dbcon: psycopg.connection or None
          If not None, just returns that.  (Doesn't check the type, so
          don't pass the wrong thing.)  Otherwise, makes a new
          connection, and then rolls back and closes that connection
          after it goes out of scope.

    Returns
    -------
       psycopg.connection

    """

    if dbcon is not None:
        yield dbcon
        return

    conn = None
    try:
        conn = get_dbcon()
        yield conn
    finally:
        if conn is not None:
            conn.rollback()
            conn.close()


class DBCon:
    """Class that encapsulates a postgres database connection.

    Prefer using this class in a context manager:

        with DBCon() as dbcon:
            rows, cols = dbcon.execute( query, subdict )
             # do other things

    That way, it will automatically close the database connection
    when the context manager exists.

    Send queries using DBCon.execute_nofetch() and DBCon.execute().

    If for some reason you need access to the underyling cursor, you can
    get it from the cursor property.

    """

    def __init__( self, con=None, dictcursor=False ):
        """Instantiate.

        If you use this, you should also use close(), and soon.

        Parameters
        ----------
          con : psycopg.Connection or DBCon
            If None (the default), will make a new connection, and will
            roll back and close it when done.  If not None, then will
            instead wrap this connection; when close() is called, or
            when the context manager that created this object ends, will
            roll back and close the connection.  However, if con is not
            None, then the assumption is that somebody else is managing
            the connection, so will not rollback or close.

          dictcursor : bool, default False
            If True, then the cursor uses psycopg.rows.dict_row as its
            row factory.  execite() will return a list of dictionaries,
            with each element of the list being one row of the result.
            If False, then execute returns two lists: a list of tuples
            (the rows) and a list of strings (the column names).

        """

        global dbuser, dbpasswd, dbhost, dbport, dbname
        # TODO : make these next two configurable rather than hardcoded
        # These are useful for debugging, but are profligate for production
        global _echoqueries, _alwaysexplain, _alwaysanalyze

        if con is not None:
            if isinstance( con, DBCon ):
                self.con = con.con
            elif isinstance( con, psycopg.Connection ):
                self.con = con
            else:
                raise TypeError( f"con must be None, a DBCon, or a psycopg.Connection, not a {type(con)}" )
            self._con_is_mine = False
        else:
            self.con = psycopg.connect( dbname=dbname, user=dbuser, password=dbpasswd, host=dbhost, port=dbport )
            self._con_is_mine = True

        self.dictcursor = dictcursor
        self.echoqueries = _echoqueries
        self.alwaysexplain = _alwaysexplain
        self.alwaysanalyze = _alwaysanalyze
        self.remake_cursor()


    def __enter__( self ):
        return self


    def __exit__( self, type, value, traceback ):
        self.close()


    def remake_cursor( self, dictcursor=None ):
        """Recreate the cursor used for database communication.

        Parameters
        ----------
          dictcursor : bool, default None
            If None, will make a cursor that returns dictionaries
            (vs. tuples) for rows based on what was passed to the
            dictcursor argument of the DBCon constructor.  If True,
            makes a cursor that will cause execute() to return a list of
            dictionaries.  If False, makes a cursor that will cause
            execute() to return two lists; the first is a list of tuples
            (the rows), the second is a list of strings (the column
            names).

        """
        self.curcursorisdict = self.dictcursor if dictcursor is None else dictcursor
        if self.curcursorisdict:
            self.cursor = self.con.cursor( row_factory=psycopg.rows.dict_row )
        else:
            self.cursor = self.con.cursor()


    def close( self ):
        """Rolls back and closes the connection if appropriate.

        If you did stuff you want kept, make sure to call commit.

        If the constructor was called with con=None, then the connection
        will be rolled back.  If the constructor was callled with a
        non-None none, then this method does nothing.

        """
        if self._con_is_mine:
            self.con.rollback()
            self.con.close()


    def commit( self ):
        """Commit changes to the database.

        Call this if you've done any INSERT or UPDATE or similar
        commands that change the database, and you want your commands to
        stick.

        """
        self.con.commit()
        self.remake_cursor( self.curcursorisdict )  # ...is this necessary?


    def execute_nofetch( self, q, subdict={}, echo=None, explain=None, analyze=None ):
        """Runs a query where you don't expect to fetch results.

        Parameters are the same as execute().  Returns nothing.

        """

        alreadydid = False
        if not isinstance( q, ( psycopg.sql.SQL, psycopg.sql.Composed ) ):
            q = psycopg.sql.SQL( q )

        if FDBLogger.instance().get().level <= logging.DEBUG:
            echo = echo if echo is not None else self.echoqueries
            explain = explain if explain is not None else self.alwaysexplain
            analyze = analyze if analyze is not None else self.alwaysanalyze
            if echo:
                FDBLogger.debug( f"Sending query\n{q.as_string()}\nwith substitutions: {subdict}" )

            nl = '\n'
            if explain:
                FDBLogger.debug( "Explaining..." )
                self.cursor.execute( psycopg.sql.SQL("EXPLAIN ") + q, subdict )
                rows = self.cursor.fetchall()
                dex = 'QUERY PLAN' if self.curcursorisdict else 0
                FDBLogger.debug( f"Query plan:\n{nl.join([r[dex] for r in rows])}" )
            if analyze:
                FDBLogger.debug( "Doing EXPLAIN ANALYZE..." )
                self.cursor.execute( psycopg.sql.SQL("EXPLAIN ANALYZE ") + q, subdict )
                alreadydid = True
                rows = self.cursor.fetchall()
                dex = 'QUERY PLAN' if self.curcursorisdict else 0
                FDBLogger.debug( f"Query plan:\n{nl.join([r[dex] for r in rows])}" )

        # NOTE: if we ran with EXPLAIN ANALYZE, any side effects of the query happened!
        # So don't run the query again.  This is why execute() forces analyze to
        # be false, because you can't EXPLAIN ANALYZE the query and get the results
        # all in one call.
        if not alreadydid:
            self.cursor.execute( q, subdict )

        if ( FDBLogger.instance().get().level <= logging.DEBUG ) and ( echo or explain ):
            FDBLogger.debug( "Query complete." )

    def execute( self, q, subdict={}, silent=False, echo=None, explain=None ):
        """Runs a query, and returns either (rows, columns) or just rows.

        Parmaeters
        ----------
          q : str or psycopg.sql.Composed
            The query.  Use %(var)s in the string for a substitution, if
            necessary.  The key "var" must then show up in subdict.

          subdict : dict
            Substitution dictionary. For every %(var)s that shows up in
            q, there must be a key "var" in this dictionary with the
            value to be substituted.  Extra keys are ignored.  Do not
            pass this if q is a Composed; in that case, you've already
            built in the substitutions.

          echo : bool, default None
            If True, echo queries before sending them.  If False, don't.
            If None, use the default (self.echoqueries, initialized from
            the _echoqueries variable at the top of this module).

          explain : bool, default None
            If True, before running the query run an EXPLAIN on it and
            send the output to debug logging.  If False, don't.  If
            None, use the default (self.alwaysexplain, initialized from
            the _alwaysexplain variable at the top of this module).

            WARNING: use of this makes you susceptible to SQL injection
            attacks if you aren't completely and totally confident about
            where your SQL came from.  Do not get bobby tablesed!

        Returns
        -------
          If the current cursor is a dict cursor, returns a list of dictionaries.

          If the current cursor is not a dict cursor, returns two lists.
          The first is a list of lists, with the rows pulled from the
          dictionary.  The second is a list of column names.

        """
        self.execute_nofetch( q, subdict, echo=echo, explain=explain, analyze=False )
        if self.curcursorisdict:
            return self.cursor.fetchall()
        else:
            if self.cursor.description is None:
                return None, None
            rows = self.cursor.fetchall()
            cols = [ desc[0] for desc in self.cursor.description ]
            return rows, cols


# ======================================================================

@contextmanager
def MG( client=None ):
    """Get a mongo client in a context manager.

    It has read/write access to the broker message database (which is
    configured in env var MONGODB_DBNAME).

    Always call this as "with MongoClient() as ..."

    Right now, this does not support Mongo transactions.  Hopefully we
    won't need that in our case.

    """

    if client is not None:
        yield client
        return

    try:
        host = os.getenv( "MONGODB_HOST" )
        dbname = os.getenv( "MONGODB_DBNAME" )
        user = os.getenv( "MONGODB_ALERT_WRITER_USER" )
        password = os.getenv( "MONGODB_ALERT_WRITER_PASSWD" )
        if any( i is None for i in [ host, dbname, user, password ] ):
            raise RuntimeError( "Failed to make mongo client; make sure all env vars are set: "
                                "MONGODB_HOST, MONGODB_DBNAME, MONGODB_ALERT_WRITER_USER, "
                                "MONGODB_ALERT_WRITER_PASSWD" )
        client = pymongo.MongoClient( f"mongodb://{user}:{password}@{host}:27017/"
                                      f"{dbname}?authSource={dbname}" )
        yield client
    finally:
        if client is not None:
            client.close()


def get_mongo_collection( mongoclient, collection_name ):
    """Get a pymongo.collection from the mongo db."""

    mongodb = getattr( mongoclient, os.getenv( "MONGODB_DBNAME" ) )
    collection = getattr( mongodb, collection_name )
    return collection


# ======================================================================
class ColumnMeta:
    """Information about a table column.

    An object has properties:
      column_name
      data_type
      column_default
      is_nullable
      element_type
      pytype

    (They can also be read as if the object were a dictionary.)

    It has methods

      py_to_pg( pyobj )
      pg_to_py( pgobj )

    """

    # A dictionary of postgres type to type of the object in Python
    typedict = {
        'uuid': uuid.UUID,
        'smallint': np.int16,
        'integer': np.int32,
        'bigint': np.int64,
        'text': str,
        'jsonb': dict,
        'boolean': bool,
        'real': np.float32,
        'double precision': np.float64
    }

    # A dictionary of "<type">: <2-element tuple>
    # The first elment is the data type as it shows up postgres-side.
    # The second element is a two element tuple of functions:
    #   first element : convert python object to what you need to send to postgres
    #   second element : convert what you got from postgres to python type
    # If a function is "None", it means the identity function.  (So 0=1, P=NP, and Δs<0.)

    typeconverters = {
        'uuid': ( util.asUUID, None ),
        'jsonb': ( psycopg.types.json.Jsonb, None )
    }

    def __init__( self, column_name=None, data_type=None, column_default=None,
                  is_nullable=None, element_type=None ):
        self.column_name = column_name
        self.data_type = data_type
        self.column_default = column_default
        self.is_nullable = is_nullable
        self.element_type = element_type


    def __getitem__( self, key ):
        return getattr( self, key )

    @property
    def pytype( self ):
        return self.typedict[ self.data_type ]


    def py_to_pg( self, pyobj ):
        """Convert a python object to the corresponding postgres object for this column.

        The "postgres object" is what would be fed to psycopg's
        cursor.execute() in a substitution dictionary.

        Most of the time, this is the identity function.

        """
        if ( ( self.data_type == "ARRAY" )
             and ( self.element_type in self.typeconverters )
             and ( self.typeconverters[self.element_type][0] is not None )
            ):
            return [ self.typeconverters[self.element_type][0](i) for i in pyobj ]

        elif ( ( self.data_type in self.typeconverters )
               and ( self.typeconverters[self.data_type][0] is not None )
              ):
            return self.typeconverters[self.data_type][0]( pyobj )

        return pyobj


    def pg_to_py( self, pgobj ):
        """Convert a postgres object to python object for this column.

        This "postgres object" is what you got back from a cursor.fetch* call.

        Most of the time, this is the identity function.

        """

        if ( ( self.data_type == "ARRAY" )
             and ( self.element_type in self.typeconverters )
             and ( self.typeconverters[self.element_type][1] is not None )
            ):
            return [ self.typeconverters[self.element_type][1](i) for i in pgobj ]
        elif ( ( self.data_type in self.typeconverters )
               and ( self.typeconverters[self.data_type][1] is not None )
              ):
            return self.typeconverters[self.data_type][1]( pgobj )

        return pgobj


    def __repr__( self ):
        if self.data_type == 'ARRAY':
            return f"ColumnMeta({self.column_name} [ARRAY({self.element_type})]"
        else:
            return f"ColumnMeta({self.column_name} [{self.data_type}])"


# ======================================================================
# ogod, it's like I'm writing my own ORM, and I hate ORMs
#
# But, two things.  (1) I'm writing it, so I know actually what it's doing
#   backend with the PostgreSQL queries, (2) I'm not trying to create a whole
#   new language to learn in place of SQL, I still intend mostly to just use
#   SQL, and (3) sometimes it's worth re-inventing the wheel so that you get
#   just a wheel (and also so that you really get a wheel and not massive tank
#   treads that you are supposed to think act like a wheel)

class DBBase:
    """A base class from which all other table classes derive themselves.

    All subclasses must include:

    __tablename__ = "<name of table in databse>"
    _tablemeta = None
    _pk = <list>

    _pk must be a list of strings with the names of the primary key
    columns.  Uusally (but not always) this will be a single-element
    list.

    """

    # A dictionary of "<colum name>": <2-element tuple>
    # The first element is the converter that converts a value into something you can throw to postgres.
    # The second element is the converter that takes what you got from postgres and turns it into what
    #   you want the object to have.
    # Often this can be left as is, but subclasses might want to override it.
    colconverters = {}

    @property
    def tablemeta( self ):
        """A dictionary of colum_name : ColumMeta."""
        if self._tablemeta is None:
            self.load_table_meta()
        return self._tablemeta

    @property
    def pks( self ):
        return [ getattr( self, k ) for k in self._pk ]


    @classmethod
    def all_columns_sql( cls, prefix=None ):
        """Returns a psycopg.sql.SQL thingy with all columns comma separated."""
        if cls._tablemeta is None:
            cls.load_table_meta()
        if prefix is None:
            return psycopg.sql.SQL(',').join( psycopg.sql.Identifier(i) for i in cls._tablemeta.keys() )
        else:
            return psycopg.sql.SQL(',').join( psycopg.sql.Identifier(prefix, i) for i in cls._tablemeta.keys() )

    @classmethod
    def load_table_meta( cls, dbcon=None ):
        if cls._tablemeta is not None:
            return

        with DBCon( dbcon, dictcursor=True ) as dbcon:
            cols = dbcon.execute( "SELECT c.column_name,c.data_type,c.column_default,c.is_nullable,"
                                  "       e.data_type AS element_type "
                                  "FROM information_schema.columns c "
                                  "LEFT JOIN information_schema.element_types e "
                                  "  ON ( (c.table_catalog, c.table_schema, c.table_name, "
                                  "        'TABLE', c.dtd_identifier) "
                                  "      =(e.object_catalog, e.object_schema, e.object_name, "
                                  "        e.object_type, e.collection_type_identifier) ) "
                                  "WHERE table_name=%(table)s",
                                  { 'table': cls.__tablename__ } )
            cls._tablemeta = { c['column_name']: ColumnMeta(**c) for c in cols }

            # See Issue #4!!!!
            for col, meta in cls._tablemeta.items():
                if col in cls.colconverters:
                    if cls.colconverters[col][0] is not None:
                        # Play crazy games because of the confusingness of python late binding
                        def _tmp_py_to_pg( self, pyobj, col=col ):
                            return cls.colconverters[col][0]( pyobj )
                        meta.py_to_pg = types.MethodType( _tmp_py_to_pg, meta )
                    if cls.colconverters[col][1] is not None:
                        def _tmp_pg_to_py( self, pgobj, col=col ):
                            return cls.colconverters[col][1]( pgobj )
                        meta.pg_to_py = types.MethodType( _tmp_pg_to_py, meta )


    def __init__( self, dbcon=None, cols=None, vals=None, _noinit=False, noconvert=True, **kwargs):
        """Create an object.

        If this is based on a fetch from a postgres connection, then you
        want to set noconvert=False (see below).

        Set properties of the object either by passing cols and vals
        (see below), or just by passing additional arguments to the
        constructor (in which case cols and vals should be left at their
        default of None).  Additional arguments will be set as
        properties of the object.  However, the names of the additional
        arguments must all be columns of the table.

        Properties
        ----------
          dbcon : DBCon or psycopg.Connection, default None
            Database connection to use.  If None (default), will open a
            new connection if necessary (to pull down the class' table
            metadata if that hasn't been done already) and close it.

          cols : list of str, default None
            Attributes of the object to set.  Each should be a column
            name in the database table.  Instead of using this and vals,
            you can just pass additional arguments to the construtor.
            (See above.)

          vals : list, default None
            Values to set; length should be the same as cols, or should
            be None if cols is None.

          _noinit : bool, default False
            Don't use this, it's used internally.

          noconvert : bool, default True
            Normally, the assumption is that the values of the
            attributes to set are regular python objects.  If instead
            vals are things that have just been read from a postgres
            database, pass False for noconvert, and then they will be
            run through a type converter to convert postgres types to
            python types as necessary.

        """

        if _noinit:
            return

        self.load_table_meta( dbcon=dbcon )
        mycols = set( self._tablemeta.keys() )

        if not ( ( cols is None ) and ( vals is None ) ):
            if ( cols is None ) or ( vals is None ):
                raise ValueError( "Both or neither of cols and vals must be none." )
            if ( not util.isSequence( cols ) ) or ( not util.isSequence( vals ) ) or ( len( cols ) != len( vals ) ):
                raise ValueError( "cols and vals most both be lists of the same length" )

        if cols is not None:
            if len(kwargs) > 0:
                raise ValueError( "Can only pass column values as named arguments "
                                  "if cols and vals are both None" )
        else:
            cols = kwargs.keys()
            vals = kwargs.values()

        keys = set( cols )
        if not keys.issubset( mycols ):
            raise RuntimeError( f"Unknown columns for {self.__tablename__}: {keys-mycols}" )

        for col in mycols:
            setattr( self, col, None )

        self._set_self_from_fetch_cols_row( cols, vals, noconvert=noconvert )


    def _set_self_from_fetch_cols_row( self, cols, fetchrow, noconvert=False, dbcon=None ):
        if self._tablemeta is None:
            self.load_table_meta( dbcon=dbcon )

        if noconvert:
            for col, val in zip( cols, fetchrow ):
                setattr( self, col, val )
        else:
            for col, val in zip( cols, fetchrow ):
                setattr( self, col, self._tablemeta[col].pg_to_py( val ) )


    def _build_subdict( self, columns=None ):
        """Create a substitution dictionary that could go into a cursor.execute() statement.

        The columns that are included in the dictionary interacts with default
        columns in a potentially confusing way.

        IF self does NOT have an attribute corresponding to a column, then
        that column will not be in the returned dictionary.

        IF self.{column} is None, and the table has a default that is *not*
        None, that column will not be in the returned dictionary.

        In other words, if self.{column} doesn't exist, or self.{column} is
        None, it means that the actual table column will get the PostgreSQL
        default value when this subdict is used (assuming the query is constructed
        using only the keys of the subdict).

        (It's not obvious that this is the best behavior; see comment in
        method source.)

        Paramters
        ---------
          columns : list of str, optional
            If given, include these columns in the returned subdict; by
            default, include all columns from the table.  (But, not not all
            columns may actually be in the returned subdict; see above.)  If
            the list includes any columns that don't actually exist for the
            table, an exception will be raised.

        Returns
        -------
          dict of { column_name: value }

        """

        subdict = {}
        if columns is not None:
            if any( c not in self.tablemeta for c in columns ):
                raise ValueError( f"Not all of the columns in {columns} are in the table" )
        else:
            columns = self.tablemeta.keys()

        for col in columns:
            if hasattr( self, col ):
                val = getattr( self, col )
                if val is None:
                    # What to do when val is None is not necessarily obvious.  There are a couple
                    #  of possibilities:
                    # (1) We really want to set this field to NULL in the database
                    # (2) It just hasn't been set yet in the object, so we want the
                    #     database row to keep what it has, or (in the case of an insert)
                    #     get the default value.
                    # How to know which is the case?  Assume that if the column_default is None,
                    # then we're in case (1), but if it's not None, we're in case (2).
                    if self.tablemeta[col]['column_default'] is None:
                        subdict[ col ] = None
                else:
                    subdict[ col ] = self.tablemeta[ col ].py_to_pg( val )

        return subdict


    @classmethod
    def _construct_pk_query_where( cls, *args, me=None ):
        if cls._tablemeta is None:
            cls.load_table_meta()

        if me is not None:
            if len(args) > 0:
                raise ValueError( "Can't pass both me and arguments" )
            args = me.pks

        if len(args) != len( cls._pk ):
            raise ValueError( f"{cls.__tablename__} has a {len(cls._pk)}-element compound primary key, but "
                              f"you passed {len(args)} values" )
        q = "WHERE "
        _and = ""
        subdict = {}
        for k, v in zip( cls._pk, args ):
            q += f"{_and} {k}=%({k})s "
            subdict[k] = cls._tablemeta[k].py_to_pg( v )
            _and = "AND"

        return q, subdict

    @classmethod
    def get( cls, *args, dbcon=None ):
        """Get an object from a table row with the specified primary key(s).

        There should be as many positional arguments as there are primary keys for the table.

        Parameters
        ----------
          dbcon : DBCon or psycopg.Connection, default None
            If given, use this database connection.  Otherwise, it will
            open and close a new database connection.

        """

        q, subdict = cls._construct_pk_query_where( *args )
        q = f"SELECT * FROM {cls.__tablename__} {q}"
        with DBCon( dbcon ) as dbcon:
            rows, cols = dbcon.execute( q, subdict )

        if len(rows) > 1:
            raise RuntimeError( f"Found multiple rows of {cls.__tablename__} with primary keys {args}; "
                                f"this should never happen." )
        if len(rows) == 0:
            return None

        obj = cls( cols=cols, vals=rows[0] )
        return obj

    @classmethod
    def get_batch( cls, pks, dbcon=None ):
        """Get a list of objects based on primary keys.

        Arguments
        ---------
          pks : list of lists
            Each element of the list must be a list whose length matches
            the length of self._pk.

          dbcon : DBCon or psycopg.Connection, default None
            If given, use this database connection.  Otherwise, it will
            open and close a new database connection.

        Returns
        -------
          list of objects
            Each object will be an instance of the class this class
            method was called on.

        """

        if ( not isinstance( pks, collections.abc.Sequence ) ) or ( isinstance( pks, str ) ):
            raise TypeError( f"Must past a list of lists, each list having {len(cls._pk)} elwements." )

        if cls._tablemeta is None:
            cls.load_table_meta( dbcon )

        comma = ""
        mess = ""
        subdict = {}
        pktypes = [ cls._tablemeta[k]['data_type'] for k in cls._pk ]
        for dex, pk in enumerate( pks ):
            if len( pk ) != len( cls._pk ):
                raise ValueError( f"{pk} doesn't have {len(cls._pk)} elements, should match {cls._pk}" )
            mess += f"{comma}("
            subcomma=""
            for subdex, ( pkval, pkcol ) in enumerate( zip( pk, cls._pk ) ):
                mess += f"{subcomma}%(pk_{dex}_{subdex})s"
                subdict[ f'pk_{dex}_{subdex}' ] = cls._tablemeta[pkcol].py_to_pg( pkval )
                subcomma = ","
            mess += ")"
            comma = ","
        comma = ""
        _and = ""
        collist = ""
        onlist = ""
        for subdex, ( pk, pktyp ) in enumerate( zip( cls._pk, pktypes ) ):
            collist += f"{comma}{pk}"
            # # SCARY.  Specific coding for uuid.  Really I probably ought to
            # #   do something with a converter dictionary to make this more
            # #   general, but I know that the only case I'll need it (at least
            # #   as of this writing) is with uuids.
            # if pktyp == 'uuid':
            #     onlist += f"{_and} CAST( t.{pk} AS uuid)={cls.__tablename__}.{pk} "
            # else:
            onlist += f"{_and} t.{pk}={cls.__tablename__}.{pk} "
            _and = "AND"
            comma = ","

        with DBCon( dbcon ) as dbcon:
            q = f"SELECT * FROM {cls.__tablename__} JOIN (VALUES {mess}) AS t({collist}) ON {onlist} "
            rows, cols = dbcon.execute( q, subdict )

        objs = []
        for row in rows:
            obj = cls( _noinit=True )
            obj._set_self_from_fetch_cols_row( cols, row )
            objs.append( obj )

        return objs

    @classmethod
    def getbyattrs( cls, dbcon=None, **attrs ):
        """Get a list of objects whose attributes match the keyword arguments passed to this function.

        Parameters
        ----------
          dbcon : DBCon or psycopg.Connection, default None
            If given, use this database connection.  Otherwise, it will
            open and close a new database connection.

        """
        if cls._tablemeta is None:
            cls.load_table_meta( dbcon )

        # WORRY : when we edit attrs below, will that also affect anything outside
        #   this function?  E.g. if it's called with a ** itself.
        q = f"SELECT * FROM {cls.__tablename__} WHERE "
        _and = ""
        for k in attrs.keys():
            attrs[k] = cls._tablemeta[k].py_to_pg( attrs[k] )
            q += f"{_and} {k}=%({k})s "
            _and = "AND"

        with DBCon( dbcon ) as con:
            rows, cols = con.execute( q, attrs )

        objs = []
        for row in rows:
            obj = cls( _noinit=True )
            obj._set_self_from_fetch_cols_row( cols, row )
            objs.append( obj )

        return objs

    def refresh( self, dbcon=None ):
        """Reload the object from the database.

        Will set the attributes of the object based on the row from the
        database whose primary keys match the primary key of the object.
        (BE CAREFUL: this may not work if the table has a default for
        the primary key column, and you depended on the database to set
        that default, e.g. when using the insert() method.  In that
        case, the object may not know its own primary key!)

        Parameters
        ----------
          dbcon : DBCon or psycopg.Connection, default None
            If given, use this database connection.  Otherwise, it will
            open and close a new database connection.

        """

        q, subdict = self._construct_pk_query_where( *self.pks )
        q = f"SELECT * FROM {self.__tablename__} {q}"

        with DBCon( dbcon ) as con:
            rows, cols = con.execute( q, subdict )

        if len(rows) > 1:
            raise RuntimeError( f"Found more than one row in {self.__tablename__} with primary keys "
                                f"{self.pks}; this probably shouldn't happen." )
        if len(rows) == 0:
            raise ValueError( f"Failed to find row in {self.__tablename__} with primary keys {self.pks}" )

        self._set_self_from_fetch_cols_row( cols, rows[0] )


    def insert( self, dbcon=None, refresh=True, nocommit=False ):
        """Insert an object into the database.

        Columns in the database will be set based on attributes of the
        object.

        Parameters
        ----------
          dbcon : DBCon or psycopg.Connection, default None
            If given, use this database connection.  Otherwise, it will
            open and close a new database connection.

          refresh : bool, default True
            After inserting the object into the database, reread the
            parameters back from the database.  This will pick up any
            colums set from defaults.  WARNING: if you're depending on
            the database default to set the primary key, then there will
            be an exception if you don't set refresh=False!  Reason: the
            object has to know its own primary key in order to refresh
            itself from the database, but if you're using a databse
            default, then the object python-side won't know what the
            database set.

            Ignored if nocommit=True.

          nocommit : bool, default False
            Normally, after inserting, the database connection is
            committed so that the object will really be on the database.
            Set this to True to skip that step.  (You might do that,
            e.g., if you make a whole bunch of insert calls at once.)

        """

        if refresh and nocommit:
            raise RuntimeError( "Can't refresh with nocommit" )

        subdict = self._build_subdict()

        q = ( f"INSERT INTO {self.__tablename__}({','.join(subdict.keys())}) "
              f"VALUES ({','.join( [ f'%({c})s' for c in subdict.keys() ] )})" )

        with DBCon( dbcon ) as con:
            con.execute_nofetch( q, subdict )
            if not nocommit:
                con.commit()
                if refresh:
                    self.refresh( con )

    def delete_from_db( self, dbcon=None, nocommit=False ):
        """Delete the row from the database with the object's primary keys.

        Won't work if the object's primary key attributes aren't set, or are None.

        Paramaeters
        -----------
          dbcon : DBCon or psycopg.Connection, default None
            If given, use this database connection.  Otherwise, it will
            open and close a new database connection.

          nocmmit : bool, default False
            Normally, the connection is committed after the DELETE
            command is sent, so the row will really be removed.  Set
            this to True to skip the commit step (e.g. if you want to do
            several deletes in one commit).

        """
        where, subdict = self._construct_pk_query_where( me=self )
        q = f"DELETE FROM {self.__tablename__} {where}"
        with DBCon( dbcon ) as con:
            con.execute_nofetch( q, subdict )
            if not nocommit:
                con.commit()


    def update( self, dbcon=None, refresh=False, nocommit=False ):
        """Update the database row with attributes from the object.

        The object's primary keys must be set so this method can figure
        out which row to update!

        See the docs on _build_subdict for the complicated behavior for
        columns with no corresponding attribute, or when attributes are
        None.

        Parameters
        ----------
          dbcon : DBCon or psycopg.Connection, default None
            If given, use this database connection.  Otherwise, it will
            open and close a new database connection.

          refresh : bool, default False
            After updating, refresh the object from the database.  This
            is useful if, for instance, some attributes don't exist in
            the object for which there are columns in the database with
            defaults.  Requires nocommit=False.

          nocommit : bool, default False.
            Normally, the connection is committed after the UPDATE
            command is sent, so the row will really be updated.  Set
            this to True to skip the commit step (e.g. if you want to do
            several updates all in one commit).

        """
        if refresh and nocommit:
            raise RuntimeError( "Can't refresh with nocommit" )

        subdict = self._build_subdict()
        q = ( f"UPDATE {self.__tablename__} SET "
              f"{','.join( [ f'{c}=%({c})s' for c in subdict.keys() if c not in self._pk ] )} " )
        where, wheresubdict = self._construct_pk_query_where( me=self )
        subdict.update( wheresubdict )
        q += where

        with DBCon( dbcon ) as con:
            con.execute_nofetch( q, subdict )
            if not nocommit:
                con.commit()
                if refresh:
                    self.refresh( con )

    @classmethod
    def bulk_insert_or_upsert( cls, data, upsert=False, assume_no_conflict=False,
                               dbcon=None, nocommit=False ):
        """Try to efficiently insert a bunch of data into the database.

        ROB TODO DOCUMENT QUIRKS

        Parmeters
        ---------
          data: dict or list
            Can be one of:
              * a list of dicts.  The keys in all dicts (including order!) must be the same
              * a dict of lists
              * a list of objects of type cls

          upsert: bool, default False
             If False, then objects whose primary key is already in the
             database will be ignored.  If True, then objects whose
             primary key is already in the database will be updated with
             the values in dict.  (SQL will have ON CONFLICT DO NOTHING
             if False, ON CONFLICT DO UPDATE if True.)

          assume_no_conflict: bool, default False
             Usually you just want to leave this False.  There are
             obscure kludge cases (e.g. if you're playing games and have
             removed primary key constraints and you know what you're
             doing-- this happens in load_snana_fits.py, for instance)
             where the conflict clauses cause the sql to fail.  Set this
             to True to avoid having those clauses.

          dbcon : DBCon or psycopg.Connection, default None
            If given, use this database connection.  Otherwise, it will
            open and close a new database connection.

          nocommit : bool, default False
             This one is very scary and you should only use it if you
             really know what you're doing.  If this is True, not only
             will we not commit to the database, but we won't copy from
             the table temp_bulk_upsert to the table of interest.  It
             doesn't make sense to set this to True unless you also
             pass a dbcon.  This is for things that want to do stuff to
             the temp table before copying it over to the main table, in
             which case it's the caller's responsibility to do that copy
             and commit to the database.

        Returns
        -------
           int OR string
             If nocommit=False, returns the number of rows actually
             inserted (which may be less than len(data)).

             If nocommit=True, returns the string to execute to copy
             from the temp table to the final table.

        """

        if len(data) == 0:
            return

        if isinstance( data, list ) and isinstance( data[0], dict ):
            columns = data[0].keys()
            # Alas, psycopg's copy seems to index the thing it's passed,
            #   so we can't just pass it d.values()
            values = [ list( d.values() ) for d in data ]
        elif isinstance( data, dict ):
            columns = list( data.keys() )
            values = [ [ data[c][i] for c in columns ] for i in range(len(data[columns[0]])) ]
            # TODO : check that the lenght of all the lists in values is the
            #   same as the length of columns
        elif isinstance( data, list ) and isinstance( data[0], cls ):
            # This isn't entirely satisfying.  But, we're going
            #   to assume that things that are None because they
            #   want to use database defaults are going to be
            #   the same in every object.
            sd0 = data[0]._build_subdict()
            columns = sd0.keys()
            data = [ d._build_subdict( columns=columns ) for d in data ]
            # Alas, psycopg's copy seems to index the thing it's passed,
            #   so we can't just pass it d.values()
            values = [ list( d.values() ) for d in data ]
        else:
            raise TypeError( f"Invalid type for data: {type(data)}" )

        with DBCon( dbcon ) as con:
            con.execute_nofetch( "DROP TABLE IF EXISTS temp_bulk_upsert", explain=False, analyze=False )
            con.execute_nofetch( f"CREATE TEMP TABLE temp_bulk_upsert (LIKE {cls.__tablename__} INCLUDING DEFAULTS)",
                                 explain=False, analyze=False )
            with con.cursor.copy( f"COPY temp_bulk_upsert({','.join(columns)}) FROM STDIN" ) as copier:
                for v in values:
                    copier.write_row( v )

            if not assume_no_conflict:
                if not upsert:
                    conflict = f"ON CONFLICT ({','.join(cls._pk)}) DO NOTHING"
                else:
                    conflict = ( f"ON CONFLICT ({','.join(cls._pk)}) DO UPDATE SET "
                                 + ",".join( f"{c}=EXCLUDED.{c}" for c in columns ) )
            else:
                conflict = ""

            q = f"INSERT INTO {cls.__tablename__} SELECT * FROM temp_bulk_upsert {conflict}"

            if nocommit:
                return q
            else:
                con.execute_nofetch( q, explain=False, analyze=False )
                ninserted = con.cursor.rowcount
                con.execute_nofetch( "DROP TABLE temp_bulk_upsert", explain=False, analyze=False )
                con.commit()
                return ninserted


# ======================================================================

class AuthUser( DBBase ):
    __tablename__ = "authuser"
    _tablemeta = None
    _pk = [ 'id' ]

    def __init__( self, *args, **kwargs ):
        super().__init__( *args, **kwargs )


# ======================================================================

class PasswordLink( DBBase ):
    __tablename__ = "passwordlink"
    _tablemeta = None
    _pk = [ 'id' ]


# ======================================================================

class BaseProcessingVersion( DBBase ):
    __tablename__ = "base_processing_version"
    _tablemeta = None
    _pk = [ 'id' ]

    @classmethod
    def base_procver_id( cls, base_processing_version, dbcon=None ):
        """Return the uuid of base_processing_version.

        Parameters
        ----------
          base_processing_version: str or UUID
            If a UUID, just return it straight.  If a str that is a string
            version of a UUID, UUIDifies it and returns it.  Otherwise,
            queries the database for the base processing version and returns
            the UUID.

         dbcon: db.DBCon or psycopg2.connection or NOne
           Databse connection to use.  If None, and one is needed, will
           open a new one and close it when done.

        Returns
        -------
          UUID

        """

        if isinstance( base_processing_version, uuid.UUID ):
            return base_processing_version
        try:
            bpv = uuid.UUID( base_processing_version )
            return bpv
        except Exception:
            pass
        with DBCon( dbcon ) as con:
            rows, _cols = con.execute( "SELECT id FROM base_processing_version WHERE description=%(pv)s",
                                       { 'pv': base_processing_version } )
            if len(rows) == 0:
                raise ValueError( f"Unknown base processing version {base_processing_version}" )
            return rows[0][0]


# ======================================================================

class ProcessingVersion( DBBase ):
    __tablename__ = "processing_version"
    _tablemeta = None
    _pk = [ 'id' ]

    @classmethod
    def procver_id( cls, processing_version, dbcon=None ):
        """Return the uuid of processing_version.

        Will also search procesing version aliases if necessary.

        Parameters
        ----------
          processing_version: str or UUID
            If a UUID, just return it straight.  If a str that is a string
            version of a UUID, UUIDifies it and returns it.  Otherwise,
            queries the database for the processing version and returns the
            UUID.

          dbcon: db.DBCon or psycopg.Connection or None
            Database connection to use.  If None, and one is needed, will
            open a new one and close it when done.

        Returns
        -------
          UUID

        """

        if isinstance( processing_version, uuid.UUID ):
            return processing_version
        try:
            ipv = uuid.UUID( processing_version )
            return ipv
        except Exception:
            pass
        with DBCon( dbcon ) as con:
            rows, _cols = con.execute( "SELECT id FROM processing_version WHERE description=%(pv)s",
                                       { 'pv': processing_version } )
            if len(rows) > 0:
                return rows[0][0]
            rows, _cols = con.execute( "SELECT procver_id FROM processing_version_alias WHERE description=%(pv)s",
                                       { 'pv': processing_version } )
            if len(rows) == 0:
                raise ValueError( f"Unknown processing version {processing_version}" )
            return rows[0][0]


    def highest_prio_base_procver( self, dbcon=None ):
        """Returns the highest priority base_processing_version associated with this processing version.

        Be careful with this.  If you don't fully understand the
        processing version scheme (and, Rob, if you haven't thought
        about it recently, that probably includes you), you can use this
        wrong.  Because there are multiple base processing versions for
        one processing version, using this to search for things is
        almost certainly the wrong thing to do; it undercuts the whole
        "fall back to other versions" nature of processing_version.
        This method may be useful for figuring out the base processing
        version to insert something under.

        Parameters
        ----------
          dbcon : DBCon or psycopg.Connection, default None
            Database connection to use.  If not given, will open a new
            one and close it when done.

        Returns
        -------
          BaseProcessingVersion

        """
        with DBCon( dbcon, dictcursor=True ) as con:
            bpv = con.execute( "SELECT b.* FROM base_processing_version b\n"
                               "INNER JOIN base_procver_of_procver j ON j.base_procver_id=b.id\n"
                               "WHERE j.procver_id=%(pv)s\n"
                               "ORDER BY j.priority DESC\n"
                               "LIMIT 1",
                               { 'pv': self.id } )
            if len(bpv) == 0:
                raise ValueError( f"Can't find base processing version for processing version {self.description}" )
            bpv = bpv[0]
            return BaseProcessingVersion( **bpv, noconvert=False )


# ======================================================================

class ProcessingVersionAlias( DBBase ):
    __tablename__ = "processing_version_alias"
    _tablemeta = None
    _pk = [ 'description' ]


# ======================================================================

class HostGalaxy( DBBase ):
    __tablename__ = "host_galaxy"
    _tablemeta = None
    _pk = [ 'id' ]


# ======================================================================

class RootDiaObject( DBBase ):
    __tablename__ = "root_diaobject"
    _tablemeta = None
    _pk = [ 'id' ]


# ======================================================================

class DiaObject( DBBase ):
    __tablename__ = "diaobject"
    _tablemeta = None
    _pk = [ 'diaobjectid', 'base_procver_id' ]


# ======================================================================

class DiaSource( DBBase ):
    __tablename__ = "diasource"
    _tablemeta = None
    _pk = [ 'base_procver_id', 'diaobjectid', 'visit' ]

    _flags_bits = { 0x00000001: 'centroid_flag',
                    0x00000002: 'apFlux_flag',
                    0x00000004: 'apFlux_flag_apertureTruncated',
                    0x00000008: 'isNegative',
                    0x00000010: 'psfFlux_flag',
                    0x00000020: 'psfFlux_flag_edge',
                    0x00000040: 'psfFlux_flag_noGoodPixels',
                    0x00000080: 'trail_flag_edge',
                    0x00000100: 'forced_PsfFlux_flag',
                    0x00000200: 'forced_PsfFlux_flag_edge',
                    0x00000400: 'forced_PsfFlux_flag_noGoodPixels',
                    0x00000800: 'shape_flag',
                    0x00001000: 'shape_flag_no_pixels',
                    0x00002000: 'shape_flag_not_contained',
                    0x00004000: 'shape_flag_parent_source',
                    0x00008000: 'isDipole',
                    0x00010000: 'dipleFitAttempted',
                    0x00020000: 'glint_trail',
                   }

    _pixelflags_bits = { 0x00000001: 'pixelFlags',
                         0x00000002: 'pixelFlags_bad',
                         0x00000004: 'pixelFlags_cr',
                         0x00000008: 'pixelFlags_crCenter',
                         0x00000010: 'pixelFlags_edge',
                         0x00000020: 'pixelFlags_nodata',
                         0x00000040: 'pixelFlags_nodataCenter',
                         0x00000080: 'pixelFlags_interpolated',
                         0x00000100: 'pixelFlags_interpolatedCenter',
                         0x00000200: 'pixelFlags_offimage',
                         0x00000400: 'pixelFlags_saturated',
                         0x00000800: 'pixelFlags_saturatedCenter',
                         0x00001000: 'pixelFlags_suspect',
                         0x00002000: 'pixelFlags_suspectCenter',
                         0x00004000: 'pixelFlags_streak',
                         0x00008000: 'pixelFlags_streakCenter',
                         0x00010000: 'pixelFlags_injected',
                         0x00020000: 'pixelFlags_injectedCenter',
                         0x00040000: 'pixelFlags_injected_template',
                         0x00080000: 'pixelFlags_injectedd_templateCenter',
                        }


# ======================================================================

class DiaForcedSource( DBBase ):
    __tablename__ = "diaforcedsource"
    _tablemeta = None
    _pk = [ 'base_procver_id', 'diaobjectid', 'visit' ]


# ======================================================================
# Spectrum cycle tables

class SpectrumInfo( DBBase ):
    __tablename__ = "spectruminfo"
    _tablemeta = None
    _pk = [ 'specinfo_id' ]


class WantedSpectra( DBBase ):
    __tablename__ = "wantedspectra"
    _tablemeta = None
    _pk = [ 'wantspec_id' ]


class PlannedSpectra( DBBase ):
    __tablename__ = "plannedspectra"
    _tablemeta = None
    _pk = [ 'plannedspec_id' ]


# ======================================================================
# SNANA PPDB simulation tables

class PPDBHostGalaxy( DBBase ):
    __tablename__ = "ppdb_host_galaxy"
    _tablemeta = None
    _pk = [ 'id' ]


class PPDBDiaObject( DBBase ):
    __tablename__ = "ppdb_diaobject"
    _tablemeta = None
    _pk = [ 'diaobjectid' ]


class PPDBDiaSource( DBBase ):
    __tablename__ = "ppdb_diasource"
    _tablemeta = None
    _pk = [ 'diaobjectid', 'visit' ]


class PPDBDiaForcedSource( DBBase ):
    __tablename__ = "ppdb_diaforcedsource"
    _tablemeta = None
    _pk = [ 'diaobjectid', 'visit' ]


# ======================================================================
class QueryQueue( DBBase ):
    __tablename__ = "query_queue"
    _tablemeta = None
    _pk = [ 'queryid' ]

    # Think... would it be OK to let this update?
    def update( self, dbcon=None, refresh=False, nocommit=False ):
        raise NotImplementedError( "update not implemented for QueryQueue" )
