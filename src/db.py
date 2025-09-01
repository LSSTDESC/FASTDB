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

from contextlib import contextmanager

import numpy as np
import psycopg
import psycopg.rows
import psycopg.types.json
import pymongo


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

    Consider using the DB context manager instead of this.
    """

    global dbuser, dbpasswd, dbhost, dbport, dbname
    conn = psycopg.connect( dbname=dbname, user=dbuser, password=dbpasswd, host=dbhost, port=dbport )
    return conn


@contextmanager
def DB( dbcon=None ):
    """Get a database connection in a context manager.

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
        # 'uuid': ( str, util.asUUID ),      # Doesn't seem to be needed any more for psycopg3
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
            self._load_table_meta()
        return self._tablemeta

    @property
    def pks( self ):
        return [ getattr( self, k ) for k in self._pk ]


    @classmethod
    def load_table_meta( cls, dbcon=None ):
        if cls._tablemeta is not None:
            return

        with DB( dbcon ) as con:
            cursor = con.cursor( row_factory=psycopg.rows.dict_row )
            cursor.execute( "SELECT c.column_name,c.data_type,c.column_default,c.is_nullable,"
                            "       e.data_type AS element_type "
                            "FROM information_schema.columns c "
                            "LEFT JOIN information_schema.element_types e "
                            "  ON ( (c.table_catalog, c.table_schema, c.table_name, "
                            "        'TABLE', c.dtd_identifier) "
                            "      =(e.object_catalog, e.object_schema, e.object_name, "
                            "        e.object_type, e.collection_type_identifier) ) "
                            "WHERE table_name=%(table)s",
                            { 'table': cls.__tablename__ } )
            cols = cursor.fetchall()

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
        """Create an object based on a row returned from psycopg's cursor.fetch*.

        You could probably use this also just to create an object fresh; in
        that case, you *probably* want to set noconvert to True.

        """

        if _noinit:
            return

        self.load_table_meta( dbcon=dbcon )
        mycols = set( self._tablemeta.keys() )

        if not ( ( cols is None ) and ( vals is None ) ):
            if ( cols is None ) or ( vals is None ):
                raise ValueError( "Both or neither of cols and vals must be none." )
            if ( ( not isinstance( cols, collections.abc.Sequence ) ) or ( isinstance( cols, str ) ) or
                 ( not isinstance( vals, collections.abc.Sequence ) ) or ( isinstance( vals, str ) ) or
                 ( len( cols ) != len( vals ) )
                ):
                raise ValueError( "cols and vals most both be lists of the same length" )

        if cols is not None:
            if len(kwargs) > 0:
                raise ValueError( "Can only column values as named arguments "
                                  "if cols and vals are both None" )
        else:
            cols = kwargs.keys()
            vals = kwargs.values()

        keys = set( cols )
        if not keys.issubset( mycols ):
            raise RuntimeError( f"Unknown columns for {self.__tablename__}: {keys-mycols}" )

        for col in mycols:
            setattr( self, col, None )

        self._set_self_from_fetch_cols_row( cols, vals )


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
        """Get an object from a table row with the specified primary key(s)."""

        q, subdict = cls._construct_pk_query_where( *args )
        q = f"SELECT * FROM {cls.__tablename__} {q}"
        with DB( dbcon ) as con:
            cursor = con.cursor()
            cursor.execute( q, subdict )
            cols = [ desc[0] for desc in cursor.description ]
            rows = cursor.fetchall()

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

        with DB( dbcon ) as con:
            cursor = con.cursor()
            q = f"SELECT * FROM {cls.__tablename__} JOIN (VALUES {mess}) AS t({collist}) ON {onlist} "
            cursor.execute( q, subdict )
            cols = [ desc[0] for desc in cursor.description ]
            rows = cursor.fetchall()

        objs = []
        for row in rows:
            obj = cls( _noinit=True )
            obj._set_self_from_fetch_cols_row( cols, row )
            objs.append( obj )

        return objs

    @classmethod
    def getbyattrs( cls, dbcon=None, **attrs ):
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

        with DB( dbcon ) as con:
            cursor = con.cursor()
            cursor.execute( q, attrs )
            cols = [ desc[0] for desc in cursor.description ]
            rows = cursor.fetchall()

        objs = []
        for row in rows:
            obj = cls( _noinit=True )
            obj._set_self_from_fetch_cols_row( cols, row )
            objs.append( obj )

        return objs

    def refresh( self, dbcon=None ):
        q, subdict = self._construct_pk_query_where( *self.pks )
        q = f"SELECT * FROM {self.__tablename__} {q}"

        with DB( dbcon ) as con:
            cursor = con.cursor()
            cursor.execute( q, subdict )
            cols = [ desc[0] for desc in cursor.description ]
            rows = cursor.fetchall()

        if len(rows) > 1:
            raise RuntimeError( f"Found more than one row in {self.__tablename__} with primary keys "
                                f"{self.pks}; this probably shouldn't happen." )
        if len(rows) == 0:
            raise ValueError( f"Failed to find row in {self.__tablename__} with primary keys {self.pks}" )

        self._set_self_from_fetch_cols_row( cols, rows[0] )


    def insert( self, dbcon=None, refresh=True, nocommit=False ):
        if refresh and nocommit:
            raise RuntimeError( "Can't refresh with nocommit" )

        subdict = self._build_subdict()

        q = ( f"INSERT INTO {self.__tablename__}({','.join(subdict.keys())}) "
              f"VALUES ({','.join( [ f'%({c})s' for c in subdict.keys() ] )})" )

        with DB( dbcon ) as con:
            cursor = con.cursor()
            cursor.execute( q, subdict )
            if not nocommit:
                con.commit()
                if refresh:
                    self.refresh( con )

    def delete_from_db( self, dbcon=None, nocommit=False ):
        where, subdict = self._construct_pk_query_where( me=self )
        q = f"DELETE FROM {self.__tablename__} {where}"
        with DB( dbcon ) as con:
            cursor = con.cursor()
            cursor.execute( q, subdict )
            con.commit()


    def update( self, dbcon=None, refresh=False, nocommit=False ):
        if refresh and nocommit:
            raise RuntimeError( "Can't refresh with nocommit" )

        subdict = self._build_subdict()
        q = ( f"UPDATE {self.__tablename__} SET "
              f"{','.join( [ f'{c}=%({c})s' for c in subdict.keys() if c not in self._pk ] )} " )
        where, wheresubdict = self._construct_pk_query_where( me=self )
        subdict.update( wheresubdict )
        q += where

        with DB( dbcon) as con:
            cursor = con.cursor()
            cursor.execute( q, subdict )
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

          assume_no_conflict: bool, default Falsea
             Usually you just want to leave this False.  There are
             obscure kludge cases (e.g. if you're playing games and have
             removed primary key constraints and you know what you're
             doing-- this happens in load_snana_fits.py, for instance)
             where the conflict clauses cause the sql to fail.  Set this
             to True to avoid having those clauses.

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
            raise TypeError( f"data must be something other than a {cls.__name__}" )

        with DB( dbcon ) as con:
            cursor = con.cursor()
            cursor.execute( "DROP TABLE IF EXISTS temp_bulk_upsert" )
            cursor.execute( f"CREATE TEMP TABLE temp_bulk_upsert (LIKE {cls.__tablename__})" )
            with cursor.copy( f"COPY temp_bulk_upsert({','.join(columns)}) FROM STDIN" ) as copier:
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
                cursor.execute( q )
                ninserted = cursor.rowcount
                cursor.execute( "DROP TABLE temp_bulk_upsert" )
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

class ProcessingVersion( DBBase ):
    __tablename__ = "processing_version"
    _tablemeta = None
    _pk = [ 'id' ]


# ======================================================================

class ProcessingVersionAlias( DBBase ):
    __tablename__ = "processing_version_alias"
    _tablemeta = None
    _pk = [ 'description' ]


# ======================================================================

class Snapshot( DBBase ):
    __tablename__ = "snapshot"
    _tablemeta = None
    _pk = [ 'id' ]


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
    _pk = [ 'diaobjectid', 'processing_version' ]
    npix_order29 = None


# ======================================================================

class DiaObjectRootMap( DBBase ):
    __tablename__ = "diaobject_root_map"
    _tablemeta = None
    _pk = [ 'rootid', 'diaobjectid', 'processing_version' ]


# ======================================================================

class DiaSource( DBBase ):
    __tablename__ = "diasource"
    _tablemeta = None
    _pk = [ 'diasourceid', 'processing_version' ]


# ======================================================================

class DiaForcedSource( DBBase ):
    __tablename__ = "diaforcedsource"
    _tablemeta = None
    _pk = [ 'diaforcedsourceid', 'processing_version' ]


# ======================================================================

class DiaObjectSnapshot( DBBase ):
    __tablename__ = "diaobject_snapshot"
    _tablemeta = None
    _pk = [ 'diaobjectid', 'processing_vesion', 'snapshot' ]


# ======================================================================

class DiaSourceSnapshot( DBBase ):
    __tablename__ = "diasource_snapshot"
    _tablemeta = None
    _pk = [ 'diasourceid', 'processing_version', 'snapshot' ]


# ======================================================================

class DiaForcedSourceSnapshot( DBBase ):
    __tablename__ = "diaforcedsource_snapshot"
    _tablemeta = None
    _pk = [ 'diaforcedsourceid', 'processing_version', 'snapshot' ]


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
    _pk = [ 'diasourceid' ]


class PPDBDiaForcedSource( DBBase ):
    __tablename__ = "ppdb_diaforcedsource"
    _tablemeta = None
    _pk = [ 'diaforcedsourceid' ]


# ======================================================================
class QueryQueue( DBBase ):
    __tablename__ = "query_queue"
    _tablemeta = None
    _pk = [ 'queryid' ]

    # Think... would it be OK to let this update?
    def update( self, dbcon=None, refresh=False, nocommit=False ):
        raise NotImplementedError( "update not implemented for QueryQueue" )
