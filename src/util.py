__all__ = [ "FDBLogger", "parse_bool", "env_as_bool", "asUUID", "isSequence",
            "float_or_none_from_dict", "int_or_none_from_dict",
            "datetime_or_none_from_dict_mjd_or_timestring", "mjd_or_none_from_dict_mjd_or_timestring",
            "datetime_to_utc",
            "parse_sexigesimal", "float_or_none_from_dict_float_or_dms", "float_or_none_from_dict_float_or_hms",
             "mjd_from_mjd_or_datetime_or_timestring", "laboriously_construct_pandas",
             "get_alert_schema", "procver_id" ]

import sys
import os
import re
import datetime
import pytz
import pathlib
import logging
import numbers
import itertools
import uuid
import collections.abc
import multiprocessing

import numpy as np
import pandas
import fastavro
import astropy.time
import rkwebutil

_fastdb_schema_namespace = 'fastdb.v10_0_0'
_lsst_schema_namespace = 'lsst.v10_0'

_default_datefmt = '%Y-%m-%d %H:%M:%S'
# _default_log_level = logging.DEBUG
_default_log_level = logging.INFO
# Normally you don't want to show milliseconds, because it's additional gratuitous information
#  that makes log output lines longer.  But, if you're debugging timing stuff, you might want
#  temporarily to set this to True.
# _show_millisec = True
_show_millisec = False

# DEPRECATED -- don't use this, use FDBLogger
logger = logging.getLogger( "FASTDB logger" )
_logout = logging.StreamHandler( sys.stderr )
logger.addHandler( _logout )
_formatter = logging.Formatter( '[%(asctime)s - %(levelname)s] - %(message)s',
                                datefmt='%Y-%m-%d %H:%M:%S' )
_logout.setFormatter( _formatter )
logger.propagate = False
logger.setLevel( _default_log_level )


# ======================================================================

class FDBLogger:
    _instance = None
    _ordinal = 0

    @classmethod
    def instance( cls, midformat=None, datefmt=_default_datefmt, level=_default_log_level ):
        """Return the singleton instance of FDBLogger."""
        if cls._instance is None:
            cls._instance = cls( midformat=midformat, datefmt=datefmt, level=level )
        return cls._instance

    @classmethod
    def get( cls ):
        """Return the logging.Logger object."""
        return cls.instance()._logger

    @classmethod
    def replace( cls, midformat=None, datefmt=None, level=None, propagate=None ):
        """Replace the logging.Logger object with a new one.

        Subsequent calls to FDBLogger.get(), .info(), etc. will now
        return the new one.  Will inherit the midformat, datefmt, and
        level from the current logger if they aren't specified here.

        See __init__ for parameters.

        Returns the logging.Logger object you'd get from get().

        """
        if cls._instance is not None:
            midformat = cls._instance.midformat if midformat is None else midformat
            datefmt = cls._instance.datefmt if datefmt is None else datefmt
            level = cls._instance._logger.level if level is None else level
            propagate = cls._instance._logger.propagate if propagate is None else propagate
        else:
            datefmt = _default_datefmt if datefmt is None else datefmt
            level = _default_log_level if level is None else level
            propagate = False if propagate is None else propagate
        cls._instance = cls( midformat=midformat, datefmt=datefmt, level=level, propagate=propagate )
        return cls._instance

    @classmethod
    def multiprocessing_replace( cls, datefmt=None, level=None, pid=False ):
        """Shorthand for replace with midformat parsed from the current multiprocessing process."""

        me = multiprocessing.current_process()
        num = str( me.pid )
        if not pid:
            # Usually processes are named things like ForkPoolWorker-{number}, or something
            match = re.search( '([0-9]+)', me.name )
            if match is not None:
                num = f'{int(match.group(1)):3d}'
        cls.replace( midformat=num, datefmt=datefmt, level=level )

    @classmethod
    def set_level( cls, level=_default_log_level ):
        """Set the log level of the logging.Logger object."""
        cls.instance()._logger.setLevel( level )

    @classmethod
    def setLevel( cls, level=_default_log_level ):
        """Set the log level of the logging.Logger object."""
        cls.instance()._logger.setLevel( level )

    @classmethod
    def getEffectiveLevel( cls ):
        return cls.instance()._logger.getEffectiveLevel()

    @classmethod
    def debug( cls, *args, **kwargs ):
        cls.get().debug( *args, **kwargs )

    @classmethod
    def info( cls, *args, **kwargs ):
        cls.get().info( *args, **kwargs )

    @classmethod
    def warning( cls, *args, **kwargs ):
        cls.get().info( *args, **kwargs )

    @classmethod
    def error( cls, *args, **kwargs ):
        cls.get().error( *args, **kwargs )

    @classmethod
    def critical( cls, *args, **kwargs ):
        cls.get().critical( *args, **kwargs )

    @classmethod
    def exception( cls, *args, **kwargs ):
        cls.get().exception( *args, **kwargs )

    def __init__( self, midformat=None, datefmt=_default_datefmt,
                  show_millisec=_show_millisec, level=_default_log_level,
                  propagate =False ):
        """Initialize a FDBLogger object, and the logging.Logger object it holds.

        Parameters
        ----------
        midformat : string, default None
            The standard formatter emits log messages like "[yyyy-mm-dd
            HH:MM:SS - INFO] Message".  If given, this adds something between the
            date and the log level ("[yyyy-mm-dd HH:MM:SS - {midformat}
            - INFO]...").  Useful, for instance, in multiprocessing to
            keep track of which process the message came from.

        datefmt : string, default '%Y-%m-%d %H:%M:%S'
            The date format to use, using standard logging.Formatter
            datefmt syntax.

        show_millisec: bool, default False
            Add millseconds after a . following the date formatted by datefmt.

        level : logging level constant, default logging.WARNING
            This can be changed later with set_level().

        propagate : bool, default False
            The value to set for the propagate property of the created logger.

        """
        FDBLogger._ordinal += 1
        self._logger = logging.getLogger( f"SeeChange_{FDBLogger._ordinal}" )
        self._logger.propagate = propagate

        self.midformat = midformat
        self.datefmt = datefmt

        logout = logging.StreamHandler( sys.stderr )
        fmtstr = "[%(asctime)s"
        if show_millisec:
            fmtstr += ".%(msecs)03d"
        fmtstr += " - "
        if midformat is not None:
            fmtstr += f"{midformat} - "
        fmtstr += "%(levelname)s] - %(message)s"
        formatter = logging.Formatter( fmtstr, datefmt=datefmt )
        logout.setFormatter( formatter )
        self._logger.addHandler( logout )
        self._logger.setLevel( level )


# ======================================================================

def parse_bool( val ):
    """Check if a variable represents a boolean value is True or False."""
    if val is None:
        return False
    if isinstance( val, bool ):
        return val
    if isinstance( val, numbers.Integral ):
        return bool( val )
    if isinstance( val, str ):
        if val.strip().lower() in [ 'true', 'yes', '1' ]:
            return True
        if val.strip().lower() in [ 'false', 'no', '0' ]:
            return False
    raise ValueError( f'Cannot parse boolean value from "{val}" (type {type(val)})' )


def env_as_bool( var ):
    """Parse an environment variable as a boolean."""
    return parse_bool( os.getenv( var ) )


def asUUID( id ):
    if id is None:
        return None
    elif isinstance( id, uuid.UUID ):
        return id
    elif isinstance( id, str ):
        return uuid.UUID( id )
    else:
        raise ValueError( f"Don't know how to turn {id} into UUID." )


NULLUUID = asUUID( '00000000-0000-0000-0000-000000000000' )


def isSequence( var ):
    """Return True if var is a sequence, but not a string or bytes.

    Todo: figure out other things we want to exclude.

    The goal is to return True if it's a list, tuple, array, or
    something that works like that.

    """
    return ( isinstance( var, collections.abc.Sequence )
             and not ( isinstance( var, str ) or
                       isinstance( var, bytes ) ) )


# These next few will, by design, raise an exception of d[kw] isn't empty and can't be parsed to the right thing

def float_or_none_from_dict( d, kw ):
    if ( kw not in d ) or ( d[kw] is None ):
        return None

    if isinstance( d[kw], str ):
        return None if len( d[kw].strip() ) == 0 else float( d[kw] )

    return float( d[kw] )


def int_or_none_from_dict( d, kw ):
    if ( kw not in d ) or ( d[kw] is None ):
        return None

    if isinstance( d[kw], str ):
        return None if len( d[kw].strip() ) == 0 else int( d[kw] )

    return int( d[kw] )


def datetime_or_none_from_dict_mjd_or_timestring( d, kw ):
    if ( kw not in d ) or ( d[kw] is None ) or ( len( d[kw].strip() ) == 0 ):
        return None

    try:
        dateval = rkwebutil.asDateTime( d[kw].strip(), defaultutc=True )
        return dateval
    except rkwebutil.ErrorMsg:
        mjd = float( d[kw].strip() )
        return astropy.time.Time( mjd, format='mjd' ).to_datetime( timezome=datetime.UTC )


def mjd_or_none_from_dict_mjd_or_timestring( d, kw ):
    if ( kw not in d ) or ( d[kw] is None ) or ( len( d[kw].strip() ) == 0 ):
        return None

    try:
        dateval = rkwebutil.asDateTime( d[kw], defaultutc=True )
        return astropy.time.Time( dateval, format='datetime' ).mjd
    except rkwebutil.ErrorMsg:
        return float( d[kw].strip() )


_sexigesimalre = re.compile( r'^\s*(?P<sign>[\-\+])?\s*(?P<d>[0-9]{0,3})\s*:\s*(?P<m>[0-9]{0,2})'
                             r'\s*:\s*(?P<s>[0-9]*\.?[0-9]+)\s*$' )
def parse_sexigesimal( val, deg=True ):  # noqa: E302
    global _sexigesimalre

    try:
        match = _sexigesimalre.search( val )
    except Exception:
        raise ValueError( f"Can't parse {val} (type {type(val)}) as sexigesimal" )
    if match is None:
        raise ValueError( f"Can't parse {val} (type {type(val)}) as sexigesimal" )
    sgn = -1 if match.group('sign') == '-' else 1
    d = int( match.group('d') )
    if ( deg and d >= 360. ) or ( (not deg) and ( d>=24. ) ):
        raise ValueError( f"Invalid {'degrees' if deg else 'hours'} {d}" )
    m = int( match.group('m') )
    if ( m >= 60. ):
        raise ValueError( f"Invalid minutes {m}" )
    s = int( match.group('s') )
    if ( s >= 60. ):
        raise ValueError( f"Invalid seconds {s}" )

    return sgn * ( d + m / 60. + s / 3600. )


def float_or_none_from_dict_float_or_dms( d, kw ):
    if ( kw not in d ) or ( d[kw] is None ):
        return None

    if isinstance( d[kw], str ) and ( len( d[kw].strip() ) == 0 ):
        return None

    try:
        return parse_sexigesimal( d[kw], deg=True )
    except ValueError:
        return float( d[kw] )


def float_or_none_from_dict_float_or_hms( d, kw ):
    if ( kw not in d ) or ( d[kw] is None ):
        return None

    if isinstance( d[kw], str ) and ( len( d[kw].strip() ) == 0 ):
        return None

    try:
        return 15. * parse_sexigesimal( d[kw], deg=False )
    except ValueError:
        return float( d[kw] )

    if isinstance( d[kw], numbers.Real ):
        return float( d[kw] )


def mjd_from_mjd_or_datetime_or_timestring( d ):
    if d is None:
        return None
    elif isinstance( d, numbers.Integral ):
        return d
    else:
        return astropy.time.Time( rkwebutil.asDateTime( d ), format='datetime' ).mjd


def datetime_to_utc( t, with_tz=False, now_on_none=False ):
    # mongodb doesn't seem to know about timezones and stores everythning UTC.
    # Try to adapt.  If we get a timezone unaware datetime, assume it's already UTC.
    # https://xkcd.com/1883/

    if t is None:
        if not now_on_none:
            return None

        t = datetime.datetime.now( tz=datetime.UTC )

    else:
        if isinstance( t, str ):
            t = datetime.datetime.fromisoformat( t )
        elif not isinstance( t, datetime.datetime ):
            raise TypeError( f"Must pass time parameters as datetime or a ISO string that can be "
                             f"parsed to a datetime; got a {type(t)}" )

        if t.tzinfo is not None:
            t = t.astimezone( datetime.UTC )
        else:
            t= pytz.timezone( 'UTC' ).localize( t )

    if not with_tz:
        t.replace( tzinfo=None )

    return t


def pandas_to_list( values ):
    # Function calling overhead makes me a little quasy.  In C this would have been an inline or macro.
    return [ ( None if isinstance(v, pandas.api.typing.NAType) else v )
             for v in values ]


def laboriously_construct_pandas( data, columns=None, int16cols=[], int32cols=[], int64cols=[],
                                  floatcols=[], doublecols=[], boolcols=[], keyname=None, indices=None,
                                  ignore_missing_cols=False ):
    """Convert one of three python structures to a pandas DataFrame.

    Two of these structures nominally could be constrcuted by just
    feeding them to pandas.DataFrame.  However, that will do bad things.
    If any of the int64 fields have None in the list, they will be
    converted to doubles by Pandas, because by default it only knows how
    to do NaN for floating point values.  That will destroy the int64
    values, as a double only has 53 bits of precision.  Instead, we have
    to use pyarrow column types, which do allow for nulls, but pandas
    does not have the ability to pass types for multiple columns at
    once.  As such, we have to build several Series objects, each with
    the type we want, and then stitch them together into a DataFrame.

    The three structures it can take are (in example for):

      { 'a': [ 1, 2, 3 ], 'b': [ 4, 5, 6 ] }

        This will yield a data frame with columns 'a' and 'b', and three rows.

      [ { 'a': 1, 'b': 4 }, { 'a': 2, 'b': 5 }, { 'a': 3, 'b': 6 } ]

        This will yield exactly the same data frame.

      { 'first': { 'a': [ 1, 2, 3 ], 'b': [4, 5, 6], 'c': [7, 8, 9] },
        'second': { 'a': [1, 2], 'b': [10, None], 'c': [ 2.718, 3.141 ] }
      }

        This is the natural structure for the return from
        ltcv.py::many_object_ltcvs.  The keys of the outer dictionary
        are the rootid of the objects, and the inner dictionary keys are
        all things like 'band', 'mjd', 'flux'...: the lightcurve for
        that object.  This *requres* a keyname, and a second index (at
        least) be passed in the indices column.  It will be turned into
        a pandas dataframe whose first index is keyname and whose index
        values are the values of the outer dictionary, and whose
        subsequent indices are the inner dictionary keys given in the
        indices argument.  If this was called with kwargs
        (keyname='which', indices=['a'], int32cols=['b'],
        floatcols['c']), the resultant dataframe would look like:

                       b      c
          which  a
          first  1     4    7.0
                 2     5    8.0
                 3     6    9.0
          second 1    10  2.718
                 2  <NA>  3.141


        with df.b.dtype=int32[pyarrow] and df.c.dtype=float[pyarrow].

    With all data types, it is strongly recommended to indicate the
    types of all columns by including the colum names in the int16cols,
    int32cols, int64cols, floatcols, doublecols, and boolcols arguments
    (all lists of names).  If there are any columns not in one of those
    lists, it will be an exception unless ignore_missingm_cols is True.
    (If there are any string columns, then you must set
    ignore_missing_cols to False and just not include the string columns
    in the lists passed to the type arguments.)

    (Note: pandas does not have a nullable bool datatype, even using
    pyarrows types, so boolean columns are converted to in16 columns
    with 1 for True, 0 for False, and <NA> for None.)

    """
    FDBLogger.debug( "Laboriously construting a pandas dataframe..." )

    serieses = {}

    def get_dtypes( columns ):
        dtypes = {}
        missing = set()
        for col in columns:
            # Pandas doesn't seem to have a bool type that one can nullify, so turn it into an int
            if ( col in int16cols ) or ( col in boolcols ):
                dtype = "int16[pyarrow]"
            elif col in int32cols:
                dtype = "int32[pyarrow]"
            elif col in int64cols:
                dtype = "int64[pyarrow]"
            elif col in floatcols:
                dtype = "float32[pyarrow]"
            elif col in doublecols:
                dtype = "float64[pyarrow]"
            else:
                dtype = None
                if not ignore_missing_cols:
                    missing.add( col )
            dtypes[col] = dtype

        if len( missing ) > 0:
            raise ValueError( f"Unknown columns: {missing}" )

        return dtypes


    if len(data) == 0:
        if columns is None:
            FDBLogger.debug( "...there were no columns." )
            return pandas.DataFrame( {} )
        else:
            dtypes= get_dtypes( columns )
            serieses = { c: pandas.Series( [], dtype=dtypes[c] ) for c in columns }

    elif isSequence( data ):
        if all( isinstance( row, dict ) for row in data ):
            if columns is not None:
                raise ValueError( "Cannot pass columns with a list of dictionaries" )
            keys = set( data[0].keys() )
            if any( set( r.keys() != keys for r in data ) ):
                raise ValueError( "List of dicts must all have the same keys" )
            columns = list( data[0].keys() )
            dtypes = get_dtypes( columns )
            serieses = { c: pandas.Series( ( r[c] for r in data ), dtype=dtypes[c] ) for c in columns }


        elif all( isSequence( row ) for row in data ):
            numcols = len( data[0] )
            if any( len(row) != numcols for row in data ):
                raise ValueError( "List of lists, all rows must have the same length" )
            if columns is not None:
                if not isSequence( columns ):
                    raise TypeError( "columns must be a list" )
                if len(columns) != numcols:
                    raise ValueError( "columns must have the same length as each row" )
            else:
                columns = [ f"column_{i}" for i in range(numcols) ]

            dtypes = get_dtypes( columns )
            serieses = { c: pandas.Series( ( r[i] for r in data ), dtype=dtypes[c] )
                         for i, c in enumerate(columns) }

    elif isinstance( data, dict ):
        if columns is not None:
            raise ValueError( "columns inconsistent with passing a dictionary" )

        if all( isinstance( row, dict ) for row in data.values() ):
            # This is a key -> { col: list } structure
            if keyname is None:
                raise ValueError( "Need a keyname" )
            columns = list( data[ list(data.keys())[0] ].keys() )
            if any( ( ( c is None ) or ( pandas.isna(c) ) ) for c in columns ):
                raise ValueError( "No dictionary key can be None" )
            if not all( set( row.keys() ) == set( columns ) for row in data.values() ):
                raise ValueError( "Improperly constructed dictionary" )
            if not all( all( isinstance( v, (list, np.array) ) for v in row.values() ) for row in data.values() ):
                raise ValueError( "Improperly constructed dictionary" )

            col0 = columns[0]
            allcolumns = columns.copy()
            allcolumns.insert( 0, keyname )
            dtypes = get_dtypes( allcolumns )
            serieses = { c: pandas.Series( itertools.chain.from_iterable( row[c] for row in data.values() ),
                                                                          dtype=dtypes[c] )
                                           for c in columns }
            serieses[keyname] = pandas.Series( itertools.chain.from_iterable( [k] * len(v[col0])
                                                                              for k, v in data.items() ) )
        else:
            if not all( isSequence( row ) for row in data.values() ):
                raise TypeError( "All dictionary values must be lists" )
            columns = list( data.keys() )
            dtypes = get_dtypes( columns )
            serieses = { c: pandas.Series( v, dtype=dtypes[c] ) for c, v in data.items() }


    df = pandas.DataFrame( serieses )

    if indices is not None:
        if not isSequence( indices ):
            indices = [ indices ]
        else:
            indices = list( indices )
        if keyname is not None:
            indices.insert( 0, keyname )
        df.set_index( indices, inplace=True )

    FDBLogger.debug( "...done laboriously constructing a pandas dataframe." )

    return df


def get_alert_schema( schemadir=None ):

    """Return a dictionary of { name: schema }, plus 'alert_schema_file': Path }"""

    schemadir = pathlib.Path( "/fastdb/share/avsc" if schemadir is None else schemadir )
    if not schemadir.is_dir():
        raise RuntimeError( f"{schemadir} is not an existing directory" )
    diaobject_schema = fastavro.schema.load_schema( schemadir / f"{_lsst_schema_namespace}.diaObject.avsc" )
    diasource_schema = fastavro.schema.load_schema( schemadir / f"{_lsst_schema_namespace}.diaSource.avsc" )
    diaforcedsource_schema = fastavro.schema.load_schema( schemadir /
                                                          f"{_lsst_schema_namespace}.diaForcedSource.avsc" )
    sssource_schema = fastavro.schema.load_schema( schemadir / f"{_lsst_schema_namespace}.ssSource.avsc" )
    mpc_orbits_schema = fastavro.schema.load_schema( schemadir / f"{_lsst_schema_namespace}.mpc_orbits.avsc" )
    named_schemas = { f'{_lsst_schema_namespace}.diaObject': diaobject_schema,
                      f'{_lsst_schema_namespace}.diaSource': diasource_schema,
                      f'{_lsst_schema_namespace}.diaForcedSource': diaforcedsource_schema,
                      f'{_lsst_schema_namespace}.ssSource': sssource_schema,
                      f'{_lsst_schema_namespace}.mpc_orbits': mpc_orbits_schema,
                     }
    alert_schema = fastavro.schema.load_schema( schemadir / f"{_lsst_schema_namespace}.alert.avsc",
                                                named_schemas=named_schemas )
    brokermessage_schema = fastavro.schema.load_schema( schemadir / f"{_fastdb_schema_namespace}.BrokerMessage.avsc",
                                                        named_schemas=named_schemas )

    return { 'alert': fastavro.schema.parse_schema( alert_schema ),
             'diaobject': fastavro.schema.parse_schema( diaobject_schema ),
             'diasource': fastavro.schema.parse_schema( diasource_schema ),
             'diaforcedsource': fastavro.schema.parse_schema( diaforcedsource_schema ),
             'sssource': fastavro.schema.parse_schema( sssource_schema ),
             'mpc_orbits': fastavro.schema.parse_schema( mpc_orbits_schema ),
             'brokermessage': fastavro.schema.parse_schema( brokermessage_schema ),
             'alert_schema_file': schemadir / f"{_lsst_schema_namespace}.alert.avsc",
             'brokermessage_schema_file': schemadir / f"{_fastdb_schema_namespace}.BrokerMessage.avsc"
            }


def procver_id( *args, **kwargs ):
    # import db here to avoid circular imports
    import db
    return db.ProcessingVersion.procver_id( *args, **kwargs )


def base_procver_id( *args, **kwargs ):
    # import db here to avoid circular imports
    import db
    return db.BaseProcessingVersion.base_procver_id( *args, **kwargs )
