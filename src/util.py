__all__ = [ "FDBLogger", "parse_bool", "env_as_bool", "asUUID", "isSequence",
            "float_or_none_from_dict", "int_or_none_from_dict",
            "datetime_or_none_from_dict_mjd_or_timestring", "mjd_or_none_from_dict_mjd_or_timestring",
            "parse_sexigesimal", "float_or_none_from_dict_float_or_dms", "float_or_none_from_dict_float_or_hms",
             "mjd_from_mjd_or_datetime_or_timestring", "get_alert_schema", "procver_id" ]

import sys
import os
import re
import datetime
import pathlib
import logging
import numbers
import uuid
import collections.abc
import multiprocessing

import fastavro
import astropy.time
import rkwebutil

import db

_fastdb_schema_namespace = 'fastdb_9_0_2'
_lsst_schema_namespace = 'lsst.v9_0'

_default_datefmt = '%Y-%m-%d %H:%M:%S'
_default_log_level = logging.DEBUG
# _default_log_level = logging.INFO
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


def get_alert_schema( schemadir=None ):
    """Return a dictionary of { name: schema }, plus 'alert_schema_file': Path }"""

    schemadir = pathlib.Path( "/fastdb/share/avsc" if schemadir is None else schemadir )
    if not schemadir.is_dir():
        raise RuntimeError( f"{schemadir} is not an existing directory" )
    diaobject_schema = fastavro.schema.load_schema( schemadir / f"{_lsst_schema_namespace}.diaObject.avsc" )
    diasource_schema = fastavro.schema.load_schema( schemadir / f"{_lsst_schema_namespace}.diaSource.avsc" )
    diaforcedsource_schema = fastavro.schema.load_schema( schemadir /
                                                          f"{_lsst_schema_namespace}.diaForcedSource.avsc" )
    MPCORB_schema = fastavro.schema.load_schema( schemadir / f"{_lsst_schema_namespace}.MPCORB.avsc" )
    sssource_schema = fastavro.schema.load_schema( schemadir / f"{_lsst_schema_namespace}.ssSource.avsc" )
    named_schemas = { f'{_lsst_schema_namespace}.diaObject': diaobject_schema,
                      f'{_lsst_schema_namespace}.diaSource': diasource_schema,
                      f'{_lsst_schema_namespace}.diaForcedSource': diaforcedsource_schema,
                      f'{_lsst_schema_namespace}.MPCORB': MPCORB_schema,
                      f'{_lsst_schema_namespace}.ssSource': sssource_schema
                     }
    alert_schema = fastavro.schema.load_schema( schemadir / f"{_lsst_schema_namespace}.alert.avsc",
                                                named_schemas=named_schemas )
    brokermessage_schema = fastavro.schema.load_schema( schemadir / f"{_fastdb_schema_namespace}.BrokerMessage.avsc",
                                                        named_schemas=named_schemas )

    return { 'alert': fastavro.schema.parse_schema( alert_schema ),
             'diaobject': fastavro.schema.parse_schema( diaobject_schema ),
             'diasource': fastavro.schema.parse_schema( diasource_schema ),
             'diaforcedsource': fastavro.schema.parse_schema( diaforcedsource_schema ),
             'MPCORB': fastavro.schema.parse_schema( MPCORB_schema ),
             'sssource': fastavro.schema.parse_schema( sssource_schema ),
             'brokermessage': fastavro.schema.parse_schema( brokermessage_schema ),
             'alert_schema_file': schemadir / f"{_lsst_schema_namespace}.alert.avsc",
             'brokermessage_schema_file': schemadir / f"{_fastdb_schema_namespace}.BrokerMessage.avsc"
            }


def procver_id( *args, **kwargs ):
    return db.ProcessingVersion.procver_id( *args, **kwargs )


def base_procver_id( *args, **kwargs ):
    return db.BaseProcessingVersion.base_procver_id( *args, **kwargs )
