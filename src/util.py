import sys
import re
import pathlib
import logging
import numbers
import uuid

import fastavro
import astropy.time
import rkwebutil

_schema_namespace = 'fastdb_test_0.1'

logger = logging.getLogger( "FASTDB logger" )
_logout = logging.StreamHandler( sys.stderr )
logger.addHandler( _logout )
_formatter = logging.Formatter( '[%(asctime)s - %(levelname)s] - %(message)s',
                                datefmt='%Y-%m-%d %H:%M:%S' )
_logout.setFormatter( _formatter )
logger.propagate = False
# logger.setLevel( logging.INFO )
logger.setLevel( logging.DEBUG )


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
        return astropy.time.Time( mjd, format='mjd' ).to_datetime()


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


def get_alert_schema( schemadir=None ):
    """Return a dictionary of { name: schema }, plus 'alert_schema_file': Path }"""

    schemadir = pathlib.Path( "/fastdb/share/avsc" if schemadir is None else schemadir )
    if not schemadir.is_dir():
        raise RuntimeError( f"{schemadir} is not an existing directory" )
    diaobject_schema = fastavro.schema.load_schema( schemadir / f"{_schema_namespace}.DiaObject.avsc" )
    diasource_schema = fastavro.schema.load_schema( schemadir / f"{_schema_namespace}.DiaSource.avsc" )
    diaforcedsource_schema = fastavro.schema.load_schema( schemadir / f"{_schema_namespace}.DiaForcedSource.avsc" )
    named_schemas = { 'fastdb_test_0.1.DiaObject': diaobject_schema,
                      'fastdb_test_0.1.DiaSource': diasource_schema,
                      'fastdb_test_0.1.DiaForcedSource': diaforcedsource_schema }
    alert_schema = fastavro.schema.load_schema( schemadir / f"{_schema_namespace}.Alert.avsc",
                                                named_schemas=named_schemas )
    brokermessage_schema = fastavro.schema.load_schema( schemadir / f"{_schema_namespace}.BrokerMessage.avsc",
                                                        named_schemas=named_schemas )

    return { 'alert': fastavro.schema.parse_schema( alert_schema ),
             'diaobject': fastavro.schema.parse_schema( diaobject_schema ),
             'diasource': fastavro.schema.parse_schema( diasource_schema ),
             'diaforcedsource': fastavro.schema.parse_schema( diaforcedsource_schema ),
             'brokermessage': fastavro.schema.parse_schema( brokermessage_schema ),
             'alert_schema_file': schemadir / f"{_schema_namespace}.Alert.avsc",
             'brokermessage_schema_file': schemadir / f"{_schema_namespace}.BrokerMessage.avsc"
            }
