import sys
import pathlib
import logging
import uuid

import fastavro

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
