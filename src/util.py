import pathlib
import uuid

import fastavro


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


_schema_namespace = 'fastdb_test_0.1'


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

    return { 'alert': alert_schema,
             'diaobject': diaobject_schema,
             'diasource': diasource_schema,
             'diaforcedsource': diaforcedsource_schema,
             'alert_schema_file': schemadir / f"{_schema_namespace}.Alert.avsc" }
