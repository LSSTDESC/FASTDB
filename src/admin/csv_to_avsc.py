import io
import argparse
import pandas
import csv


def main():
    parser = argparse.ArgumentParser( 'csv_to_avsc',
                                      description='Convert an APDB CSV file to a prototype .avsc file' )
    parser.add_argument( 'csvfile', help="Name of the CSV file" )
    parser.add_argument( '--namespace', required=True, help="Namespace to put the schema in" )
    parser.add_argument( '-n', '--name', required=True, help="Name of this schema" )
    parser.add_argument( '--no-null', nargs='*', default=[], help="Columns that aren't nullable" )
    args = parser.parse_args()

    outio = io.StringIO()
    outio.write( f'{{ "type": "record",\n'
                 f'  "namespace": "{args.namespace}",\n'
                 f'  "name": "{args.name}",\n'
                 f'  "fields": [\n' )

    schema = pandas.read_csv( args.csvfile, quoting=csv.QUOTE_ALL )
    # pandas spazzes out with iterrows and ittertuples when
    #  column names have a space in them, so remove the spaces.
    schema.rename( { 'Data Type': 'Data_Type', 'Column Name': 'Column_Name' }, axis=1, inplace=True )

    typeconv = {
        'boolean': 'boolean',
        'short': 'int',         # AVRO doesn't seem to have a 16-bit integer type
        'int': 'int',
        'long': 'long',
        'float': 'float',
        'double': 'double',
        'char': 'string',
        'string': 'string',
        'timestamp': 'long',
    }

    logicaltype = {
        'timestamp': 'timestamp-millis'
    }

    first = True
    for row in schema.itertuples():
        if row.Data_Type not in typeconv:
            raise ValueError( f"Unknown type {row.Data_Type}" )
        typestr = typeconv[row.Data_Type]
        docstr = row.Description
        if ( isinstance( row.Unit, str) ) and ( len( row.Unit ) > 0 ):
            docstr = docstr + f" ({row.Unit})"
        if first:
            first = False
        else:
            outio.write( ',\n' )
        outio.write( f'    {{ "name": "{row.Column_Name}",\n' )
        if row.Column_Name in args.no_null:
            outio.write( f'      "type": "{typestr}",\n' )
        else:
            outio.write( f'      "type": [ "null", "{typestr}" ],\n' )
            outio.write(  '      "default": null,\n' )
        if row.Data_Type in logicaltype:
            outio.write( f'      "logicalType": "{logicaltype[row.Data_Type]}",\n' )
        outio.write( f'      "doc": "{docstr}"\n' )
        outio.write(  '    }' )

    outio.write( '\n  ]\n}\n' )

    print( outio.getvalue() )


# ======================================================================
if __name__ == "__main__":
    main()
