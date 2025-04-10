# TODO : wrap this with --do and --really-do options

# Empty out all data tables, don't remove users
data_tables = [ 'processing_version', 'snapshot',
                'host_galaxy', 'root_diaobject', 'diaobject', 'diasource', 'diaforcedsource',
                'diaobject_root_map', 'diaobject_snapshot', 'diasource_snapshot', 'diaforcedsource_snapshot',
                'query_queue' ]

import sys
import psycopg
import config

with open ( config.dbpasswdfile ) as ifp:
    dbpasswd = ifp.read().strip()

conn = psycopg.connect( host=config.dbhost, port=config.dbport, dbname=config.dbdatabase,
                        user=config.dbuser, password=dbpasswd )
try:
    cursor = conn.cursor()
    for table in data_tables:
        sys.stderr.write( f"Truncating table {table}...\n" )
        cursor.execute( f"TRUNCATE TABLE {table} CASCADE" )
    conn.commit()
finally:
    conn.rollback()
    conn.close()
