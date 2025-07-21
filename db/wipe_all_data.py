# TODO : wrap this with --do and --really-do options
# TODO : add ability to also wipe users
# TODO : add ability to also wipe migrations

import sys
import db

# By default, we don't want to drop users or migrations
tablenames = db.all_table_names.copy()
tablenames.remove( "authuser" )
tablenames.remove( "migrations_applied" )

with db.DB() as conn:
    try:
        cursor = conn.cursor()
        for table in tablenames:
            sys.stderr.write( f"Truncating table {table}...\n" )
            cursor.execute( f"TRUNCATE TABLE {table} CASCADE" )
        conn.commit()
    finally:
        conn.rollback()
