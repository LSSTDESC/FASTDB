import sys
import io
import argparse
import pathlib
import hashlib
import uuid
import subprocess

import psycopg


def main():
    parser = argparse.ArgumentParser( 'apply_migrations', description='Apply .sql migration files' )
    parser.add_argument( '-H', '--host', default='postgres', help="Postgres host" )
    parser.add_argument( '-P', '--port', type=int, default=5432, help="Postgres port" )
    parser.add_argument( '-d', '--db', default="fastdb", help="Postgres database" )
    parser.add_argument( '-u', '--user', default="postgres", help="Postgres user" )
    parser.add_argument( '-p', '--password', default="fragile", help="Postgres password" )
    args = parser.parse_args()

    direc = pathlib.Path( __file__ ).parent
    sqlfiles = list( direc.glob( "*.sql" ) )
    sqlfiles.sort()

    with psycopg.connect( host=args.host, port=args.port, dbname=args.db,
                           user=args.user, password=args.password ) as conn:
        cursor = conn.cursor()
        try:
            cursor.execute( "SELECT filename,md5sum,applied_time "
                            "FROM migrations_applied "
                            "ORDER BY filename" )
            rows = cursor.fetchall()
            applied = [ row[0] for row in rows ]
            md5sums = [ row[1] for row in rows ]
            when = [ row[2] for row in rows ]
        except psycopg.errors.UndefinedTable:
            conn.rollback()
            cursor.execute( "CREATE TABLE migrations_applied( "
                            "  filename text,"
                            "  applied_time timestamp with time zone DEFAULT NOW(),"
                            "  md5sum UUID"
                            ")" )
            conn.commit()
            applied = []
            md5sums = []
            when = []

        strio = io.StringIO()
        strio.write( "Previously applied:\n" )
        for a, w in zip( applied, when ):
            strio.write( f"   {a:48s}  ({w})\n" )
        sys.stderr.write( strio.getvalue() )

        for i, a in enumerate( applied ):
            if sqlfiles[i].name != a:
                raise ValueError( f"Mismatch between applied and files at file {sqlfiles[i].name}, "
                                  f"applied logged {a}" )
            filemd5 = hashlib.md5()
            with open( sqlfiles[i], "rb" ) as ifp:
                filemd5.update( ifp.read() )
            if uuid.UUID( filemd5.hexdigest() ) != md5sums[i]:
                raise ValueError( f"Contents of migration file {a} md5sum does not match "
                                  f"what was previously applied" )

        for i in range( len(applied), len(sqlfiles) ):
            sys.stderr.write( f"Applying {sqlfiles[i]}...\n" )
            rval = subprocess.run( [ "psql", "-h", args.host, "-p", str(args.port), "-U", args.user,
                                     "-v", "ON_ERROR_STOP=on",
                                     "-f", sqlfiles[i],
                                     "--single-transaction", "-b",
                                     args.db ],
                                   env={ 'PGPASSWORD': args.password },
                                   capture_output=True )
            if rval.returncode != 0:
                sys.stderr.write( f"Error processing {sqlfiles[i]}:\n{rval.stderr.decode('utf-8')}\n" )
                raise RuntimeError( "SQL error" )
            filemd5 = hashlib.md5()
            with open( sqlfiles[i], "rb" ) as ifp:
                filemd5.update( ifp.read() )
            md5sum = filemd5.hexdigest()

            cursor = conn.cursor()
            cursor.execute( "INSERT INTO migrations_applied(filename,md5sum) "
                            "VALUES(%(fn)s,%(md5)s)",
                            { 'fn': sqlfiles[i].name, 'md5': md5sum } )
            conn.commit()


# ======================================================================
if __name__ == "__main__":
    main()
