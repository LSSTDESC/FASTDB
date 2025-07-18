from psycopg import sql

from db import DB


def dump_to_parquet(filehandler, connection=None):
    with DB(connection) as conn, conn.cursor() as cursor:
        # Looks like a bug in pg_parquet
        # https://github.com/CrunchyData/pg_parquet/issues/68
        cursor.execute("""
            DROP EXTENSION pg_parquet;
            CREATE EXTENSION pg_parquet;
        """)
        with cursor.copy(
            "COPY diasource TO STDOUT WITH (format 'parquet', compression 'zstd')"
        ) as data:
            for chunk in data:
                filehandler.write(chunk)

        conn.commit()