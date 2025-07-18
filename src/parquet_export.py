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

def create_diaobject_sources_view(cursor):
    """Create a materialized view joining ``diaobject`` and ``diasource``."""

    cursor.execute(
        "DROP MATERIALIZED VIEW IF EXISTS diaobject_with_sources"
    )
    cursor.execute(
        sql.SQL(
            """
            CREATE MATERIALIZED VIEW diaobject_with_sources AS
                SELECT o.*, ds.diasources
                FROM diaobject o
                LEFT JOIN (
                    SELECT diaobjectid, diaobject_procver,
                        array_agg(s ORDER BY s.midpointmjdtai) AS diasources
                    FROM diasource s
                    GROUP BY diaobjectid, diaobject_procver
                ) ds
                ON ds.diaobjectid = o.diaobjectid
               AND ds.diaobject_procver = o.processing_version
            """
        )
    )

def dump_objects_with_sources(filehandler, connection=None):
    """Dump joined ``diaobject`` and ``diasource`` rows to a Parquet file."""

    with DB(connection) as conn, conn.cursor() as cursor:
        create_diaobject_sources_view(cursor)
        cursor.execute(
            """
            DROP EXTENSION IF EXISTS pg_parquet;
            CREATE EXTENSION pg_parquet;
            """
        )        
        with cursor.copy(
            "COPY diaobject_with_sources TO STDOUT WITH (format 'parquet', compression 'zstd')"
        ) as data:
            for chunk in data:
                filehandler.write(chunk)

        conn.commit()