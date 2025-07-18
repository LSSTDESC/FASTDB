from psycopg import sql

from db import DB


def create_diaobject_sources_view(connection, procver):
    """Create a materialized view joining ``diaobject`` and ``diasource``."""

    # cursor.execute(
    #     "DROP MATERIALIZED VIEW IF EXISTS diaobject_with_sources"
    # )
    with connection.cursor() as cursor:
        cursor.execute(
            sql.SQL(
                """
                CREATE TEMPORARY TABLE diaobject_with_sources AS
                    SELECT o.*, ds.diasources
                    FROM diaobject AS o
                    LEFT JOIN (
                        SELECT diaobjectid,
                            array_agg(s ORDER BY s.midpointmjdtai) AS diasources
                        FROM diasource AS s
                        WHERE s.diaobject_procver = {procver} AND s.processing_version = {procver}
                        GROUP BY diaobjectid
                    ) AS ds
                    ON ds.diaobjectid = o.diaobjectid
                    WHERE o.processing_version = {procver}
                """
            ).format(procver=procver)
        )

def dump_to_parquet(filehandler, *, procver, connection=None):
    """Dump joined ``diaobject`` and ``diasource`` rows to a Parquet file."""

    with DB(connection) as conn, conn.cursor() as cursor:
        create_diaobject_sources_view(connection, procver=procver)
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