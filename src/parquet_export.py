from psycopg import sql

from db import DB


def create_diaobject_sources_view(connection, procver):
    """Create a materialized view joining ``diaobject`` and ``diasource``."""

    # We select the same procver for both object and source, but this should be changed when the provinance
    # system matures.
    # We generally want all the columns to be there, and probably join with more tables, but for now we keep
    # it simple. Also, there is a bug in pg_parquet, which prevents us from using UUID columns:
    # https://github.com/CrunchyData/pg_parquet/issues/140
    with connection.cursor() as cursor:
        cursor.execute(
            sql.SQL(
                """
                CREATE TEMPORARY TABLE diaobject_with_sources AS
                    SELECT o.diaobjectid, o.processing_version, o.radecmjdtai, o.validitystart, o.validityend,
                           o.ra, o.raerr, o.dec, o.decerr, o.ra_dec_cov, o.nearbyextobj1, o.nearbyextobj1sep,
                           o.nearbyextobj2, o.nearbyextobj2sep, o.nearbyextobj3, o.nearbyextobj3sep,
                           o.nearbylowzgal, o.nearbylowzgalsep, o.parallax, o.parallaxerr, o.pmra, o.pmraerr,
                           o.pmra_parallax_cov, o.pmdec, o.pmdecerr, o.pmdec_parallax_cov,
                           ds.diasources
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