import db

from psycopg import sql


def create_diaobject_sources_query(connection, procver, diaobjs=None, offset=None, limit=None):
    """Create a temporary view joining objects with sources and forced sources.

    Parameters
    ----------
      connection : DBCon or psycopg.Connection
         Database connection.  Required, since it creates a temporary
         view that will only be visible within this connection.

      procver : str or UUID
         The processing version to search photometry.  NOTE!  If this
         processing version is not consistent with the diaobjects you
         pass, then you won't get any photometry out!

      diaobjs : list of bigint, default None
         If passed, a list of diaobjectIDs to include.  If None, will
         include everything.  Cannot be used together with offset/limit.

      offset : int, default None
         If passed, only stick in lightcurves for objects starting this offset
         from the first.  (Objects are sorted by diaobjectid.)  Use this and
         limit together to divide things into chunks.

      limit : int, default None
         If passed, only stick in lightcurves for this many objects.

    """

    if ( diaobjs is not None ) and ( ( offset is not None ) or ( limit is not None ) ):
        raise ValueError( "Can only specify either diabojs or offset/limit, not both." )

    procver = db.ProcessingVersion.procver_id( procver )
    with db.DBCon( connection ) as dbcon:
        dbcon.alwaysexplain = False

        # When Kostya originally wrote this, this wasn't necessary, but somehow an
        #   additional level of subqueries made postgres start spitting about
        #   not being able to create a column with pseudo-type record[], so
        #   we have to define an explicit record type to cast things to.
        # A side-effect of this is that we have to explicitly list the columns
        #   of diasource and diaforcedsource we want to include, rather than
        #   including them all.  (This may be just as well; there's a lot of
        #   gratuitous extra stuff there.  But, see Issues #51, #62.)

        dbcon.execute( "CREATE TYPE pg_temp.srcrow AS (visit bigint, midpointmjdtai double precision,"
                       "                               band character(1), psfflux real, psffluxerr real)" )
        q = sql.SQL(
            """CREATE TEMPORARY VIEW diaobject_with_sources AS
                   SELECT o.*, ds.diasource, dfs.diaforcedsource
                   FROM diaobject AS o
                   INNER JOIN (
                       SELECT s.diaobjectid,
                         array_agg((s.visit, s.midpointmjdtai, s.band, s.psfflux, s.psffluxerr)::pg_temp.srcrow
                                   ORDER BY s.midpointmjdtai)
                       AS diasource
                       FROM (
                           SELECT DISTINCT ON (subs.diaobjectid,subs.visit) subs.diaobjectid, subs.visit,
                                                                            subs.midpointmjdtai, subs.band,
                                                                            subs.psfflux, subs.psffluxerr
                           FROM diasource AS subs
                           INNER JOIN base_procver_of_procver pv ON pv.base_procver_id=subs.base_procver_id
                                                                AND pv.procver_id={procver}
                           ORDER BY subs.diaobjectid, subs.visit, pv.priority DESC
                       ) s
                       GROUP BY s.diaobjectid
                   ) AS ds
                   ON ds.diaobjectid = o.diaobjectid
                   LEFT JOIN (
                       SELECT s.diaobjectid,
                         array_agg((s.visit, s.midpointmjdtai, s.band, s.psfflux, s.psffluxerr)::pg_temp.srcrow
                                   ORDER BY s.midpointmjdtai)
                       AS diaforcedsource
                       FROM (
                           SELECT DISTINCT ON (subs.diaobjectid,subs.visit) subs.diaobjectid, subs.visit,
                                                                            subs.midpointmjdtai, subs.band,
                                                                            subs.psfflux, subs.psffluxerr
                           FROM diaforcedsource AS subs
                           INNER JOIN base_procver_of_procver pv ON pv.base_procver_id=subs.base_procver_id
                                                                AND pv.procver_id={procver}
                           ORDER BY subs.diaobjectid, subs.visit, pv.priority DESC
                       ) s
                       GROUP BY s.diaobjectid
                   ) AS dfs
                   ON dfs.diaobjectid = o.diaobjectid
            """
        ).format( procver=procver )

        if diaobjs is not None:
            q += sql.SQL( "WHERE diaobjectid=ANY({obj})\n" ).format( obj=diaobjs )
        q += sql.SQL( "ORDER BY diaobjectid\n" )
        if offset is not None:
            q += sql.SQL( "OFFSET {offset}\n" ).format( offset=offset )
        if limit is not None:
            q += sql.SQL( "LIMIT {limit}\n" ).format( limit=limit )

        dbcon.execute( q )


def dump_to_parquet(filehandler, procver, diaobjs=None, offset=None, limit=None, connection=None):
    """Dump joined ``diaobject`, ``diasource``, and ``diaforcedsource`` rows to a Parquet file.

    TODO : limit the number of objects included.  Right now this could
    create a truly gigantic parquet file!

    """

    with db.DBCon(connection) as conn:
        # Previously, we made a temporary view and then extracted that.
        # However, adding one more subtable, for reasons I don't understand,
        # made postgres yell about how column "diasource" had a pseudo-type record[].
        # We do seem to be able to just run the query even without the
        # CREATE TEMPORARY VIEW, so we just do that here.
        # create_diaobject_sources_view(conn, procver=procver)

        create_diaobject_sources_query(conn, procver=procver, diaobjs=diaobjs, offset=offset, limit=limit)

        conn.execute("DROP EXTENSION IF EXISTS pg_parquet; CREATE EXTENSION pg_parquet;", explain=False )
        with conn.cursor.copy(
            """
            COPY (SELECT * FROM diaobject_with_sources)
                TO STDOUT
                WITH (format 'parquet', compression 'zstd')
            """
        ) as data:
            for chunk in data:
                filehandler.write(chunk)

        conn.commit()
