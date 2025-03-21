import sys
import pathlib
import logging

import fastavro

import db

_logger = logging.getLogger( __file__ )
_logout = logging.StreamHandler( sys.stderr )
_logger.addHandler( _logout )
_formatter = logging.Formatter( '[%(asctime)s - %(levelname)s] - %(message)s',
                                datefmt='%Y-%m-%d %H:%M:%S' )
_logout.setFormatter( _formatter )
_logger.propagate = False
# _logger.setLevel( logging.INFO )
_logger.setLevel( logging.DEBUG )


class AlertReconstructor:
    """A class that constructs alerts (in dict format) from ppdb."""

    def __init__( self, prevsrc=365, prevfrced=365, prevfrced_gap=1, schemadir=None ):
        """Constructor.

        Parameters
        ----------
          prevsrc: float
            Include previous diaSources that go back at most this many days.

          prevfrced: float
            Include previous diaForcedSources that go back at most this many days.

          prevfrced_gap: float
            Only include previous diaForcedSources that are at least this many days ago.

          schemadir : str or Path
            Directory with Alert.avsc, DiaObject.avsc, DiaSource.avsc, and DiaForcedSource.avsc

        """

        self.prevsrc = prevsrc
        self.prevfrced = prevfrced
        self.prevfrced_gap = prevfrced_gap

        schemadir = pathlib.Path( "/fastdb/share/avsc" if schemadir is None else schemadir )
        if not schemadir.is_dir():
            raise RuntimeError( f"{schemadir} is not an existing directory" )
        self.diaobject_schema = fastavro.schema.load_schema( schemadir / "DiaObject.avsc" )
        self.diasource_schema = fastavro.schema.load_schema( schemadir / "DiaSource.avsc" )
        self.diaforcedsource_schema = fastavro.schema.load_schema( schemadir / "DiaForcedSource.avsc" )
        named_schemas = { 'fastdb_test_0.1.DiaObject': self.diaobject_schema,
                          'fastdb_test_0.1.DiaSource': self.diasource_schema,
                          'fastdb_test_0.1.DiaForcedSource': self.diaforcedsource_schema }
        self.alert_schema = fastavro.schema.load_schema( schemadir / "Alert.avsc", named_schemas=named_schemas )


    def object_data_to_dicts( self, rows, columns ):
        allfields = [ f['name'] for f in self.diaobject_schema['fields'] ]
        lcfields = { 'diaObjectId', 'raDecMjdTai', 'ra', 'raErr', 'dec', 'decErr', 'ra_dec_Cov',
                     'nearbyExtObj1', 'nearbyExtObj1Sep', 'nearbyExtObj2', 'nearbyExtObj2Sep',
                     'nearbyExtObj3', 'nearbyExtObj3Sep', 'nearbyLowzGal', 'nearbyLowzGalSep',
                     'parallax', 'parallaxErr', 'pmRa', 'pmRaErr', 'pmRa_parallax_Cov',
                     'pmDec', 'pmDecErr', 'pmDec_parallax_Cov', 'pmRa_pmDec_Cov'
                    }
        timefields = { 'validityStart': 'validitystart',
                       'validityEnd': 'validityend' }

        dicts = []
        for row in rows:
            curdict = {}
            for col in allfields:
                if col in lcfields:
                    curdict[col] = row[ columns[ col.lower()] ]
                elif col in timefields:
                    val = row[ columns[ timefields[col] ] ]
                    curdict[col] = None if val is None else int( val.timestamp() * 1000 + 0.5 )
                else:
                    curdict[col] = None
            dicts.append( curdict )

        return dicts


    def source_data_to_dicts( self, rows, columns ):
        allfields = [ f['name'] for f in self.diasource_schema['fields'] ]
        lcfields = { 'diaSourceId', 'diaObjectId', 'ssObjectId', 'visit', 'detector',
                     'x', 'y', 'xErr', 'yErr', 'x_y_Cov',
                     'band', 'midpointMjdTai', 'ra', 'raErr', 'dec', 'decErr', 'ra_dec_Cov',
                     'psfFlux', 'psfFluxErr', 'psfRa', 'psfRaErr', 'psfDec', 'psfDecErr', 'psfRa_psfDec_Cov',
                     'psfFlux_psfRa_cov', 'psfFlux_psfDec_cov', 'psfLnL', 'psfChi2', 'psfNdata', 'snr',
                     'scienceFlux', 'scienceFluxErr', 'fpBkgd', 'fpBkdgErr',
                     'parentDiaSourceId', 'extendedness', 'reliability',
                     'ixx', 'ixxErr', 'iyy', 'iyyErr', 'ixy', 'ixx_ixy_Cov', 'ixx_iyy_Cov', 'iyy_ixy_Cov',
                     'ixxPSF', 'iyyPSF', 'ixyPSF' }

        # TODO : flags, pixelflags

        dicts = []
        for row in rows:
            curdict = {}
            for col in allfields:
                if col in lcfields:
                    curdict[col] = row[ columns[col.lower()] ]
                else:
                    curdict[col] = None
            dicts.append( curdict )

        return dicts


    def forced_source_data_to_dicts( self, rows, columns ):
        allfields = [ f['name'] for f in self.diaforcedsource_schema['fields'] ]
        lcfields = [ 'diaForcedSourceId', 'diaObjectId', 'visit', 'detector', 'midpointMjdTai',
                     'band', 'ra', 'dec', 'psfFlux', 'psfFluxErr', 'scienceFlux', 'sciencefluxErr', ]
        timefields = { 'time_processed': 'time_processed',
                       'time_withdrawn': 'time_withdrawn' }

        dicts = []
        for row in rows:
            curdict = {}
            for col in allfields:
                if col in lcfields:
                    curdict[col] = row[ columns[col.lower()] ]
                elif col in timefields:
                    val = row[ columns[ timefields[col] ] ]
                    curdict[col] = None if val is None else int( val.timestamp() * 1000 + 0.5 )
                else:
                    curdict[col] = None
            dicts.append( curdict )

        return dicts


    def previous_sources( self, diasource, con=None ):
        with db.DB( con ) as con:
            cursor = con.cursor()
            q = ( "SELECT * FROM ppdb_diasource WHERE diaobjectid=%(objid)s "
                  "AND midpointmjdtai>=%(minmjd)s AND midpointmjdtai<%(maxmjd)s "
                  "AND diasourceid!=%(srcid)s ORDER BY midpointmjdtai" )
            cursor.execute( q, { 'objid': diasource['diaObjectId'],
                                 'srcid': diasource['diaSourceId'],
                                 'minmjd': diasource['midpointMjdTai'] - self.prevsrc,
                                 'maxmjd': diasource['midpointMjdTai'] } )
            columns = { col_desc[0]: i for i, col_desc in enumerate(cursor.description) }
            rows = cursor.fetchall()

        return self.source_data_to_dicts( rows, columns )


    def previous_forced_sources( self, diasource, con=None ):
        with db.DB( con ) as con:
            cursor = con.cursor()
            q = ( "SELECT * FROM ppdb_diaforcedsource WHERE diaobjectid=%(objid)s "
                  "AND midpointmjdtai>%(minmjd)s AND midpointmjdtai<%(maxmjd)s "
                  "ORDER BY midpointmjdtai" )
            cursor.execute( q, { 'objid': diasource['diaObjectId'],
                                 'minmjd': diasource['midpointMjdTai'] - self.prevfrced,
                                 'maxmjd': diasource['midpointMjdTai'] - self.prevfrced_gap } )
            columns = { col_desc[0]: i for i, col_desc in enumerate(cursor.description) }
            rows = cursor.fetchall()

        return self.forced_source_data_to_dicts( rows, columns )


    def reconstruct( self, diasourceid, con=None ):
        with db.DB( con ) as con:
            cursor = con.cursor()
            cursor.execute( "SELECT * FROM ppdb_diasource WHERE diasourceid=%(id)s", { 'id': diasourceid } )
            columns = { col_desc[0]: i for i, col_desc in enumerate(cursor.description) }
            rows = cursor.fetchall()
            if len(rows) == 0:
                raise ValueError( f"Unknown diasource {diasourceid}" )
            if len(rows) > 1:
                raise RuntimeError( f"diasource {diasourceid} is multiply defined, I don't know how to cope." )
            diasource = self.source_data_to_dicts( rows, columns )[0]
            previous_sources = self.previous_sources( diasource, con=con )
            previous_forced_sources = self.previous_forced_sources( diasource, con=con )
            cursor.execute( "SELECT * FROM ppdb_diaobject WHERE diaobjectid=%(id)s",
                            { 'id': diasource['diaObjectId'] } )
            columns = { col_desc[0]: i for i, col_desc in enumerate(cursor.description) }
            rows = cursor.fetchall()
            if len(rows) == 0:
                raise ValueError( f"Unknown diaobject {diasource['diaObjectId']} for source {diasourceid}" )
            if len(rows) > 1:
                raise RuntimeError( f"diaobject {diasource['diaObjectId']} is multiply defined, I can't cope." )
            diaobject = self.object_data_to_dicts( rows, columns )[0]

            alert = { "alertId": diasourceid,
                      "diaSource": diasource,
                      "prvDiaSources": previous_sources if len(previous_sources) > 0 else None,
                      "prvDiaForcedSources": previous_forced_sources if len(previous_forced_sources) > 0 else None,
                      "diaObject": diaobject }

            return alert


class AlertSender:
    def __init__( self, kafka_server, reconstruct_procs=5, actually_stream=True ):
        self.kafka_sever = None
        self.reconstruct_procs = reconstruct_procs
        self.actually_stream = actually_stream


    def find_alerts_to_send( self, addeddays=1 ):
        """Gets a list of diasources to send alerts for.

        First finds the source with the latest midpointmjdtai for which
        an alert has already been sent.  Then finds all sources from
        whom an alert has not been sent and whose midpointmjdtai is less
        than the latest midpointmjdtai plus added days.  Returns a list
        of diasourceid.

        """

        with db.DB() as con:
            cursor = con.cursor()
            cursor.execute( "SELECT MAX(s.midpointmjdtai) "
                            "FROM ppdb_alerts_sent a "
                            "INNER JOIN ppdb_diasource s ON a.diasourceid=s.diasourceid" )
            row = cursor.fetchone()
            if row[0] is None:
                cursor.execute( "SELECT MIN(s.midpointmjdtai) FROM ppdb_diasource" )
                row = cursor.fetchone()
                if row[0] is None:
                    raise RuntimeError( "There are no sources in ppdb_diasource" )
            maxalertmjd = row[0]

            cursor.execute( "SELECT s.diasourceid "
                            "FROM ppdb_diasource s "
                            "LEFT JOIN ppdb_alerts_sent a ON a.diasourceid=s.diasourceid"
                            "WHERE a.id IS NULL "
                            "AND s.midpointmjdtai<%(maxmjd)s "
                            "ORDER BY s.midpointmjdtai ",
                            { "maxmjd": maxalertmjd + addeddays } )
            diasourceids = [ row[0] for row in cursor.fetchall() ]

            return diasourceids
