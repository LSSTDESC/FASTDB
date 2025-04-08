import sys
import io
import logging
import datetime
import time
import multiprocessing
# import signal

import confluent_kafka
import fastavro

import db
import util

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

        schema = util.get_alert_schema( schemadir=schemadir )
        self.alert_schema = schema[ 'alert' ]
        self.diaobject_schema = schema[ 'diaobject' ]
        self.diasource_schema = schema[ 'diasource' ]
        self.diaforcedsource_schema = schema[ 'diaforcedsource' ]


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


    def __call__( self, pipe ):
        """Listen for requests on pipe reconstruct alerts.  Reconstruct, send info back through pipe."""

        # Make our own logger so it can have the PID in the header
        pid = multiprocessing.current_process().pid
        logger = logging.getLogger( f"__file__-{pid}" )
        logout = logging.StreamHandler( sys.stderr )
        logger.addHandler( logout )
        formatter = logging.Formatter( f'[%(asctime)s - {pid} - %(levelname)s] - %(message)s',
                                       datefmt='%Y-%m-%d %H:%M:%S' )
        logout.setFormatter( formatter )
        logger.propagate = False
        # logger.setLevel( logging.INFO )
        logger.setLevel( logging.DEBUG )

        logger.info( "Subprocess starting." )
        overall_t0 = time.perf_counter()

        reconstructtime = 0
        avrowritetime = 0
        commtime = 0
        tottime = 0

        done = False
        while not done:
            try:
                msg = pipe.recv()
                if msg['command'] == 'die':
                    logger.info( "Subprocess got die command." )
                    done = True

                elif msg['command'] == 'do':
                    t0 = time.perf_counter()
                    sourceiddex = msg['sourceiddex']
                    sourceid = msg['sourceid']

                    alert = self.reconstruct( sourceid )

                    t1 = time.perf_counter()
                    msgio = io.BytesIO()
                    fastavro.write.schemaless_writer( msgio, self.alert_schema, alert )

                    t2 = time.perf_counter()
                    pipe.send( { 'response': 'alert produced',
                                 'sourceid': alert['diaSource']['diaSourceId'],
                                 'sourceiddex': sourceiddex,
                                 'alert': msgio.getvalue() } )

                    t3 = time.perf_counter()
                    reconstructtime += t1 - t0
                    avrowritetime += t2 - t1
                    commtime += t3 - t2
                    tottime += t3 - t0

                else:
                    raise ValueError( f"Unknown command {msg['command']}" )
            except Exception as ex:
                # Should I be sending an error message back to the parent process instead of just raising?
                raise ex

        logger.info( "Subprocess sending finished message" )
        pipe.send( { 'response': 'finished',
                     'runtime': time.perf_counter() - overall_t0,
                     'tottime': tottime,
                     'reconstructtime': reconstructtime,
                     'avrowritetime': avrowritetime,
                     'commtime': commtime } )



class AlertSender:
    def __init__( self, kafka_server, kafka_topic, reconstruct_procs=5, actually_stream=True ):
        self.kafka_server = kafka_server
        self.kafka_topic = kafka_topic
        self.reconstruct_procs = int( reconstruct_procs )
        self.actually_stream = bool( actually_stream )


    def interruptor( self, signum, frame ):
        _logger.error( "Got an interupt signal, cleaning up and exiting." )
        self.cleanup()

    def cleanup( self ):
        # Send a terminate message to processes that haven't died every 0.25 seconds
        #   for 5 seconds.  Then go nuclear on anything left.
        t0 = time.perf_counter()
        _logger.debug( "In cleanup." )
        # I'm sometimes getting errors "can only test a subprocess" here, but these *are* subprocesses... ??
        while still_alive := [ p for p in self.procinfo.values() if p['proc'].is_alive() ]:
            dt = time.perf_counter() - t0
            _logger.debug( f"{len(still_alive)} still alive after {dt:.2f} seconds" )
            if dt < 5:
                for proc in still_alive:
                    proc['proc'].terminate()
            else:
                for proc in still_alive:
                    proc['proc'].kill()
            time.sleep( 0.25 )
        _logger.debug( "Done with cleanup" )

    def find_alerts_to_send( self, addeddays=1, throughday=None ):
        """Gets a list of diasources to send alerts for.

        First, figures out the mjd of the last alert to send.  If
        throughday is not None, then that is it.  If throughday is None,
        finds the source with the latest midpointmjdtai for which an
        alert has already been sent.  That latest midpointmjdtai plus
        added days is the mjd of the last alert to send.

        Second, finds all sources for which an alert has not been sent
        whose midpointmjdtai is less than or equal to the latest mjd of
        alerts to send.

        Returns a list of diasource id.

        Parameters
        ----------
          addeddays : float, default 1
            Ignored if throughday is not None.  Return source ids of
            as-yet-unsent alerts whose midpointmjdtai is less than the
            latest alert already sent plus this value.

          throughday : float or None, default None
            The MJD of the last alert to send.  Return the source ids
            for which no alret has been sent whose midpointmjdtai is ≤
            than this value.

        Returns
        -------
          list of int

        """

        with db.DB() as con:
            cursor = con.cursor()

            if throughday is None:
                cursor.execute( "SELECT MAX(s.midpointmjdtai) "
                                "FROM ppdb_alerts_sent a "
                                "INNER JOIN ppdb_diasource s ON a.diasourceid=s.diasourceid" )
                row = cursor.fetchone()
                if row[0] is None:
                    cursor.execute( "SELECT MIN(midpointmjdtai) FROM ppdb_diasource" )
                    row = cursor.fetchone()
                    if row[0] is None:
                        raise RuntimeError( "There are no sources in ppdb_diasource" )
                throughday = row[0] + addeddays

            cursor.execute( "SELECT s.diasourceid, s.midpointmjdtai "
                            "FROM ppdb_diasource s "
                            "LEFT JOIN ppdb_alerts_sent a ON a.diasourceid=s.diasourceid "
                            "WHERE a.id IS NULL "
                            "AND s.midpointmjdtai<%(maxmjd)s "
                            "ORDER BY s.midpointmjdtai ",
                            { "maxmjd": throughday } )
            rows = cursor.fetchall()
            diasourceids = [ row[0] for row in rows ]

            return diasourceids


    def update_alertssent( self, sourceids ):
        now = datetime.datetime.now( tz=datetime.UTC ) # .isoformat()
        with db.DB() as con:
            cursor = con.cursor()
            with cursor.copy( "COPY ppdb_alerts_sent(diasourceid,senttime) FROM stdin" ) as curcopy:
                for sourceid in sourceids:
                    curcopy.write_row( ( sourceid, now ) )
            con.commit()


    def __call__( self, addeddays=1, throughday=None, reallysend=False, flush_every=1000, log_every=10000 ):
        """Send alerts.

        Launches AlertReconstructor subprocesses to build the alert
        dictionaries, and sends those alerts to the kafka server and
        topic configured at object instantiation.

        Properties
        ----------
          addeddays, throughday : see find_alerts_to_send

          reallysend : bool, default False
             If False (default), won't actually send alerts, will just
             reconstruct them (for testing purposese).  If True, sends
             alerts.


          flush_every : int, default 1000
             Flush alerts from the producer once this many alerts have
             been produced.  (Will also do a flush at the end to catch
             the remainder.)

          log_every : int, default 10000
             Write a log message at this interval of alerts submitted to
             subprocesses for reconstruction.

        """

        self.procinfo = {}
        try:
            # TODO : tag a runningfile so that two jobs don't run at once

            # Get alerts to send
            _logger.info( "Figuring out which source to send alerts for..." )
            sourceids = self.find_alerts_to_send( addeddays=addeddays, throughday=throughday )
            _logger.info( f"...got {len(sourceids)} diasource ids to send alerts for." )

            if len( sourceids ) == 0:
                _logger.info( "No alerts to send, returning." )
                return

            if reallysend:
                _logger.info( f"Creating kafka producer, will send to topic {self.kafka_topic}" )
                producer = confluent_kafka.Producer( { 'bootstrap.servers': self.kafka_server,
                                                       'batch.size': 131072,
                                                       'linger.ms': 50 } )

            totflushed = 0
            nextlog = 0
            _tottime = 0
            _commtime = 0
            _flushtime = 0
            _updatealertsenttime = 0
            _producetime = 0
            overall_t0 = time.perf_counter()

            def launch_reconstructor( pipe ):
                reconstructor = AlertReconstructor()
                reconstructor( pipe )

            freeprocs = set()
            busyprocs = set()
            donesources = set()
            ids_produced = []

            _logger.info( f'Launching {self.reconstruct_procs} alert reconstruction processes.' )
            for i in range( self.reconstruct_procs ):
                parentconn, childconn = multiprocessing.Pipe()
                proc = multiprocessing.Process( target=lambda: launch_reconstructor( childconn ), daemon=True )
                proc.start()
                self.procinfo[ proc.pid ] = { 'proc': proc,
                                              'parentconn': parentconn,
                                              'childconn': childconn }
                freeprocs.add( proc.pid )

            sourceiddex = 0
            nextlog = 0
            while ( sourceiddex < len(sourceids) ) or ( len(busyprocs) > 0 ):
                if ( log_every > 0 ) and ( sourceiddex >= nextlog ):
                    _logger.info( f"Have started {sourceiddex} of {len(sourceids)} sources, "
                                  f"{totflushed} flushed." )
                    _logger.info( f"Timings:\n"
                                  f"               overall: {time.perf_counter() - overall_t0}\n"
                                  f"             _commtime: {_commtime}\n"
                                  f"            _flushtime: {_flushtime}\n"
                                  f"          _producetime: {_producetime}\n"
                                  f"  _updatealertsenttime: {_updatealertsenttime}" )
                    nextlog += log_every

                # Submit source ids to any free reconstructor process
                t0 = time.perf_counter()
                while ( sourceiddex < len(sourceids) ) and ( len(freeprocs) > 0 ):
                    pid = freeprocs.pop()
                    busyprocs.add( pid )
                    self.procinfo[pid]['parentconn'].send( { 'command': 'do',
                                                             'sourceiddex': sourceiddex,
                                                             'sourceid': sourceids[ sourceiddex ] } )
                    sourceiddex += 1
                _commtime += time.perf_counter() - t0

                # Check for responses from busy reconstructor processes
                doneprocs = set()
                for pid in busyprocs:
                    t0 = time.perf_counter()
                    if not self.procinfo[pid]['parentconn'].poll():
                        continue
                    doneprocs.add( pid )

                    msg = self.procinfo[pid]['parentconn'].recv()
                    if ( 'response' not in msg ) or ( msg['response'] != 'alert produced' ):
                        raise ValueError( f"Unexpected response from child process: {msg}" )

                    didid = msg['sourceid']
                    if didid in donesources:
                        raise RuntimeError(  f'{didid} got processed more than once' )
                    donesources.add( didid )
                    _commtime += time.perf_counter() - t0

                    if reallysend:
                        t0 = time.perf_counter()
                        producer.produce( self.kafka_topic, msg['alert'] )
                        ids_produced.append( didid )
                        _producetime += time.perf_counter() - t0

                    if len( ids_produced ) > flush_every:
                        if reallysend:
                            t0 = time.perf_counter()
                            nstart = len( producer )
                            nleft = producer.flush()
                            _logger.debug( f"producer.flush() {nstart} alerts, returned {nleft}" )
                            totflushed += len( ids_produced )
                            t1 = time.perf_counter()
                            self.update_alertssent( ids_produced )
                            t2 = time.perf_counter()
                            _flushtime += t1 - t0
                            _updatealertsenttime += t2 - t1
                        ids_produced = []

                for pid in doneprocs:
                    busyprocs.remove( pid )
                    freeprocs.add( pid )

                # end of while( sourceiddex < len(sourceids) ) or ( len(busyprocs) > 0 ):

            if len( ids_produced ) > 0:
                if reallysend:
                    t0 = time.perf_counter()
                    nstart = len( producer )
                    nleft = producer.flush()
                    _logger.debug( f"producer.flush() {nstart} alerts, returned {nleft}" )
                    totflushed += len( ids_produced )
                    t1 = time.perf_counter()
                    self.update_alertssent( ids_produced )
                    t2 = time.perf_counter()
                    _flushtime += t1 - t0
                    _updatealertsenttime += t2 - t1
                ids_produced = []

            # Tell alll subprocesses to end
            subtimings = {}
            for pid, proc in self.procinfo.items():
                proc['parentconn'].send( { 'command': 'die' } )
                msg = proc['parentconn'].recv()
                for key, val in msg.items():
                    if key != 'response':
                        if key not in subtimings:
                            subtimings[key] = val
                        else:
                            subtimings[key] += val

            _tottime = time.perf_counter() - overall_t0

            _logger.info( f"**** Done sending {len(sourceids)} alerts; {totflushed} flushed ****" )
            strio = io.StringIO()
            strio.write( f"Timings:\n"
                         f"               overall: {time.perf_counter() - overall_t0}\n"
                         f"             _commtime: {_commtime}\n"
                         f"            _flushtime: {_flushtime}\n"
                         f"          _producetime: {_producetime}\n"
                         f"  _updatealertsenttime: {_updatealertsenttime}\n"
                         f"                   Sum over subprocesses:\n"
                         f"                   ----------------------\n" )
            for key, val in subtimings.items():
                strio.write( f"{key:>36s} : {val}\n" )
            _logger.info( strio.getvalue() )

            return totflushed

        finally:
            _logger.info( "I really hope cleanup gets called." )
            self.cleanup()

# ROB
#
# When you make a main, have it instantiate the alertsender object,
#   and then call the following:

#     # Catch INT and TERM signals so that we can clean up our subprocess
#     signal.signal( signal.SIGINT, lambda signum, frame: sender.interruptor( signum, frame ) )
#     signal.signal( signal.SIGTERM, lambda signum, frame: sender.interruptor( signum, frame ) )

# We *don't* want this in the AlertSender.__call__ method, because that method is called from
#   tests, and it screws up the main process.
