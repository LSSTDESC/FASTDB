import sys
import io
import logging
import datetime
import time
import multiprocessing
import signal
import argparse
import traceback

import confluent_kafka
import fastavro

import db
import util

import numpy as np
from astropy.io import fits

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
    """A class that constructs alerts (in dict format) from the fastdb ppdb tables."""

    def __init__( self, prevsrc=365, prevfrced=365, prevfrced_gap=1, schemadir=None,
                  make_cutouts=False, cutout_size=41 ):
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

          make_cutouts : bool, default False
            If True, create FITS files of random values 100±10 (1σ) to stick in the cutouts fields.

          cutout_size : int, default 41
            If True, the size of the array in FITS files.

        """

        self.prevsrc = prevsrc
        self.prevfrced = prevfrced
        self.prevfrced_gap = prevfrced_gap

        schema = util.get_alert_schema( schemadir=schemadir )
        self.alert_schema = schema[ 'alert' ]
        self.diaobject_schema = schema[ 'diaobject' ]
        self.diasource_schema = schema[ 'diasource' ]
        self.diaforcedsource_schema = schema[ 'diaforcedsource' ]

        if make_cutouts:
            bio = io.BytesIO()
            rng = np.random.get_default_generator()
            fits.writeto( bio, rng.normal(loc=100., scale=10., size=(cutout_size, cutout_size)).astype( np.float32 ) )
            self.fitsdata = bio.getvalue()
        else:
            self.fitsdata = None


    def _reset_timings( self ):
        self.reconstructtime = 0
        self.connecttime = 0
        self.findsourcetime = 0
        self.sourcetodicttime = 0
        self.prevsourcetime = 0
        self.prevforcedsourcetime = 0
        self.prevforcedsourcequerytime = 0
        self.prevforcedsourcetodicttime = 0
        self.objtime = 0
        self.avrowritetime = 0
        self.commtime = 0
        self.tottime = 0


    def object_data_to_dicts( self, rows, columns ):
        allfields = [ f['name'] for f in self.diaobject_schema['fields'] ]
        lcfields = { 'diaObjectId', 'ra', 'raErr', 'dec', 'decErr', 'ra_dec_Cov', 'validityStartMjdTai' }
        timefields = {}

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
        lcfields = { 'diaSourceId', 'visit', 'detector', 'diaObjectId', 'ssObjectId',
                     'parentDiaSourceId', 'midpointMjdTai', 'ra', 'raErr', 'dec', 'decErr', 'ra_dec_Cov',
                     'x', 'xErr', 'y', 'yErr', 'apFlux', 'apFluxErr', 'snr',
                     'psfFlux', 'psfFluxErr', 'psfFluxLnL', 'psfChi2', 'psfNdata',
                     'scienceflux', 'scienceFluxErr', 'templateFlux', 'templateFluxErr',
                     'ixx', 'iyy', 'ixy', 'ixxPSF', 'iyyPSF', 'ixyPSF',
                     'extendedness', 'realibility', 'band',
                     'timeProcessedMjdTai', 'timeWithddrawnMjdTai', 'bboxSize' }
        timefields = {}

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

            # diasource has some pixel flags that are converted to a bitmask in the database
            for mask, field in db.DiaSource._flags_bits.items():
                curdict[field] = bool( row[columns['flags']] & mask )

            for mask, field in db.DiaSource._pixelflags_bits.items():
                curdict[field] = bool( row[columns['pixelflags']] & mask )

            dicts.append( curdict )

        return dicts


    def forced_source_data_to_dicts( self, rows, columns ):
        allfields = [ f['name'] for f in self.diaforcedsource_schema['fields'] ]
        lcfields = [ 'diaForcedSourceId', 'diaObjectId', 'ra', 'dec', 'visit', 'detector',
                     'psfFlux', 'psfFluxErr', 'midpointMjdTai', 'scienceFlux', 'scienceFluxErr',
                     'band', 'timeProcessedMjdTai', 'timeWithdrawnMjdTai' ]
        timefields = {}

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
                  "AND visit!=%(visit)s ORDER BY midpointmjdtai" )
            cursor.execute( q, { 'objid': diasource['diaObjectId'],
                                 'visit': diasource['visit'],
                                 'minmjd': diasource['midpointMjdTai'] - self.prevsrc,
                                 'maxmjd': diasource['midpointMjdTai'] } )
            columns = { col_desc[0]: i for i, col_desc in enumerate(cursor.description) }
            rows = cursor.fetchall()

        return self.source_data_to_dicts( rows, columns )


    def previous_forced_sources( self, diasource, con=None ):
        t0 = time.perf_counter()
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

        t1 = time.perf_counter()
        retval = self.forced_source_data_to_dicts( rows, columns )
        t2 = time.perf_counter()

        self.prevforcedsourcequerytime += t1 - t0
        self.prevforcedsourcetodicttime += t2 - t1

        return retval


    def reconstruct( self, diaobjectid, visit, con=None ):
        t0 = time.perf_counter()
        with db.DB( con ) as con:
            cursor = con.cursor()
            t1 = time.perf_counter()
            cursor.execute( "SELECT * FROM ppdb_diasource WHERE diaobjectid=%(id)s AND visit=%(v)s",
                            { 'id': diaobjectid, 'v': visit } )
            columns = { col_desc[0]: i for i, col_desc in enumerate(cursor.description) }
            rows = cursor.fetchall()
            if len(rows) == 0:
                raise ValueError( f"Unknown diasource diaobjectid={diaobjectid} visit={visit}" )
            if len(rows) > 1:
                raise RuntimeError( f"diasource w/ diaobjectid={diaobjectid} and visit={visit} is multiply defined, "
                                    "I don't know how to cope." )
            t2 = time.perf_counter()
            diasource = self.source_data_to_dicts( rows, columns )[0]
            t3 = time.perf_counter()
            previous_sources = self.previous_sources( diasource, con=con )
            t4 = time.perf_counter()
            previous_forced_sources = self.previous_forced_sources( diasource, con=con )
            t5 = time.perf_counter()
            cursor.execute( "SELECT * FROM ppdb_diaobject WHERE diaobjectid=%(id)s",
                            { 'id': diasource['diaObjectId'] } )
            columns = { col_desc[0]: i for i, col_desc in enumerate(cursor.description) }
            rows = cursor.fetchall()
            if len(rows) == 0:
                raise ValueError( f"Unknown diaobject {diasource['diaObjectId']}" )
            if len(rows) > 1:
                raise RuntimeError( f"diaobject {diasource['diaObjectId']} is multiply defined, I can't cope." )
            diaobject = self.object_data_to_dicts( rows, columns )[0]
            diaobject['nDiaSources'] = 1 + len(previous_sources)
            t6 = time.perf_counter()

            self.connecttime += t1 - t0
            self.findsourcetime += t2 - t1
            self.sourcetodicttime += t3 - t2
            self.prevsourcetime += t4 - t3
            self.prevforcedsourcetime += t5 - t4
            self.objtime += t6 - t5

            # TODO : figure out a good unique alertid, right now this is bad!
            # (See Issue #49)
            alert = { "diaSourceId": diasource['diaSourceId'],
                      "observation_reason": "simulation",
                      "target_name": str( diaobject['diaObjectId'] ),
                      "diaSource": diasource,
                      "prvDiaSources": previous_sources if len(previous_sources) > 0 else None,
                      "prvDiaForcedSources": previous_forced_sources if len(previous_forced_sources) > 0 else None,
                      "diaObject": diaobject,
                      "ssSource": None,
                      "MPCORB": None,
                      "cutoutDifference": self.fitsdata,
                      "cutoutScience": self.fitsdata,
                      "cutoutTemplate": self.fitsdata }

            return alert


    def __call__( self, pipe ):
        """Listen for requests on pipe reconstruct alerts.  Reconstruct, send info back through pipe.

        WARNING : holds open a database connection.  This is necessary,
        because otherwise the overhead of re-estasblishing a connection
        over and over again kills us.  (Weirdly, most of the time shows
        up in "preforcedsourcetime", specifically in t1-t0 of
        previous_forced_sources, but empirically not reconnecting for
        every alert made things an order of magnitude faster.)

        """

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

        self._reset_timings()
        logger.info( "Subprocess starting." )
        overall_t0 = time.perf_counter()

        done = False
        with db.DB() as con:
            while not done:
                try:
                    msg = pipe.recv()
                    if msg['command'] == 'die':
                        logger.info( "Subprocess got die command." )
                        done = True

                    elif msg['command'] == 'do':
                        t0 = time.perf_counter()
                        sourcedex = msg['sourcedex']
                        diaobjid = msg['diaobjid']
                        visit = msg['visit']

                        alert = self.reconstruct( diaobjid, visit, con=con )

                        t1 = time.perf_counter()
                        msgio = io.BytesIO()
                        fastavro.write.schemaless_writer( msgio, self.alert_schema, alert )

                        t2 = time.perf_counter()
                        pipe.send( { 'response': 'alert produced',
                                     'diaobjectid': alert['diaSource']['diaObjectId'],
                                     'visit': alert['diaSource']['visit'],
                                     'sourcedex': sourcedex,
                                     'alert': msgio.getvalue() } )

                        t3 = time.perf_counter()
                        self.reconstructtime += t1 - t0
                        self.avrowritetime += t2 - t1
                        self.commtime += t3 - t2
                        self.tottime += t3 - t0

                    else:
                        raise ValueError( f"Unknown command {msg['command']}" )
                except Exception as ex:
                    sio = io.StringIO()
                    traceback.print_exception( ex, file=sio )
                    _logger.exception( sio.getvalue() )
                    raise ex

        logger.info( "Subprocess sending finished message" )
        pipe.send( { 'response': 'finished',
                     'runtime': time.perf_counter() - overall_t0,
                     'tottime': self.tottime,
                     'reconstructtime': self.reconstructtime,
                     'connecttime': self.connecttime,
                     # 'findsourcetime': self.findsourcetime,
                     # 'sourcetodicttime': self.sourcetodicttime,
                     'prevsourcetime': self.prevsourcetime,
                     'prevforcedsourcetime': self.prevforcedsourcetime,
                     # 'prevforcedsourcequerytime': self.prevforcedsourcequerytime,
                     # 'prevforcedsourcetodicttime': self.prevforcedsourcetodicttime,
                     'objtime': self.objtime,
                     'avrowritetime': self.avrowritetime,
                     'commtime': self.commtime } )



class AlertSender:
    """A class to send simulated LSST AP alerts based on data in the fastdb ppdb tables."""

    def __init__( self, kafka_server, kafka_topic, reconstruct_procs=5, make_cutouts=False, cutout_size=41 ):
        """Constructor

        Parmaeters
        ----------
          kafka_server : str
            The kafka server to send to (passed to
            confluent_kafka.Producer as "bootstrap.servers").

          kafka_topic : str
            The kafka topic to send to.

          reconstruct_procs : int, default 5
            When an object of this class does its work, it will launch
            this many subprocesses each running an AlertReconster to
            reconstruct alerts from the fastdb ppdb tables.  (Because
            reconstruction takes more time than actually sending the
            alerts once they exist, parallelize that part of the
            process.)

        """
        self.kafka_server = kafka_server
        self.kafka_topic = kafka_topic
        self.reconstruct_procs = int( reconstruct_procs )


    def interruptor( self, signum, frame ):
        _logger.error( "Got an interupt signal, cleaning up and exiting." )
        self.cleanup()
        sys.exit()


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
                                "INNER JOIN ppdb_diasource s ON a.diaobjectid=s.diaobjectid "
                                "                           AND a.visit=s.visit" )
                row = cursor.fetchone()
                if row[0] is None:
                    cursor.execute( "SELECT MIN(midpointmjdtai) FROM ppdb_diasource" )
                    row = cursor.fetchone()
                    if row[0] is None:
                        raise RuntimeError( "There are no sources in ppdb_diasource" )
                throughday = row[0] + addeddays

            cursor.execute( "SELECT s.diaobjectid, s.visit, s.midpointmjdtai "
                            "FROM ppdb_diasource s "
                            "LEFT JOIN ppdb_alerts_sent a ON a.diaobjectid=s.diaobjectid AND a.visit=s.visit "
                            "WHERE a.id IS NULL "
                            "AND s.midpointmjdtai<%(maxmjd)s "
                            "ORDER BY s.midpointmjdtai ",
                            { "maxmjd": throughday } )
            rows = cursor.fetchall()
            diaobjids = [ row[0] for row in rows ]
            visits = [ row[1] for row in rows ]

            return diaobjids, visits


    def update_alertssent( self, diaobjids_and_visits ):
        now = datetime.datetime.now( tz=datetime.UTC ) # .isoformat()
        with db.DB() as con:
            cursor = con.cursor()
            with cursor.copy( "COPY ppdb_alerts_sent(diaobjectid,visit,senttime) FROM stdin" ) as curcopy:
                for diaobjid, visit in diaobjids_and_visits:
                    curcopy.write_row( ( diaobjid, visit, now ) )
            con.commit()


    def __call__( self, addeddays=1, throughday=None, reallysend=False, flush_every=1000, log_every=10000,
                  catch_int_and_term=False, make_cutouts=False, cutout_size=21 ):
        """Send alerts.

        Launches AlertReconstructor subprocesses to build the alert
        dictionaries, and sends those alerts to the kafka server and
        topic configured at object instantiation.

        This function is not re-entrant.  Once you've called it once,
        don't call it again.  (Exit the whole program and start over.)
        It mucks about with signal handlers, but doesn't restore state.

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

          catch_int_and_term : bool, default False
             Usually you want this to be True, actually.  Set up signal
             handlers to catch INT and TERM signals, and shut down
             cleanly.  This is not True by default because our tests
             need it to be False.

          make_cutouts : bool, default False
             If True, then FITS images with gratuitous data will be
             included in the cutout* fields.  (It's just noise, don't
             try to actually interpret it.)

          cutout_size : int, default 41
             If make_cutouts is True, then this is the square size of
             the coutout data included.

        """

        self.procinfo = {}
        try:
            # TODO : tag a runningfile so that two jobs don't run at once

            # Get alerts to send
            _logger.info( "Figuring out which sources to send alerts for..." )
            diaobjids, visits = self.find_alerts_to_send( addeddays=addeddays, throughday=throughday )
            _logger.info( f"...got {len(diaobjids)} diasources to send alerts for." )

            if len( diaobjids ) == 0:
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

            def launch_reconstructor( pipe, make_cutouts, cutout_size ):
                reconstructor = AlertReconstructor( make_cutouts=make_cutouts, cutout_size=cutout_size )
                reconstructor( pipe )

            freeprocs = set()
            busyprocs = set()
            donesources = set()
            ids_produced = []

            _logger.info( f'Launching {self.reconstruct_procs} alert reconstruction processes.' )
            for i in range( self.reconstruct_procs ):
                parentconn, childconn = multiprocessing.Pipe()
                proc = multiprocessing.Process( target=lambda: launch_reconstructor( childconn,
                                                                                     make_cutouts,
                                                                                     cutout_size ),
                                                daemon=True )
                proc.start()
                self.procinfo[ proc.pid ] = { 'proc': proc,
                                              'parentconn': parentconn,
                                              'childconn': childconn }
                freeprocs.add( proc.pid )

            # Catch INT and TERM signals to shut down cleanly
            # Need to connect the signal handlers here, not before we launch the
            #   subprocesses, so the subprocesses won't inherit them!
            if catch_int_and_term:
                signal.signal( signal.SIGINT, lambda signum, frame: self.interruptor( signum, frame ) )
                signal.signal( signal.SIGTERM, lambda signum, frame: self.interruptor( signum, frame ) )

            sourcedex = 0
            nextlog = 0
            while ( sourcedex < len(diaobjids) ) or ( len(busyprocs) > 0 ):
                if ( log_every > 0 ) and ( sourcedex >= nextlog ):
                    _logger.info( f"Have started {sourcedex} of {len(diaobjids)} sources, "
                                  f"{totflushed} flushed." )
                    dt = time.perf_counter() - overall_t0
                    _logger.info( f"Timings:\n"
                                  f"               overall: {dt:.2f}  "
                                  f"({sourcedex/dt:.1f} or {totflushed/dt:.1f} Hz)\n"
                                  f"             _commtime: {_commtime:.2f}\n"
                                  f"            _flushtime: {_flushtime:.2f}\n"
                                  f"          _producetime: {_producetime:.2f}\n"
                                  f"  _updatealertsenttime: {_updatealertsenttime:.2f}" )
                    nextlog += log_every

                # Submit source ids to any free reconstructor process
                t0 = time.perf_counter()
                while ( sourcedex < len(diaobjids) ) and ( len(freeprocs) > 0 ):
                    pid = freeprocs.pop()
                    busyprocs.add( pid )
                    self.procinfo[pid]['parentconn'].send( { 'command': 'do',
                                                             'sourcedex': sourcedex,
                                                             'diaobjid': diaobjids[ sourcedex ],
                                                             'visit': visits[ sourcedex ] } )
                    sourcedex += 1
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

                    didid = ( msg['diaobjectid'], msg['visit'] )
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

                # end of while( sourcedex < len(diaobjids) ) or ( len(busyprocs) > 0 ):

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

            _logger.info( f"**** Done sending {len(diaobjids)} alerts; {totflushed} flushed ****" )
            strio = io.StringIO()
            dt = time.perf_counter() - overall_t0
            strio.write( f"Timings:\n"
                         f"               overall: {dt:.2f}  ({len(diaobjids)/dt:.1f} or {totflushed/dt:.1f} Hz)\n"
                         f"             _commtime: {_commtime:.2f}\n"
                         f"            _flushtime: {_flushtime:.2f}\n"
                         f"          _producetime: {_producetime:.2f}\n"
                         f"  _updatealertsenttime: {_updatealertsenttime:.2f}\n"
                         f"                   Sum over subprocesses:\n"
                         f"                   ----------------------\n" )
            for key, val in subtimings.items():
                strio.write( f"{key:>36s} : {val:.2f}\n" )
            _logger.info( strio.getvalue() )

            return totflushed

        finally:
            _logger.info( "I really hope cleanup gets called." )
            self.cleanup()


# ======================================================================

def main():
    parser = argparse.ArgumentParser( 'projectsim.py', description='Send out simulated LSST AP alerts',
                                      formatter_class=argparse.ArgumentDefaultsHelpFormatter )
    parser.add_argument( "-s", "--kafka-server", default="kafka:9092", help="Kafka server to send to" )
    parser.add_argument( "-t", "--kafka-topic", default="test-ap-alerts", help="Kafka topic to send to" )
    parser.add_argument( "-p", "--processes", type=int, default=5,
                         help="Number of alert reconstruction subprocesses to run." )
    parser.add_argument( "--do", action='store_true', default=False,
                         help="Actually stream alerts (otherwise, just test reconstructing them)." )
    parser.add_argument( "-a", "--added-days", type=float, default=None,
                         help="Send alerts for this many days of detections past the last alert sent." )
    parser.add_argument( "-d", "--through-day", type=float, default=None,
                         help=( "Send alerts for sources detected through this MJD.  Must specify exactly "
                                "one of --added-days and --through-day" ) )
    parser.add_argument( "-f", "--flush-every", type=int, default=1000,
                         help="Flush the kafka producer when its accumulated this may messages" )
    parser.add_argument( "-l", "--log-every", type=int, default=10000,
                         help="Print timing information at intervals of this many alerts sent." )
    args = parser.parse_args()

    if ( args.added_days is None ) == ( args.through_day is None ):
        raise ValueError( "Must specify at least but only one of --added-days and --through-day" )

    sender = AlertSender( args.kafka_server, args.kafka_topic, reconstruct_procs=args.processes )

    sender( addeddays=args.added_days, throughday=args.through_day, reallysend=args.do,
            flush_every=args.flush_every, log_every=args.log_every,
            catch_int_and_term=True )


# ======================================================================
if __name__ == "__main__":
    main()
