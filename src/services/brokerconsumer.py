import sys
import os
import io
import re
import collections
import time
import yaml
import datetime
import traceback
import pathlib
import urllib
import logging
import argparse
import multiprocessing
import signal
import simplejson

import confluent_kafka
import fastavro
import pymongo

import db
from kafka_consumer import KafkaConsumer

# Default location of BrokerMessage schema
_default_brokermessage_schemafile = "/fastdb/share/avsc/fastdb.v10_0_0.BrokerMessage.avsc"

from concurrent.futures import ThreadPoolExecutor  # for pittgoogle
import pittgoogle

_rundir = pathlib.Path(__file__).parent
_logdir = pathlib.Path( os.getenv( 'LOGDIR', '/logs' ) )


class BrokerConsumer:
    """A class for consuming broker messages from brokers.

    It populates the following collections in mongo, whose schema are
    based on the postgres tables.  These are caches; source_importer.py
    merges them into their permanent storage.  All of these collections

       {mongodb_collection_base}_diaobject
          { 'diaobjectid': long long [INDEX],
            'savetime': datetime [INDEX],
            'diaobjectposition: diaobject_position columns (omit: diaobjectid, base_procver_id, created_at)
          }

       {mongodb_collection_base}_diasource
          { 'diasourceid': long long [INDEX],
            'savetime': datetime [INDEX]
            [ diasource columns (omit: base_procver_id) ]
          }

       {mongodb_collection_base}_diasource_extra,
          { 'diasourceid': long long [INDEX],
            'savetime': datetime [INDEX]
            [ diasource_extra columns (omit: base_procver_id) ]
          }

       {mongodb_collection_base}_diaforcedsource
          { 'diaforcedsourceid': long long [INDEX],
            'savetime': datetime [INDEX],
            [ diaforcedsource columns (omit: base_procver_id) ]
          }

       {mongodb_collection_base}_diaforcedsource_extra
          { 'diaforcedsourceid': long long [INDEX],
            'savetime': datetime [INDEX],
            [ diaforcedsource_extra columns (omit: base_procver_id) ]
          }

       {mongodb_collection_base}_thumbnails
          { 'diasourceid': long long [INDEX],
            'savetime': datetime [INDEX],
            'cutoutdifference': bytes,
            'cutoutscience': bytes,
            'cutouttemplate': bytes
          }

       {mongodb_collection_base}_brokerinfo
          { 'brokername': str,
            'topic': str,
            'diasourceid': long long [INDEX],
            'diaobjectid': long long,
            'prv_diasourceid': array of long long or null,
            'prv_diaforcedsourceid': array of long long or null,
            'savetime': datetime [INDEX],
            'msgtime': str,
            'info': dict
          }


    Optionally, there's also {mongodb_collection_base}_alertcache that has:
      { 'topic': str
        'msgoffset': int?
        'timestamp': datetime,
        'savetime': datetime,
        'msg': dict
      }
    (If this is included it doubles the amount of stuff saved in mongo!
    This is really here only for debugging purposes.)

    This class will work as-is only if the broker is a kafka server
    requiring no authentication (though you may be able to get it to
    work using extraconfig).  Often you will instantiate a subclass
    instead of instantating BrokerConsumer directly.

    Currently supports only kafka brokers, though there is some
    (currently broken and commented out) code for pulling from the
    pubsub Pitt-Google broker.

    Logging : sends log messages to stderr with a log message prefix that
    includes loggername_prefix and loggername.  Writes log messages with
    counts to a file created under _logdir (which is set in the env var
    $LOGDIR, but defaults to /logs).  The count log file is named
    "countlogger_{loggername_prefix}{loggername}".  (The variables
    loggername_prefix and loggername are passed at object construction).

    TODO : implement count log file rotation?

    """

    _standard_lsst_alert_fields = [ 'diaSourceId', 'observation_reason', 'target_name',
                                    'diaSource', 'prvDiaSources', 'prvDiaForcedSources',
                                    'diaObject', 'ssSource', 'mpc_orbits',
                                    'cutoutDifference', 'cutoutScience', 'cutoutTemplate' ]

    def __init__( self, server, groupid, topics=None, updatetopics=False, extraconfig={},
                  schemaless=False, schema_in_key=False, schemafile=None,
                  brokername_for_alerts=None, brokername_key=None,
                  mongodb_collection_base=None, cache_alerts=False, no_wrangle=False,
                  pipe=None, loggername="BROKER", loggername_prefix='',
                  consume_timeout=1, nomsg_sleeptime=5, batch_size=1000 ):
        """Create a connection to a kafka server and consumer broker messages.

        Note that you often (but not always) want to instantiate a subclass.

        Parameters
        ----------
          server : str
            Name of kafka server

          groupid : str
            Group id to connect to

          topics : list, str, or None
            Topics to subscribe to.  If None, won't subscribe on object
            construction.

          updatetopics : bool, default False
            True if topic list needs to be updated dynamically.  This is
            implemented by some subclasses, is not supported directly by
            BrokerConsumer.

          extraconfig : dict, default {}
            Additional config to pass to the confluent_kafka.Consumer
            constructor.  (Do not include bootstrap.servers,
            auto.offset.reset, or group.id; those are automatically
            constructed and sent.)

          schemaless : bool, default False
            If True, expecting schemaless avro messages.  If False,
            expecting embedded schema.  Ignored if you pass a handler to
            poll.

          schema_in_key : bool, default False
            Ignored if schemaless is True.  If schemaless is False, this
            means that in the kafka messages, the schem is expected to
            be embedded in the message key (which is the way Fink does
            it, different from fakebroker).

          schemafile : Path or str
            The .avsc the that holds the schema of the messsages we'll
            be ingesting.  Required if schemaless is True.  The schema
            must be named properly for its namespace, and any other
            schema in the same namespace referred to by that .avsc file
            must be in the same directory with the right names.  If not
            given, uses the location where it is find in the docker
            image we use.

          brokername_for_alerts : str, default None
            If given, then when the mongo database is populated, the
            'brokername' field will have this value.  If neither this
            nor brokername_key is given, then brokername will be
            cls._brokername.

          brokername_key : str, default None
            If given, then when the mongo database is populated, the
            'brokername' field will have the value from this key of the
            message.  If neither this nor brokername_for_alerts is given,
            then brokername will have the class name.

          mongodb_collection_base : str, required
            Will create several collections in the mongo database and write to them:
              {mongodb_collection_base}_diaobject
              {mongodb_collection_base}_diasource
              {mongodb_collection_base}_diasource_extra
              {mongodb_collection_base}_diaforcedsource
              {mongodb_collection_base}_diaforcedsource_extra
              {mongodb_collection_base}_thumbnails
              {mongodb_collection_base}_brokerinfo
              {mongodb_collection_base}_alertcache  [ empty unless cache_alerts is True ]

          cache_alerts : bool, default False
             If True, make almost-raw copies of the alerts into
             {mongodb_collection_base}_alertcache, for debugging
             purposes.  Only set this to True when you're debugging, as
             it doubles the amount of stuff saved to the mongodb.

          no_wrangle : bool, default False
             Requires cache_alerts.  If true, the only the
             {mongdb_collection_base}_alertcache collection will be
             written, not the wrangled ones with parsed information.
             Mostly useful for debugging purposes.  May not be
             implemented in all subclasses.

          pipe : multiprocessing.Pipe or None
            If not None, a call to poll will regularly send hearbeats to
            this Pipe.  It will also poll the pipe for messages.
            (Currently ,the only message it will handle is a request to
            die.)

          loggername : str, default "BROKER"
            Used in creating log files and in headers of log messages

          loggername_prefix : str, default ""
            Used in headers of log messages

          consume_timeout : int (float?), default 1
            Number of seconds the kafka consumer should wait to see if it
            can fill it's batch size of messages.

          nomsg_sleeptime : int, default 5
            The KafkaConsumer (src/kafkaconsumer.py) will sleep this
            many seconds between not finding any new messages and
            polling again to ask for new messages.

          batch_size : int, default 1000
            Try to consume this many messages at once.

        """

        if not _logdir.is_dir():
            raise RuntimeError( f"Log directory {_logdir} isn't an existing directory." )
        # TODO: verify we can write to it

        self.logger = logging.getLogger( loggername )
        self.logger.propagate = False
        logout = logging.StreamHandler( sys.stderr )
        self.logger.addHandler( logout )
        formatter = logging.Formatter( ( f'[%(asctime)s - {loggername_prefix}{loggername} - '
                                         f'%(levelname)s] - %(message)s' ),
                                       datefmt='%Y-%m-%d %H:%M:%S' )
        logout.setFormatter( formatter )
        # self.logger.setLevel( logging.INFO )
        self.logger.setLevel( logging.DEBUG )

        self.countlogger = logging.getLogger( f"countlogger_{loggername_prefix}{loggername}" )
        self.countlogger.propagate = False
        _countlogout = logging.FileHandler( _logdir / f"brokerpoll_counts_{loggername_prefix}{loggername}.log" )
        _countformatter = logging.Formatter( '[%(asctime)s - %(levelname)s] - %(message)s',
                                             datefmt='%Y-%m-%d %H:%M:%S' )
        _countlogout.setFormatter( _countformatter )
        self.countlogger.addHandler( _countlogout )
        self.countlogger.setLevel( logging.INFO )
        # self.countlogger.setLevel( logging.DEBUG )

        if schemafile is None:
            # This is where the schema lives inside our docker images...
            #   though the version of the namespace will evolve.
            schemafile = _default_brokermessage_schemafile

        self.countlogger.info( f"************ Starting BrokerConsumer for {loggername} ****************" )

        self.pipe = pipe
        self.server = server
        self.groupid = groupid
        if self.groupid is None:
            raise ValueError( "groupid is required" )
        if isinstance( topics, str ):
            self.topics = [ topics ]
        elif isinstance( topics, collections.abc.Sequence ):
            self.topics = list( topics )
        elif topics is not None:
            raise TypeError( f"topics must be a str or a list of str, got a {type(topics)}" )
        self.topics = topics
        self._updatetopics = updatetopics
        self.extraconfig = extraconfig
        self.nomsg_sleeptime = nomsg_sleeptime
        self.batch_size = batch_size
        self.consume_timeout = consume_timeout
        self.brokername_for_alerts = brokername_for_alerts
        self.brokername_key = brokername_key

        self.schemaless = schemaless
        self.schema_in_key=schema_in_key
        if schemafile is None:
            self.schemafile = None
            self.schema = None
        else:
            self.schemafile = schemafile
            self.schema = fastavro.schema.load_schema( self.schemafile )

        if ( not isinstance( mongodb_collection_base, str ) ) or ( len(mongodb_collection_base) == 0 ):
            raise ValueError( "Must pass a non-0 length string as mongdb_collection_base" )
        self.mongodb_collection_base = urllib.parse.quote_plus( mongodb_collection_base )
        self.cache_alerts = cache_alerts
        self.no_wrangle = no_wrangle
        if self.no_wrangle and ( not self.cache_alerts ):
            raise ValueError( "no_wrangle requires cache_alerts" )
        self.ensure_collections()

        self.logger.info( f"Writing broker messages to monogdb collections {self.mongodb_collection_base}*" )


    def ensure_collections( self ):
        with db.MGCon() as mg:
            suffixes = { 'diaobject': [ ['diaobjectid'], ['savetime'] ],
                         'diasource': [ ['diasourceid'], ['savetime'] ],
                         'diasource_extra': [ ['diasourceid'], ['savetime'] ],
                         'diaforcedsource': [ ['diaforcedsourceid'], ['savetime'] ],
                         'diaforcedsource_extra': [ ['diaforcedsourceid'], ['savetime'] ],
                         'thumbnails': [ ['diasourceid'], ['savetime'] ],
                         'brokerinfo': [ ['brokername', 'topic', 'diasourceid'], ['savetime'] ],
                         'alertcache': []
                        }
            for suffix, wantedindexes in suffixes.items():
                if self.no_wrangle and ( suffix != 'alertcache' ):
                    continue
                col = mg.collection( f"{self.mongodb_collection_base}_{suffix}" )
                for wantedindex in wantedindexes:
                    if wantedindex not in [ list( i['key'].keys() ) for i in col.list_indexes() ]:
                        col.create_index( [ (i, pymongo.ASCENDING) for i in wantedindex ] )

    def create_connection( self, reset=False ):
        countdown = 5
        if reset:
            self.countlogger.info( "*************** Resetting to start of broker kafka stream ***************" )
        else:
            self.countlogger.info( "*************** Connecting to kafka stream without reset  ***************" )
        while countdown >= 0:
            try:
                self.consumer = KafkaConsumer( self.server, self.groupid,
                                               schema=self.schemafile, schemaless=self.schemaless,
                                               topics=self.topics, reset=reset,
                                               extraconsumerconfig=self.extraconfig,
                                               consume_nmsgs=self.batch_size, consume_timeout=self.consume_timeout,
                                               nomsg_sleeptime=self.nomsg_sleeptime,
                                               logger=self.logger, countlogger=self.countlogger )
                countdown = -1
            except Exception as e:
                countdown -= 1
                strio = io.StringIO("")
                strio.write( f"Exception connecting to broker: {str(e)}" )
                traceback.print_exc( file=strio )
                self.logger.warning( strio.getvalue() )
                if countdown >= 0:
                    self.logger.warning( "Sleeping 5s and trying again." )
                    time.sleep(5)
                else:
                    self.logger.error( "Repeated exceptions connecting to broker, punting." )
                    self.countlogger.error( "Repeated exceptions connecting to broker, punting." )
                    raise RuntimeError( "Failed to connect to broker" )

        self.countlogger.info( "**************** Consumer connection opened *****************" )

    def close_connection( self ):
        try:
            self.countlogger.info( "**************** Closing consumer connection ******************" )
            self.consumer.close()
        except Exception as ex:
            self.countlogger.error( f"Got exception trying to close consumer, ignoring it: {str(ex)}" )
        self.consumer = None

    def update_topics( self, *args, **kwargs ):
        self.countlogger.info( "Subclass must implement this if you use it." )
        raise NotImplementedError( "Subclass must implement this if you use it." )

    def reset_to_start( self ):
        raise RuntimeError( "This is probably broken" )
        self.logger.info( "Resetting all topics to start" )
        for topic in self.topics:
            self.consumer.reset_to_start( topic )

    @classmethod
    def add_flags( cls, dictobj, key, flagmap, row ):
        if not any( field in row for field in flagmap.values() ):
            return
        val = 0
        for mask, field in flagmap.items():
            if ( field in row ) and ( row[field] ):
                val |= mask
        dictobj[ key ] = val

    @classmethod
    def _filter_dict_to_table( cls, alertdict, tablemeta ):
        outdict = {}
        for field, value in alertdict.items():
            colname = field.lower()
            if colname in tablemeta.keys():
                colinfo = tablemeta[colname]
                if not colinfo.is_nullable:
                    value = colinfo.null_to_nan_if_necessary( value )
                outdict[colname] = value
        return outdict

    @classmethod
    def _wrangle_object( cls, msg, metamsg ):
        obj = { 'diaobjectid': msg['diaObject']['diaObjectId'],
                'savetime': metamsg['savetime'],
                'diaobjectposition': None }
        if all( ( i in msg['diaObject'] ) and ( i is not None ) for i in ['ra', 'dec'] ):
            obj['diaobjectposition'] = {
                'ra': msg['diaObject']['ra'],
                'dec': msg['diaObject']['dec'],
                'raerr': msg['diaObject']['raErr'] if 'raErr' in msg['diaObject'] else None,
                'decerr': msg['diaObject']['decErr'] if 'decErr' in msg['diaObject'] else None,
                'ra_dec_cov': msg['diaObject']['ra_dec_Cov'] if 'ra_dec_Cov' in msg['diaObject'] else None
            }
        return obj

    @classmethod
    def _wrangle_diasource( cls, submsg, metamsg, msg ):
        out = cls._filter_dict_to_table( submsg, db.DiaSource.tablemeta() )
        # This next field is used in one of our tests....
        out['msg_diasourceid'] = msg['diaSourceId']
        out['savetime'] = metamsg['savetime']
        return out

    @classmethod
    def _wrangle_diasource_extra( cls, submsg, metamsg, msg ):
        out = cls._filter_dict_to_table(submsg, db.DiaSourceExtra.tablemeta() )
        if ( ( len(out) == 0 )
             and ( not any( i in submsg for i in [ db.DiaSourceExtra._flags_bits.values() ] ) )
             and ( not any( i in submsg for i in [ db.DiaSourceExtra._pixelflags_bits.values() ] ) )
            ):
            return None
        out['savetime'] = metamsg['savetime']
        # a couple fields that are composed from mutiple fileds from the alert
        cls.add_flags( out, 'flags', db.DiaSourceExtra._flags_bits, submsg )
        cls.add_flags( out, 'pixelflags', db.DiaSourceExtra._pixelflags_bits, submsg )
        return out

    @classmethod
    def _wrangle_diaforcedsource( cls, submsg, metamsg, msg ):
        out = cls._filter_dict_to_table( submsg, db.DiaForcedSource.tablemeta() )
        # This next field is used in one of our tests....
        out['msg_diasourceid'] = msg['diaSourceId']
        out['savetime'] = metamsg['savetime']
        return out

    @classmethod
    def _wrangle_diaforcedsource_extra( cls, submsg, metamsg, msg ):
        out = cls._filter_dict_to_table( submsg, db.DiaForcedSourceExtra.tablemeta() )
        if len( out ) == 0:
            return None
        out['savetime'] = metamsg['savetime']
        return out

    @classmethod
    def _wrangle_all_standard_lsst_fields( cls, metamsg, msg ):
        obj = cls._wrangle_object( msg, metamsg )

        sources = [ cls._wrangle_diasource( msg['diaSource'], metamsg, msg ) ]
        ext = cls._wrangle_diasource_extra( msg['diaSource'], metamsg, msg )
        sources_extra = [] if ext is None else [ ext ]

        if ( 'prvDiaSources' in msg ) and ( msg['prvDiaSources'] is not None ):
            sources.extend( [ cls._wrangle_diasource( p, metamsg, msg ) for p in msg['prvDiaSources'] ] )
            sources_extra.extend( [ cls._wrangle_diasource_extra( p, metamsg, msg )
                                    for p in msg['prvDiaSources'] if p is not None ] )

        forcedsources = []
        forcedsources_extra = []
        if ( 'prvDiaForcedSources' in msg ) and ( msg['prvDiaForcedSources'] is not None ):
            forcedsources.extend( [ cls._wrangle_diaforcedsource( p, metamsg, msg )
                                    for p in msg['prvDiaForcedSources'] ] )
            forcedsources_extra.extend( [ cls._wrangle_diaforcedsource_extra( p, metamsg, msg )
                                          for p in msg['prvDiaForcedSources'] if p is not None ] )

        if any( ( f in msg and f is not None ) for f in [ 'cutoutDifference', 'cutoutScience', 'cutoutTemplate' ] ):
            thumbnails = { 'diasourceid': msg['diaSource']['diaSourceId'],
                           'savetime': metamsg['savetime'] }
            thumbnails.update( { f.lower(): msg[f] if f in msg else None
                                 for f in ['cutoutDifference', 'cutoutScience', 'cutoutTemplate' ] } )
        else:
            thumbnails = None

        return { 'object': obj,
                 'sources': sources,
                 'sources_extra': sources_extra,
                 'forcedsources': forcedsources,
                 'forcedsources_extra': forcedsources_extra,
                 'thumbnails': thumbnails }


    def alert_wrangler( self, messagebatch ):
        """Convert the alert structure from what we get to what we want to stuff in the mongo db.

        Subclasses should override this to customize the behavior for
        each broker.  This default assumes the message came in following
        the fastdb*brokerMessage schema.

        Parmameters
        -----------
          messagebatch: list of... something
            A list of whatever it is that the broker sends.  Nomrmally
            this is going to be a sequence of avro messages, and that's
            what this class expects.  Subclasses may potentially do
            something very different.

        Returns
        -------
         dictionary of lists: objects, sources_extra, forcedsources, forcedsources_extra, thumbnailses, brokerinfos

           These are suitable for stuffing into the respedctive mongo collections

        """

        objects = []
        sources = []
        sources_extra = []
        forcedsources = []
        forcedsources_extra = []
        thumbnailses = []
        brokerinfos = []
        for i in range( len(messagebatch) ):
            metamsg = messagebatch[i]
            msg = metamsg['msg']
            stuff = self._wrangle_all_standard_lsst_fields( metamsg, msg )
            if stuff['object'] is not None:
                objects.append( stuff['object'] )
            sources.extend( stuff['sources'] )
            sources_extra.extend( stuff['sources_extra'] )
            forcedsources.extend( stuff['forcedsources'] )
            forcedsources_extra.extend( stuff['forcedsources_extra'] )
            if stuff['thumbnails'] is not None:
                thumbnailses.append( stuff['thumbnails'] )

            brokerinfos.append(
                { 'brokername': metamsg['brokername'],
                  'topic': metamsg['topic'],
                  'diasourceid': msg['diaSourceId'],
                  'diaobjectid': msg['diaObject']['diaObjectId'],
                  'prv_diasourceid': ( None if msg['prvDiaSources'] is None
                                       else [ m['diaSourceId'] for m in msg['prvDiaSources'] ] ),
                  'prv_diaforcedsourceid': ( None if msg['prvDiaForcedSources'] is None
                                             else [ m['diaForcedSourceId'] for m in msg['prvDiaForcedSources'] ] ),
                  'savetime': metamsg['savetime'],
                  'msgtime': metamsg['timestamp'],
                  'info': { k:v for k, v in msg.items() if k not in self._standard_lsst_alert_fields }
                 }
            )

        return { 'objects': objects,
                 'sources': sources,
                 'sources_extra': sources_extra,
                 'forcedsources': forcedsources,
                 'forcedsources_extra': forcedsources_extra,
                 'thumbnailses': thumbnailses,
                 'brokerinfos': brokerinfos }

    def handle_message_batch( self, msgs ):
        messagebatch = []
        self.countlogger.info( f"Handling {len(msgs)} messages; consumer has received "
                               f"{self.consumer.tot_handled} messages." )
        now = datetime.datetime.now( tz=datetime.UTC )
        t0 = time.perf_counter()
        for msg in msgs:
            timestamptype, timestamp = msg.timestamp()

            if timestamptype == confluent_kafka.TIMESTAMP_NOT_AVAILABLE:
                timestamp = None
            else:
                timestamp = datetime.datetime.fromtimestamp( timestamp / 1000 )

            key = msg.key()
            payload = msg.value()
            if self.schemaless:
                alert = fastavro.schemaless_reader( io.BytesIO( payload ), self.schema )
            else:
                if self.schema_in_key:
                    if isinstance( key, bytes ):
                        key = key.decode( "utf-8" )
                    parsed_schema = fastavro.schema.parse_schema( simplejson.loads( key ) )
                    alert = fastavro.schemaless_reader( io.BytesIO( payload ), parsed_schema )
                else:
                    # ...there may be a better way than instantiating a new reader for every
                    #   message.  Figure it out.
                    reader = fastavro.read.reader( io.BytesIO( payload ) )
                    alertlist = [ m for m in reader ]
                    if len(alertlist) != 1:
                        raise RuntimeError( "This should never happen." )
                    alert = alertlist[0]

            if self.brokername_for_alerts is not None:
                bname = self.brokername_for_alerts
            elif self.brokername_key is not None:
                bname = alert[ self.brokername_key ]
            else:
                bname = self._brokername

            messagebatch.append( { 'brokername': bname,
                                   'topic': msg.topic(),
                                   'msgoffset': msg.offset(),
                                   'timestamp': timestamp,
                                   'savetime': now,
                                   'msg': alert } )

        t1 = time.perf_counter()
        if self.no_wrangle:
            wrangled = {}
        else:
            wrangled = self.alert_wrangler( messagebatch )
        t2 = time.perf_counter()
        nadded = self.mongodb_store( messagebatch=messagebatch, **wrangled )
        t3 = time.perf_counter()

        strio = io.StringIO()
        strio.write( f"...added to mongodb:\n"
                     f"              {nadded['diaobject']} diaobject\n"
                     f"              {nadded['diasource']} diasource\n"
                     f"              {nadded['diasource_extra']} diasource_extra\n"
                     f"              {nadded['diaforcedsource']} diaforcedsource\n"
                     f"              {nadded['diaforcedsource_extra']} diaforcedsource_extra\n"
                     f"              {nadded['thumbnails']} thumbnails\n"
                     f"              {nadded['brokerinfo']} brokerinfo"
                    )
        if self.cache_alerts:
            strio.write( f"\n              {nadded['alertcache']} cached alerts" )
        strio.write( f"\n   ...parse time: {t1-t0:.3f}\n" )
        strio.write( f"   ...wrangle time: {t2-t1:.3f}\n" )
        strio.write( f"   ...store time: {t3-t2:.3f}" )
        self.countlogger.info( strio.getvalue() )


    def mongodb_store( self, objects=[], sources=[], sources_extra=[],
                       forcedsources=[], forcedsources_extra=[],
                       thumbnailses=[], brokerinfos=[], messagebatch=[] ):
        # ****
        self.logger.debug( f"mongodb_store called with:\n"
                           f"    ....{len(objects)} objects\n"
                           f"    ....{len(sources)} sources\n"
                           f"    ....{len(sources_extra)} sources_extra\n"
                           f"    ....{len(forcedsources)} forcedsources\n"
                           f"    ....{len(forcedsources_extra)} forcedsources_extra\n"
                           f"    ....{len(brokerinfos)} brokerinfos\n"
                           f"    ....{len(messagebatch)} messagebatch\n" )
        # ****
        inserted = {}
        with db.MGCon() as mg:
            for arr, suffix in zip( [ objects, sources, sources_extra,
                                      forcedsources, forcedsources_extra,
                                      thumbnailses, brokerinfos ],
                                    [ 'diaobject', 'diasource', 'diasource_extra',
                                      'diaforcedsource', 'diaforcedsource_extra',
                                      'thumbnails', 'brokerinfo' ] ):
                if len( arr ) > 0:
                    if self.no_wrangle:
                        inserted[suffix] = 0
                    else:
                        col = mg.collection( f'{self.mongodb_collection_base}_{suffix}' )
                        results = col.insert_many( arr, ordered=False )
                        inserted[suffix] = len( results.inserted_ids )
            if self.cache_alerts and ( len(messagebatch) > 0 ):
                col = mg.collection( f'{self.mongodb_collection_base}_alertcache' )
                results = col.insert_many( messagebatch, ordered=False )
                inserted['alertcache'] = len( results.inserted_ids )
        # ****
        import pprint
        strio = io.StringIO()
        strio.write( "mongodb_store returning:\n" )
        pprint.pp( inserted, stream=strio )
        self.logger.debug( strio.getvalue() )
        # ****
        return inserted


    def poll( self, reset=False, restart_time=datetime.timedelta(minutes=30),
              notopic_sleeptime=300, max_restarts=None, max_msgs=None ):
        """Poll the server, saving consumed messages to the Mongo DB.

        Parameters
        ----------
          reset: bool, default False
            If True, reset the topics the first time we connect to the server.
            Usually you want this to be False, so you will pick up where you
            left off (with the server remembering where you were based on the
            groupid you passed at object construction).

          restart_time: datetime.timedelta, default 30 minutes
            Only query the kafka server for this long before closing and
            reopening the connection.  This is just a standard "turn it
            off and back on" cruft-cleaning mechanism.  Make this None
            to never restart.  (Which means you're very trusting
            of a lack of a need to power cycle.)

          notopic_sleeptime : float, default 300
            If the topic doesn't exist on the kafka server, sleep this
            many seconds before checking again to see if the topic
            exists.

          max_restarts: int, default None
            If not None, after this many restarts of the server (after a
            restart_time timeout), exit the poll loop.  If this is None, the
            poll loop runs indefinitely (or until a "die" message is sent
            over the pipe).

          max_msgs: int, default None
            If given, exit after ingesting this many messages

        TODO : separate max_restarts from polling from max_restarts from
        topic not existing, because the timeouts for the two are likely very
        different.

        """

        tstart = datetime.datetime.now()
        try:
            self.create_connection( reset )
            n_restarts = 0
            max_exceptions = 5
            num_exceptions = 0
            while True:
                if self._updatetopics:
                    self.update_topics()
                strio = io.StringIO("")
                if len(self.consumer.topics) == 0:
                    self.logger.info( f"No topics, will wait {notopic_sleeptime}s and reconnect." )
                    time.sleep( notopic_sleeptime )
                else:
                    self.logger.info( f"Subscribed to topics: {self.consumer.topics}; starting poll loop." )
                    self.countlogger.info( f"Subscribed to topics: {self.consumer.topics}; starting poll loop." )
                    try:
                        happy = self.consumer.poll_loop( handler=self.handle_message_batch, pipe=self.pipe,
                                                         stopafter=restart_time,
                                                         stopafternmessages=max_msgs,
                                                         stopafternsleeps=None )
                        if happy:
                            strio.write( f"Reached poll timeout and/or message limit for {self.server}; "
                                         f"handled {self.consumer.tot_handled} messages.  " )
                        else:
                            strio.write( f"Poll loop received die command after handling "
                                         f"{self.consumer.tot_handled} messages.  Exiting.  " )
                            self.logger.info( strio.getvalue() )
                            self.countlogger.info( strio.getvalue() )
                            tot_handled = self.consumer.tot_handled
                            self.close_connection()
                            if self.pipe is not None:
                                self.pipe.send( { "message": "died", "nconsumed": -1,
                                                  "tot_handled": tot_handled,
                                                  "runtime": datetime.datetime.now() - tstart } )
                            return

                    except Exception as e:
                        otherstrio = io.StringIO("")
                        traceback.print_exc( file=otherstrio )
                        num_exceptions += 1
                        if num_exceptions >= max_exceptions:
                            self.close_connection()
                            strio.write( f"Exception polling: {str(e)}.  Exiting after {num_exceptions} exceptions.\n"
                                         f"{otherstrio.getValue()}" )
                            self.logger.error( strio.getvalue() )
                            tot_handled = self.consumer.tot_handled
                            self.close_connection()
                            if self.pipe is not None:
                                self.pipe.send( { "message": "too many exceptions", "nconsumed": -1,
                                                  "tot_handled": tot_handled,
                                                  "runtime": datetime.datetime.now() - tstart } )
                            return
                        else:
                            strio.write( f"Exception polling: {str(e)}; will restart consumer.\n"
                                         f"{otherstrio.getvalue()}" )
                            self.logger.warning( strio.getvalue() )

                if ( max_restarts is not None ) and ( n_restarts >= max_restarts ):
                    strio.write( f"Exiting after {n_restarts} restarts." )
                    self.logger.info( strio.getvalue() )
                    self.countlogger.info( strio.getvalue() )
                    tot_handled = self.consumer.tot_handled
                    self.close_connection()
                    if self.pipe is not None:
                        self.pipe.send( { "message": "exited", "nconsumed": -1,
                                          "tot_handled": tot_handled,
                                          "runtime": datetime.datetime.now() - tstart } )
                    return

                strio.write( "Reconnecting to server.\n" )
                self.logger.info( strio.getvalue() )
                self.countlogger.info( strio.getvalue() )
                self.close_connection()
                # TODO : think about automatic topic updating
                # if self._updatetopics:
                #     self.consumer.topics = None
                # Only want to reset the at most first time we connect!
                # If we disconnect and reconnect in the loop, we want to
                # pick up where we left off.
                self.create_connection( reset=False )
                n_restarts += 1

        except Exception as ex:
            self.logger.error( f"Unhandled exception in BrokerConsumer.poll: {ex}" )
            self.countlogger.error( f"Unhandled exception in BrokerConsumer.poll: {ex}" )
            self.close_connection()
            if self.pipe is not None:
                self.pipe.send( { "message": "unhandled exception", "nconsumed": -1,
                                  "exception": str(ex),
                                  "tot_handled": self.consumer.tot_handled,
                                  "runtime": datetime.datetime.now() - tstart } )
            return



# ======================================================================

class FinkConsumer(BrokerConsumer):
    _brokername = 'Fink'

    def __init__( self, server='kafka-lsst.fink-broker.org:24499', groupid=None, **kwargs ):
        super().__init__( server, groupid, schemaless=False, schema_in_key=True, **kwargs )
        self.logger.info( f"Fink group id is {groupid}" )


# ======================================================================

class AMPELConsumer(BrokerConsumer):
    _brokername = "AMPEL"

    def __init__( self, server='kafka.scimma.org', groupid=None,
                  username=None, password=None,
                  usernamefile="/secrets/scimma_username", passwordfile="/screts/scimma_password",
                  **kwargs ):

        extraconfig = {}
        if 'extraconfig' in kwargs:
            extraconfig = kwargs[ 'extraconfig' ]
            del kwargs[ 'extraconfig' ]

        if username is None:
            with open( usernamefile ) as ifp:
                username = ifp.readline().strip()
        if password is None:
            with open( passwordfile ) as ifp:
                password = ifp.readline().strip()

        if ( not isinstance( groupid, str ) ) or ( groupid[0:len(username)] != username ):
            raise ValueError( f"groupid must start with {username}" )

        extraconfig.update( { 'sasl.mechanism': 'SCRAM-SHA-512',
                              'security.protocol': 'SASL_SSL',
                              'sasl.username': username,
                              'sasl.password': password } )

        super().__init__( server, groupid, schemaless=False, extraconfig=extraconfig, **kwargs )
        self.logger.info( f"AMPEL group id is {groupid}" )


# ======================================================================
# THIS IS VESTIGAL FROM ELASTICC2.  Needs to be updated!

class AntaresConsumer(BrokerConsumer):
    _brokername = 'ANTARES'

    def __init__( self, server='kafka.antares.noirlab.edu:9092', groupid=None,
                  username=None, password=None,
                  usernamefile='/secrets/antares_username', passwordfile='/secrets/antares_passwd',
                  cafile='/fastdb/share/antares-ca.pem',
                  **kwargs ):
        extraconfig = {}
        if 'extraconfig' in kwargs:
            extraconfig = kwargs[ 'extraconfig' ]
            del kwargs[ 'extgraconfig' ]

        if username is None:
            with open( usernamefile ) as ifp:
                username = ifp.readline().strip()
        if password is None:
            with open( passwordfile ) as ifp:
                password = ifp.readline().strip()

        # Reference for the config:
        #   https://api.antares.noirlab.edu/v1/client/config/streaming/default
        #   https://gitlab.com/nsf-noirlab/csdc/antares/client/-/blob/master/antares_client/stream.py

        extraconfig = {
            # "api.version.request": True,
            # "broker.version.fallback": "0.10.0.0",
            # "api.version.fallback.ms": "0",
            "enable.auto.commit": True,
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "PLAIN",
            "sasl.username": username,
            "sasl.password": password,
            "ssl.endpoint.identification.algorithm": "none",
            "ssl.ca.location": cafile,
            "auto.offset.reset": "earliest",
        }
        super().__init__( server, groupid, schemaless=False, extraconfig=extraconfig, **kwargs )
        self.logger.info( f"Antares group id is {groupid}" )


# ======================================================================
# THIS IS VESTIGAL FROM ELASTICC2.  Needs to be updated!

class AlerceConsumer(BrokerConsumer):
    _brokername = 'alerce'

    def __init__( self,
                  grouptag=None,
                  usernamefile='/secrets/alerce_username',
                  passwdfile='/secrets/alerce_passwd',
                  loggername="ALERCE",
                  early_offset=os.getenv( "ALERCE_TOPIC_RELDATEOFFSET", -4 ),
                  alerce_topic_pattern=r'^lc_classifier_.*_(\d{4}\d{2}\d{2})$',
                  **kwargs ):
        raise RuntimeError( "Left over from ELAsTiCC2; needs to be updated." )
        server = os.getenv( "ALERCE_KAFKA_SERVER", "kafka.alerce.science:9093" )
        groupid = "elasticc-lbnl" + ( "" if grouptag is None else "-" + grouptag )
        self.early_offset = int( early_offset )
        self.alerce_topic_pattern = alerce_topic_pattern
        topics = None
        updatetopics = True
        with open( usernamefile ) as ifp:
            username = ifp.readline().strip()
        with open( passwdfile ) as ifp:
            passwd = ifp.readline().strip()
        extraconfig = {  "security.protocol": "SASL_SSL",
                         "sasl.mechanism": "SCRAM-SHA-512",
                         "sasl.username": username,
                         "sasl.password": passwd }
        super().__init__( server, groupid, topics=topics, updatetopics=updatetopics, extraconfig=extraconfig,
                          loggername=loggername, **kwargs )
        self.logger.info( f"Alerce group id is {groupid}" )

        self.badtopics = [ 'lc_classifier_balto_20230807' ]

    def update_topics( self, *args, **kwargs ):
        now = datetime.datetime.now( tz=datetime.UTC )
        datestrs = []
        for ddays in range(self.early_offset, 3):
            then = now + datetime.timedelta( days=ddays )
            datestrs.append( f"{then.year:04d}{then.month:02d}{then.day:02d}" )
        tosub = []
        topics = self.consumer.get_topics()
        for topic in topics:
            match = re.search( self.alerce_topic_pattern, topic )
            if match and ( match.group(1) in datestrs ) and ( topic not in self.badtopics ):
                tosub.append( topic )
        self.topics = tosub
        self.consumer.subscribe( self.topics )


# =====================================================================

class PittGoogleConsumer(BrokerConsumer):
    """Pitt-Google-Hernandez Broker

    cf: https://mwvgroup.github.io/pittgoogle-client/api-reference/pubsub.html

    Known topic names:
       'alerts' : full lsst alert stream
       'loop' : the same alert repeated every second, for testing
       'supernnoa'
       'upsilon'
       'variability'

    """

    _brokername = 'pitt-google'

    def __init__(
        self,
        server_not_used: str = "",
        groupid: str = "default_pittgooglebroker_fastdb_groupid",
        survey: str = "lsst",
        topic_name: str = None,
        testid: bool | str = False,
        max_workers: int = 8,  # max number of ThreadPoolExecutor workers
        loggername: str = "PITTGOOGLE",
        **kwargs
    ):
        if 'brokername_for_alerts' in kwargs:
            brokername_for_alerts = kwargs['brokername_for_alerts']
            del kwargs['brokername_for_alerts']
        else:
            brokername_for_alerts = 'Pitt-Google'

        if topic_name is None:
            raise ValueError( "Need to pass topic_name" )

        super().__init__(server=None, groupid='not_used', brokername_for_alerts=brokername_for_alerts,
                         loggername=loggername, **kwargs)

        # I would prefer it if I could pass the arguments explicitly as function arguments, but
        #  I haven't figured out how ot get that to work, and it may not work with
        #  pittgoogle.Topic.from_cloud.
        if ( ( os.getenv("GOOGLE_CLOUD_PROJECT") is None )
             or ( os.getenv("GOOGLE_APPLICATION_CREDENTIALS" ) is None )
            ):
            raise ValueError( "Need to set env vars GOOGLE_CLOUD_PROJECT and GOOGLE_APPLICATION_CREDENTIALS" )

        self._survey = survey
        self._name = topic_name
        self._testid = testid
        self._max_workers = max_workers
        self._groupid = groupid
        self.tot_n_messages_consumed = 0

    def worker_init(self):
        """Initializer for the ThreadPoolExecutor."""
        self.logger.info( "PittGoogleConsumer starting a new ThreadPoolExecutor worker to handle messages" )

    def handle_message(self, alert: pittgoogle.pubsub.Alert) -> pittgoogle.pubsub.Response:
        """Callback that will process a single message. This will run in a background thread."""

        self.logger.debug( "In handle_message" )

        # NOTE -- start reading alert.msg.data at byte 5 because the first 4 bytes
        #   are a schema ID of some sort.
        parsedalert = fastavro.schemaless_reader(io.BytesIO(alert.msg.data[5:]), self.schema)

        if self.brokername_for_alerts is not None:
            bname = self.brokername_for_alerts
        elif self.brokername_key is not None:
            bname = parsedalert[ self.brokername_key ]
        else:
            bname = self._brokername

        message = {
            "brokername": bname,
            "topic": self.topic,
            # there is no offset in pubsub
            # if this cannot be null, perhaps the message id would work?
            "msgoffset": alert.msg.message_id,
            # this is a DatetimeWithNanoseconds, a subclass of datetime.datetime
            # https://googleapis.dev/python/google-api-core/latest/helpers.html
            "timestamp": alert.msg.publish_time.astimezone(datetime.UTC),
            "savetime": datetime.datetime.now( tz=datetime.UTC ),
            "msg": parsedalert
        }

        self.logger.debug( "Returning from handle_message" )
        return pittgoogle.pubsub.Response(result=message, ack=True)


    def handle_message_batch(self, messagebatch: list) -> None:
        """Callback that will process a batch of messages. This will run in the main thread."""

        self.logger.info( f"In handle_message_batch, received {len(messagebatch)} messages" )
        t0 = time.perf_counter()
        if self.no_wrangle:
            wrangled = {}
        else:
            wrangled = self.alert_wrangler( messagebatch )
        t1 = time.perf_counter()
        nadded = self.mongodb_store( messagebatch=messagebatch, **wrangled )
        t2 = time.perf_counter()
        self.tot_n_messages_consumed += len(messagebatch)
        self.countlogger.info( f"...added {nadded} messages to mongodb collections {self.mongodb_collection_base}*\n"
                               f"    ...wrangle time: {t1-t0:.3f}\n"
                               f"    ...store time: {t2-t1:.3f}\n" )
        # ****
        self.logger.info( f"...added {nadded} messages to mongodb collections {self.mongodb_collection_base}*\n"
                          f"    ...wrangle time: {t1-t0:.3f}\n"
                          f"    ...store time: {t2-t1:.3f}\n" )
        self.logger.info( f"Total handled: {self.tot_n_messages_consumed}" )
        # ****


    def poll(self, reset=None, restart_time=None, max_restarts=None, max_msgs=None, **kwargs ):
        if len(kwargs) > 0:
            raise RuntimeError( f"Parameters unknown to PittGoogleConsumer.poll: {list(kwargs.keys())}" )
        if reset is not None:
            self.logger.warning( "reset is not known by PittGoogleConsumer.poll, ignorig it" )

        currenttotconsumed = 0
        restarts = 0
        tstart = datetime.datetime.now()
        try:
            while True:
                topic = pittgoogle.Topic.from_cloud( name=self._name,
                                                     survey=self._survey,
                                                     testid=self._testid,
                                                     projectid='pitt-alert-broker' )
                subscription = pittgoogle.Subscription( name=self._groupid,
                                                        topic=topic,
                                                        schema_name="lsst" )
                self.topic = subscription.topic.name

                # if the subscription doesn't already exist, this will create one
                subscription.touch()

                self.consumer = pittgoogle.pubsub.Consumer(
                    subscription=subscription,
                    msg_callback=self.handle_message,
                    batch_callback=self.handle_message_batch,
                    batch_maxn=self.batch_size,
                    batch_max_wait_between_messages=self.consume_timeout,
                    logger=self.countlogger,
                    executor=ThreadPoolExecutor(
                        max_workers=self._max_workers,
                        initializer=self.worker_init,
                        initargs=(),
                        # initargs=(
                        #     self,
                        #     self.schema,
                        #     subscription.topic.name,
                        #     self.logger,
                        #     self.countlogger
                        # ),
                    ),
                )

                self.countlogger.info( f"Launching a pittgoogle stream, topic={self.topic}..." )

                result = self.consumer.stream( pipe=self.pipe, heartbeat=60,
                                                  max_runtime=restart_time, max_nmsgs=max_msgs )
                currenttotconsumed += result['totprocessed']
                self.countlogger.info( f"...pittgoogle stream consumed {result['totprocessed']} messages; "
                                       f"this call to poll consumed {currenttotconsumed} messages, "
                                       f"overall {self.tot_n_messages_consumed} messages." )
                if result[ "status" ] == "die":
                    self.ogger.info( "PittGoogleConsumer.poll exiting becasue of die command." )
                    self.countlogger.info( "PittGoogleConsumer.poll exiting becasue of die command." )
                    if self.pipe is not None:
                        self.pipe.send( { "message": "died", "nconsumed": -1,
                                          "tot_handled": self.tot_n_messages_consumed,
                                          "runtime": datetime.datetime.now() - tstart } )
                    return
                elif ( max_restarts is not None ) and ( restarts >= max_restarts ):
                    self.countlogger.info( f"PittGoogleConsumer.poll exiting after {restarts} restarts." )
                    self.logger.info( f"PittGoogleConsumer.poll exiting after {restarts} restarts." )
                    if self.pipe is not None:
                        self.pipe.send( { "message": "existed", "nconsumed": -1,
                                          "tot_handled": self.tot_n_messages_consumed,
                                          "runtime": datetime.datetime.now() - tstart } )
                    return
                else:
                    self.countlogger.info( f"PittGoogleConsumer.poll restarting the stream "
                                           f"after {restarts} previous restarts..." )
                restarts += 1

        except Exception as ex:
            self.logger.error( f"Unhandled exception in PittGoogleConsumer.poll: {ex}" )
            self.countlogger.error( f"Unhandled exception in PittGoogleConsumer.poll: {ex}" )
            if self.pipe is not None:
                self.pipe.send( { "message": "unhandled exception", "nconsumed": -1,
                                  "exception": str(ex),
                                  "tot_handled": self.tot_n_messages_consumed,
                                  "runtime": datetime.datetime.now() - tstart } )
            return


class BrokerConsumerLauncher:
    """Launch a bunch of BrokerConsumer (or subclass) processes to listen to brokers.

    Make an object, and then call it as a function.  That will run them the
    BrokerConsumers subprocesses so they can all run in parallel.

    The subprocess will all send regular heartbeats back to the main process.
    If the main process doesn't hear a heartbeat from a subprocess for 5
    minutes, it will conclude that it's locked up or died, kill it, and start
    a new one.

    IMPORTANT : if you run this class directly, be aware that it futzes
    around with the signal handlers of its process, which can potentially
    screw you up.  For that reason, you may want to run
    BrokerConsumerLauncher itself in a multiprocessing subprocess; you can
    send an INT or TERM signal to it to get it to (eventually) exit.  Normal
    use of this class is from main() below.

    """

    def __init__( self, configfile, barf='', barf2=None, verbose=False, logtag=None, shutdown_graceperiod=20 ):
        """Create a BrokerConsumerLauncher.

        Parmaeters
        ----------
          configfile : Path or str
            A yaml file with a configuration of the brokers to launch.  One
            example is in tests/services/brokerconsumer.yaml.  TODO: Rob,
            make a better example with better production defaults.

          barf : str, default ''
            A string of characters that will replace the string "{barf}" on
            some of the lines of the config file.  Used in our tests; you can
            probably ignore this.

          verbose : bool, default False
            If True, show debug log messages, otherwise just info.

          logtag : str, default None
            If not None, will be added to the header part of every log messag.e

          shutdown_graceperiod : int, default 20
            When a running BrokerConsumerLauncher receives a TERM or INT
            signal, it tells all of its own subprocesses (one for each
            broker) to die, and then waits this many seconds before exiting.
            Ideally, this should be long enough that the subprocesses can be
            relied upon to finish any sleeps and clean up, but not too long
            that whatever launched the BrokerConsumerLauncher will kill it.

            The 20s default comes from: (a) BrokerConsumer.poll() by default
            has a 10s sleep timeout waiting for topics, and (b) kubernetes
            (at least no NERSC) sends a TERM and then waits 30s before
            shutting things down.  We want our shutdown messages to have a
            chance to go through, but also we want to exit before they kill
            us.

        """



        self.config = configfile
        self.barf = barf
        self.barf2 = barf if barf2 is None else barf2
        self.verbose = verbose
        self.logtag = logtag

        # This is the grace period between when the main process tells launched broker to die and
        #   when it returns.
        # I chose 20s because (a)
        self.shutdown_graceperiod=20


    @classmethod
    def _launch_broker( cls, brokerinfo ):
        # Ignore signals; the main process will tell us to die when we need to
        signal.signal( signal.SIGTERM, lambda sig, stack: True )
        signal.signal( signal.SIGINT, lambda sig, stack: True )

        bc = brokerinfo['class']( **brokerinfo['kwargs'] )
        bc.poll( **brokerinfo['pollkwargs'] )


    def __call__( self ):
        """Run the BrokerConsumerLauncher.

        IMPORTANT: Only ever run this in a subprocess, or from main() below.
        It will screw up your process' signal handlers otherwise.
        See docstring on BrokerConsumerLauncher class for more info.

        """

        logger = logging.getLogger( f"BrokerConsumerLauncher{f'-{self.logtag}' if self.logtag is not None else ''}" )
        logger.propagate = False
        if not logger.hasHandlers():
            logout = logging.StreamHandler( sys.stderr )
            logger.addHandler( logout )
            formatter = logging.Formatter( f'[%(asctime)s - {f"{self.logtag} - " if self.logtag is not None else ""}'
                                           f'%(levelname)s] - %(message)s', datefmt='%Y-%m-%d %H:%M:%S' )
            logout.setFormatter( formatter )
        else:
            logger.warning( "I am surprised, I already have handlers.  Logger is mysterious." )
        logger.setLevel( logging.DEBUG if self.verbose else logging.INFO )

        brokers = {}
        clsmap = { 'BrokerConsumer': BrokerConsumer,
                   'FinkConsumer': FinkConsumer,
                   'PittGoogleConsumer': PittGoogleConsumer }

        config = yaml.safe_load( open( self.config ) )
        # ****
        # logger.debug( f"Loaded config: {config}" )
        # ****
        if ( ( not isinstance( config, dict ) ) or
             ( 'brokers' not in config ) or
             ( not isinstance( config['brokers'], dict ) )
            ):
            raise ValueError( "Config must be a dictionary with key brokers, and config['brokers'] must be a dict." )
        missing = set( config.keys() ) - { 'brokers' }
        if len(missing) > 0:
            logger.warning( f"Unparsed root-level keys in config: {missing}" )

        for brokername, brokerinfo in config[ 'brokers' ].items():
            if not isinstance( brokerinfo, dict ):
                raise ValueError( "Each value of the brokers dict must itself be a dict" )

            # There is some parsing we have to do the yaml. *Most* of
            #   the keys are things that are passed in the kwargs to the
            #   broker class constructor, but there are a handful of
            #   things that need to be pulled out for editing, and we
            #   need to substitute {barf} and {barf2} for the barf(s)
            #   passed to the BrokerConsumerLauncher constructor (needed
            #   for tests, might also sometimes be useful in
            #   production).

            cls = clsmap[ brokerinfo['class'] ]
            name = brokername

            if 'mongodb_collection_base' not in brokerinfo:
                raise ValueError( "Every broker must have a mongodb_collection_base" )

            if ( 'extraconfigjson' in brokerinfo ) and ( brokerinfo['extraconfigjson'] is not None ):
                if ( 'extraconfig' in brokerinfo ) and ( brokerinfo['extraconfig'] is not None ):
                    raise ValueError( "Can't have non-null for both extraconfig and extraconfigjson" )
                brokerinfo['extraconfiog'] = simplejson.loads( brokerinfo['extraconfigjson'] )
                del brokerinfo['extraconfigjson']

            if ( 'extraconfig' in brokerinfo ) and ( brokerinfo['extraconfig'] is None ):
                del brokerinfo['extraconfig']

            pollkwargs = {}
            if ( 'pollkwargs' in brokerinfo ) and ( brokerinfo['pollkwargs'] is not None ):
                for k, v in brokerinfo['pollkwargs'].items():
                    if isinstance( v, str ):
                        pollkwargs[k] = v.replace( "{barf}", self.barf ).replace( "{barf2}", self.barf2 )
                    elif isinstance( v, list ):
                        pollkwargs[k] = [ i.replace("{barf}", self.barf).replace( "{barf2}", self.barf2 )
                                          if isinstance(i, str) else i
                                          for i in v ]
                    else:
                        pollkwargs[k] = v
                if 'restart_time_min' in pollkwargs:
                    if pollkwargs['restart_time_min'] is not None:
                        pollkwargs['restart_time'] = datetime.timedelta( minutes=pollkwargs['restart_time_min'] )
                    del pollkwargs['restart_time_min']

            kwargs = {}
            for k, v in brokerinfo.items():
                if k in [ 'class', 'pollkwargs']:
                    continue
                if isinstance( v, str ):
                    kwargs[k] = v.replace( "{barf}", self.barf ).replace( "{barf2}", self.barf2 )
                elif isinstance( v, list ):
                    kwargs[k] = [ i.replace("{barf}", self.barf).replace( "{barf2}", self.barf2 )
                                  if isinstance(i, str) else i
                                  for i in v ]
                else:
                    kwargs[k] = v

            brokers[name] = { 'class': cls,
                              'name': name,
                              'kwargs': kwargs,
                              'pollkwargs': pollkwargs,
                              'nfails': 0
                             }

        for broker in brokers.values():
            strio = io.StringIO()
            strio.write( f"Launching a {broker['class']}" )
            if 'server' in broker['kwargs']:
                strio.write( f" looking at server {broker['kwargs']['server']}" )
            if 'groupid' in broker['kwargs']:
                strio.write( f" with group id {broker['kwargs']['groupid']}" )
            if 'topics' in broker['kwargs']:
                strio.write( f" listening to topics {broker['kwargs']['topics']}" )
                if ( 'updatetopics' in broker['kwargs'] ) and broker['kwargs']['updatetopics']:
                    strio.write( " (will be updated)" )
            strio.write( f" saving to collections {broker['kwargs']['mongodb_collection_base']}*" )
            logger.info( strio.getvalue() )
            parentconn, childconn = multiprocessing.Pipe()
            broker['pipe'] = parentconn
            broker['kwargs']['pipe'] = childconn
            proc = multiprocessing.Process( target=self._launch_broker, args=[broker] )
            broker['process'] = proc
            proc.start()
            broker['lastheartbeat'] = time.monotonic()

        # Catch INT and TERM signals so we can try to shut down cleanly.
        self.mustdie = False

        def sigged( sig="TERM" ):
            logger.warning( f"Got a {sig} signal, trying to die." )
            self.mustdie = True

        signal.signal( signal.SIGTERM, lambda sig, stack: sigged( "TERM" ) )
        signal.signal( signal.SIGINT, lambda sig, stack: sigged( "INT" ) )

        # Listen for a heartbeat from all processes.
        # If we don't get a heartbeat for 5min, kill
        # that process and restart it.

        heartbeatwait = 2
        toolongsilent = 300
        max_n_fails = 5
        while not self.mustdie:
            try:
                pipelist = [ b['pipe'] for b in brokers.values() ]
                _whichpipe = multiprocessing.connection.wait( pipelist, timeout=heartbeatwait )
                # ****
                # logger.debug( f"broker pipe wait timed out, got: {_whichpipe}" )
                # ****

                brokerstorestart = set()
                brokerstodelete = set()
                for brokername, broker in brokers.items():
                    try:
                        while broker['pipe'].poll():
                            msg = broker['pipe'].recv()
                            if ( not isinstance( msg, dict ) ) or ( 'message' not in msg ):
                                logger.error( f"Got unexpected message from {brokername}, marking it for restart." )
                                logger.debug( str(msg) )
                                broker['nfails'] += 1
                                brokerstorestart.add( brokername )

                            elif msg['message'] == 'ok':
                                broker['lastheartbeat'] = time.monotonic()
                                logger.debug( f"Got heartbeat from {brokername}; it claims to have handled "
                                              f"a total of {msg['tot_handled']} messages "
                                              f"over {str(msg['runtime'])}" )

                            elif msg['message'] == 'too many exceptions':
                                logger.error( f"Too many exceptions for {brokername}, going to give up on it. "
                                              f"It claims to have handled {msg['tot_handled']} messages "
                                              f"over {str(msg['runtime'])}" )
                                brokerstodelete.add( brokername )

                            elif msg['message'] == 'died':
                                logger.warning( f"Consumer {brokername} claims to have received the die command.  "
                                                f"This surprises me, because I was supposed to send it, and didn't. "
                                                f"But, OK, whatever.  Removing it from the list of broker "
                                                f"consumers I'm tracking." )
                                brokerstodelete.add( brokername )

                            elif msg['message'] == 'exited':
                                logger.info( f"Consumer {brokername} exited cleanly, probably because it hit "
                                             f"the configured number of maximum internal restarts.  Remove it "
                                             f"from the list of broker consumers I'm tracking." )
                                brokerstodelete.add( brokername )

                            elif msg['messge'] == 'unhandled exception':
                                logger.error( f"Consumer {brokername} got an unhandled exception: {msg['exception']}. "
                                              f"Marking it for restart." )
                                broker['nfails'] += 1
                                brokerstorestart.add( brokername )

                            else:
                                logger.error( f"Got message '{msg['message']}' from {name}, which probably means "
                                              f"there's a coding error, because I don't undertanding it. Marking "
                                              f"the consumer for restart." )
                                broker['nfails'] += 1
                                brokerstorestart.add( brokername )

                    except Exception as ex:
                        logger.error( f"Got exception listening for heartbeat from {brokername}; will restart." )
                        logger.debug( str(ex) )
                        brokerstorestart.add( broker )

                for brokername, broker in brokers.items():
                    # ****
                    # logger.debug( f"At {time.monotonic()} broker {brokername} "
                    #               f"heartbeat = {broker['lastheartbeat']}" )
                    # ****
                    if brokername not in brokerstodelete:
                        dt = time.monotonic() - broker['lastheartbeat']
                        if dt > toolongsilent:
                            logger.error( f"It's been {dt:.0f} seconds since last heartbeat from {brokername}; "
                                          f"will restart." )
                            brokerstorestart.add( brokername )

                for todel in brokerstodelete:
                    if todel in brokers:
                        try:
                            brokers[todel]['process'].terminate()
                            brokers[todel]['process'].kill()
                            brokers[todel]['process'].close()
                            brokers[todel]['pipe'].close()
                        except Exception as ex:
                            logger.error( f"Exception trying to kill the process: {ex}" )
                        del brokers[todel]
                    if todel in brokerstorestart:
                        brokerstorestart.discard( todel )

                for brokername in brokerstorestart:
                    broker = brokers[brokername]
                    if broker['nfails'] >= max_n_fails:
                        logger.error( f"Consumer {brokername} has had {broker['nfails']} failures, "
                                      f"killing it and giving up on it instead of restarting it." )
                        try:
                            broker['process'].terminate()
                            broker['process'].kill()
                            broker['process'].close()
                            broker['pipe'].close()
                        except Exception as ex:
                            logger.error( f"Exception trying to kill the process: {ex}" )
                        del brokers[brokername]
                    else:
                        logger.warning( f"Killing and restarting process for {brokername}" )
                        try:
                            broker['process'].terminate()
                            broker['process'].kill()
                            broker['process'].close()
                            broker['pipe'].close()
                        except Exception as ex:
                            logger.error( f"Exception trying to kill the process: {ex}" )
                        parentconn, childconn = multiprocessing.Pipe()
                        broker['pipe'] = parentconn
                        broker['kwargs']['pipe'] = childconn
                        proc = multiprocessing.Process( target=self._launch_broker, args=[broker] )
                        broker['process'] = proc
                        broker['lastheartbeat'] = time.monotonic()
                        proc.start()

                if len( brokers ) == 0:
                    logger.info( "All broker consumers have exited one way or another; shutting down." )
                    self.mustdie = True

            except Exception as ex:
                logger.exception( ex )
                logger.error( "brokerconsumer main process got an exception, going to shut down" )
                self.mustdie = True

        if len(brokers) > 0:
            logger.warning( f"Shutting down.  Sending die to all remaining consumer processes and "
                            f"giving them {self.shutdown_graceperiod}s to acknowledge." )
            for broker in brokers.values():
                broker['pipe'].send( { "command": "die" } )

            tstartshutdown = time.monotonic()
            while ( len(brokers) > 0 ) and ( time.monotonic() - tstartshutdown <= self.shutdown_graceperiod ):
                pipelist = [ b['pipe'] for b in brokers.values() ]
                _whichpipe = multiprocessing.connection.wait( pipelist, 1 )
                brokerstodel = set()
                for brokername, broker in brokers.items():
                    if broker['pipe'].poll():
                        msg = broker['pipe'].recv()
                        if isinstance(msg, dict) and ( 'message' in msg ) and ( msg['message'] == 'died' ):
                            brokerstodel.add( brokername )
                        else:
                            logger.warning( f"Got a message other than 'died' from {brokername} while waiting for "
                                            f"them all to shut down; continuing to listen for 'died'." )
                for brokername in brokerstodel:
                    broker['process'].join()
                    broker['process'].close()
                    broker['pipe'].close()
                    del brokers[brokername]

            if len( brokers ) > 0:
                logger.warning( f'{len(brokers)} consumers never responded to the die command.  Killing them and '
                                f'waiting {self.shutdown_graceperiod}s to give them one last chance to clean up' )
                for brokername, broker in brokers.items():
                    try:
                        broker['process'].terminate()
                        broker['process'].kill()
                    except Exception as ex:
                        logger.error( f"Exception trying to kill the process for {brokername}: {ex}" )
                time.sleep( self.shutdown_graceperiod )
                for broker in brokers.values():
                    try:
                        broker['process'].close()
                        broker['pipe'].close()
                    except Exception as ex:
                        logger.error( f"Exception trying to clean up after {brokername}: {ex}" )

        logger.info( "Exiting BrokerConsumerLauncher." )
        return


# ======================================================================
def main():
    parser = argparse.ArgumentParser( 'brokerconsumer',
                                      description="Listen to broker streams and save broker messages",
                                      formatter_class=argparse.ArgumentDefaultsHelpFormatter )
    parser.add_argument( 'config', help='YAML file with config of brokers to listen to' )
    parser.add_argument( '-b', '--barf', default='abcdef',
                         help=( "String of random characters for group and topic names.  (Used in tests.)"
                                "Will have no effect if you never put {barf} in your config file." ) )
    parser.add_argument( '-v', '--verbose', default=False, action='store_true',
                         help="Show a few more log messages in the main process." )
    args = parser.parse_args()

    mongodb_host = os.getenv( "MONGODB_HOST" )
    mongodb_dbname = os.getenv( "MONGODB_DBNAME" )
    mongodb_user = os.getenv( "MONGODB_ALERT_WRITER_USER" )
    mongodb_password = os.getenv( "MONGODB_ALERT_WRITER_PASSWD" )
    if any ( i is None for i in [ mongodb_host, mongodb_dbname, mongodb_user, mongodb_password ] ):
        raise ValueError( "Must set all the following env vars: MONGODB_HOST, MONGODB_DBNAME, "
                          "MONGODB_ALERT_WRITER_USER, MONGODB_ALERT_WRITER_PASSWD" )

    bcl = BrokerConsumerLauncher( args.config, barf=args.barf, verbose=args.verbose )
    bcl()


# ======================================================================
if __name__ == "__main__":
    main()
