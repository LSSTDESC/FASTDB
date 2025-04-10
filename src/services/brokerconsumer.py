import sys
import os
import io
import re
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

import confluent_kafka
import fastavro
from pymongo import MongoClient

from kafka_consumer import KafkaConsumer

# TODO : uncomment this next line
#   and the whole PittGoogleBroker class
#   when pittgoogle works again
# from concurrent.futures import ThreadPoolExecutor  # for pittgoogle
# import pittgoogle

_rundir = pathlib.Path(__file__).parent
_logdir = pathlib.Path( os.getenv( 'LOGDIR', '/logs' ) )


class BrokerConsumer:
    """A class for consuming broker messages from brokers.

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

    def __init__( self, server, groupid, topics=None, updatetopics=False, extraconfig={},
                  schemaless=True, schemafile=None, pipe=None, loggername="BROKER", loggername_prefix='',
                  nomsg_sleeptime=5,
                  mongodb_host=None, mongodb_dbname=None, mongodb_collection=None,
                  mongodb_user=None, mongodb_password=None ):
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

          schemaless : bool, default True
            If True, expecting schemaless avro messages.  If False,
            expecting embedded schema.  Ignored if you pass a handler to
            poll.  Currently can't handle False.

          schemafile : Path or str
            The .avsc the that holds the schema of the messsages we'll be
            ingesting.  Required if schemaless is True (which, right now, it
            has to be).  The schema must be named properly for its namespace,
            and any other schema in the same namespace referred to by that
            .avsc file must be in the same directory with the right names.  If
            not given, uses the location where it is find in the docker image
            we use.

          pipe : multiprocessing.Pipe or None
            If not None, a call to poll will regularly send hearbeats to
            this Pipe.  It will also poll the pipe for messages.
            (Currentl;y ,the only message it will handle is a request to
            die.)

          loggername : str, default "BROKER"
            Used in creating log files and in headers of log messages

          loggername_prefix : str, default ""
            Used in headers of log messages

          nomsg_sleeptime : int, default 5
            The KafkaConsumer (src/kafkaconsumer.py) will sleep this
            many seconds between not finding any new messages and
            polling again to ask for new messages.

          mongodb_host : str, default $MONGODB_HOST
            The host where Mongo is running

          mongodb_dbname : str, default $MONGODB_DBNAME
            The database name

          mongodb_collection : str, default $MONGODB_DEFAULT_COLLECTION
            The collection

          mongodb_user : str, default $MONGODB_ALERT_WRITER_USER
            Username that can write alerts to mongodb_collection

          mongodb_password : str, default $MONGODB_ALERT_WRITER_PASSWD
            Password for mongodb_user.

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
            schemafile = "/fastdb/share/avsc/fastdb_test_0.1.BrokerMessage.avsc"

        self.countlogger.info( f"************ Starting BrokerConsumer for {loggername} ****************" )

        self.pipe = pipe
        self.server = server
        self.groupid = groupid
        self.topics = topics
        self._updatetopics = updatetopics
        self.extraconfig = extraconfig
        self.nomsg_sleeptime = nomsg_sleeptime

        self.schemaless = schemaless
        if not self.schemaless:
            self.countlogger.error( "CRASHING.  I only know how to handle schemaless streams." )
            raise RuntimeError( "I only know how to handle schemaless streams" )
        self.schemafile = schemafile
        self.schema = fastavro.schema.load_schema( self.schemafile )

        self.nmessagesconsumed = 0

        mongoconfigs = [ ( 'mongodb_host', mongodb_host, 'MONGODB_HOST' ),
                         ( 'mongodb_dbname', mongodb_dbname, 'MONGODB_DBNAME' ),
                         ( 'mongodb_collection', mongodb_collection, 'MONGODB_DEFAULT_COLLECTION' ),
                         ( 'mongodb_user', mongodb_user, 'MONGODB_ALERT_WRITER_USER' ),
                         ( 'mongodb_password', mongodb_password, 'MONGODB_ALERT_WRITER_PASSWD' ) ]
        missing = []
        for mc in mongoconfigs:
            setattr( self, mc[0], mc[1] if mc[1] is not None else os.getenv( mc[2] ) )
            if getattr( self, mc[0] ) is None:
                missing.append( mc )
            else:
                setattr( self, mc[0], urllib.parse.quote_plus( getattr( self, mc[0] ) ) )
        if len( missing ) > 0:
            strio = io.StringIO()
            strio.write( "Must provide all of:\n" )
            for mc in mongoconfigs:
                strio.write( f"    * {mc[0]}, or set env var {mc[2]}\n" )
            strio.write( f"Missing: {','.join( [ mc[0] for mc in missing ] ) }" )
            self.logger.error( strio.getvalue () )
            raise ValueError( "Incomplete mongo config" )

        self.logger.info( f"Writing broker messages to monogdb {self.mongodb_dbname} "
                          f"collection {self.mongodb_collection}" )


    def create_connection( self, reset=False ):
        countdown = 5
        if reset:
            self.countlogger.info( "*************** Resetting to start of broker kafka stream ***************" )
        else:
            self.countlogger.info( "*************** Connecting to kafka stream without reset  ***************" )
        while countdown >= 0:
            try:
                self.consumer = KafkaConsumer( self.server, self.groupid, self.schemafile,
                                               self.topics, reset=reset,
                                               extraconsumerconfig=self.extraconfig,
                                               consume_nmsgs=1000, consume_timeout=1,
                                               nomsg_sleeptime=self.nomsg_sleeptime,
                                               logger=self.logger )
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
        self.countlogger.info( "**************** Closing consumer connection ******************" )
        self.consumer.close()
        self.consumer = None

    def update_topics( self, *args, **kwargs ):
        self.countlogger.info( "Subclass must implement this if you use it." )
        raise NotImplementedError( "Subclass must implement this if you use it." )

    def reset_to_start( self ):
        raise RuntimeError( "This is probably broken" )
        self.logger.info( "Resetting all topics to start" )
        for topic in self.topics:
            self.consumer.reset_to_start( topic )

    def handle_message_batch( self, msgs ):
        messagebatch = []
        self.countlogger.info( f"Handling {len(msgs)} messages; consumer has received "
                               f"{self.consumer.tot_handled} messages." )
        now = datetime.datetime.now( tz=datetime.UTC )
        for msg in msgs:
            timestamptype, timestamp = msg.timestamp()

            if timestamptype == confluent_kafka.TIMESTAMP_NOT_AVAILABLE:
                timestamp = None
            else:
                timestamp = datetime.datetime.fromtimestamp( timestamp / 1000, tz=datetime.UTC )

            payload = msg.value()
            if not self.schemaless:
                self.countlogger.error( "I only know how to handle schemaless streams" )
                raise RuntimeError( "I only know how to handle schemaless streams" )
            alert = fastavro.schemaless_reader( io.BytesIO( payload ), self.schema )
            messagebatch.append( { 'topic': msg.topic(),
                                   'msgoffset': msg.offset(),
                                   'timestamp': timestamp,
                                   'savetime': now,
                                   'msg': alert } )

        nadded = self.mongodb_store( messagebatch )
        self.countlogger.info( f"...added {nadded} messages to mongodb {self.mongodb_dbname} "
                               f"collection {self.mongodb_collection}" )


    def mongodb_store(self, messagebatch=None):
        if messagebatch is None:
            return 0
        connstr = ( f"mongodb://{self.mongodb_user}:{self.mongodb_password}@{self.mongodb_host}:27017/"
                    f"?authSource={self.mongodb_dbname}" )
        self.logger.debug( f"mongodb connection string {connstr}" )
        client = MongoClient( connstr )
        db = getattr( client, self.mongodb_dbname )
        collection = db[ self.mongodb_collection ]
        results = collection.insert_many( messagebatch )
        return len( results.inserted_ids )


    def poll( self, reset=False, restart_time=datetime.timedelta(minutes=30),
              notopic_sleeptime=300, max_restarts=None ):
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

        TODO : separate max_restarts from polling from max_restarts from
        topic not existing, because the timeouts for the two are likely very
        different.

        """


        self.create_connection( reset )
        n_restarts = 0
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
                                                     stopafternmessages=None, stopafternsleeps=None )
                    if happy:
                        strio.write( f"Reached poll timeout for {self.server}; "
                                     f"handled {self.consumer.tot_handled} messages. " )
                    else:
                        strio.write( f"Poll loop received die command after handling "
                                     f"{self.consumer.tot_handled} messages.  Exiting." )
                        self.logger.info( strio.getvalue() )
                        self.countlogger.info( strio.getvalue() )
                        self.close_connection()
                        return

                except Exception as e:
                    otherstrio = io.StringIO("")
                    traceback.print_exc( file=otherstrio )
                    self.logger.warning( otherstrio.getvalue() )
                    strio.write( f"Exception polling: {str(e)}. " )

            if ( self.pipe is not None ) and ( self.pipe.poll() ):
                msg = self.pipe.recv()
                if ( 'command' in msg ) and ( msg['command'] == 'die' ):
                    if len( strio.getvalue() ) > 0:
                        self.logger.info( strio.getvalue() )
                        self.countlogger.info( strio.getvalue() )
                    self.logger.info( "Exiting broker poll due to die command." )
                    self.countlogger.info( "Exiting broker poll due to die command." )
                    self.close_connection()
                    return

            if ( max_restarts is not None ) and ( n_restarts >= max_restarts ):
                strio.write( f"Exiting after {n_restarts} restarts." )
                self.logger.info( strio.getvalue() )
                self.countlogger.info( strio.getvalue() )
                self.close_connection()
                return

            strio.write( "Reconnecting to server.\n" )
            self.logger.info( strio.getvalue() )
            self.countlogger.info( strio.getvalue() )
            self.close_connection()
            # TODO : think about automatic topic updating
            # if self._updatetopics:
            #     self.consumer.topics = None
            # Only want to reset the at most first time we connect!  If
            # we disconnect and reconnect in the loop below, we want to
            # pick up where we left off.
            self.create_connection( reset=False )
            n_restarts += 1


# ======================================================================
# THIS IS VESTIGAL FROM ELASTICC2.  Needs to be updated!

class AntaresConsumer(BrokerConsumer):
    _brokername = 'antares'

    def __init__( self, grouptag=None,
                  usernamefile='/secrets/antares_username', passwdfile='/secrets/antares_passwd',
                  loggername="ANTARES", antares_topic='elasticc2-st1-ddf-full', **kwargs ):
        raise RuntimeError( "Left over from ELAsTiCC2; needs to be updated." )
        server = "kafka.antares.noirlab.edu:9092"
        groupid = "elasticc-lbnl" + ( "" if grouptag is None else "-" + grouptag )
        topics = [ antares_topic ]
        updatetopics = False
        with open( usernamefile ) as ifp:
            username = ifp.readline().strip()
        with open( passwdfile ) as ifp:
            passwd = ifp.readline().strip()
        extraconfig = {
            "api.version.request": True,
            "broker.version.fallback": "0.10.0.0",
            "api.version.fallback.ms": "0",
            "enable.auto.commit": True,
            "security.protocol": "SASL_SSL",
            "sasl.mechanism": "PLAIN",
            "sasl.username": username,
            "sasl.password": passwd,
            "ssl.ca.location": str( _rundir / "antares-ca.pem" ),
            "auto.offset.reset": "earliest",
        }
        super().__init__( server, groupid, topics=topics, updatetopics=updatetopics,
                          extraconfig=extraconfig, loggername=loggername, **kwargs )
        self.logger.info( f"Antares group id is {groupid}" )


# ======================================================================
# THIS IS VESTIGAL FROM ELASTICC2.  Needs to be updated!

class FinkConsumer(BrokerConsumer):
    _brokername = 'fink'

    def __init__( self, grouptag=None, loggername="FINK", fink_topic='fink_elasticc-2022fall', **kwargs ):
        raise RuntimeError( "Left over from ELAsTiCC2; needs to be updated." )
        server = "134.158.74.95:24499"
        groupid = "elasticc-lbnl" + ( "" if grouptag is None else "-" + grouptag )
        topics = [ fink_topic ]
        updatetopics = False
        super().__init__( server, groupid, topics=topics, updatetopics=updatetopics,
                          loggername=loggername, **kwargs )
        self.logger.info( f"Fink group id is {groupid}" )


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
        now = datetime.datetime.now()
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
# MAKE SURE TO UPDATE WHAT'S BELOW TO MATCH CHANGES TO BrokerConsumer
# AS WELL AS WHAT'S NEEDED FOR Pitt-Google

# class PittGoogleBroker(BrokerConsumer):
#     _brokername = 'pitt-google'
#
#     def __init__(
#         self,
#         pitt_topic: str,
#         pitt_project: str,
#         max_workers: int = 8,  # max number of ThreadPoolExecutor workers
#         batch_maxn: int = 1000,  # max number of messages in a batch
#         batch_maxwait: int = 5,  # max seconds to wait between messages before processing a batch
#         loggername: str = "PITTGOOGLE",
#         **kwargs
#     ):
#         super().__init__(server=None, groupid=None, loggername=loggername, **kwargs)

#         topic = pittgoogle.pubsub.Topic(pitt_topic, pitt_project)
#         subscription = pittgoogle.pubsub.Subscription(name=f"{pitt_topic}-desc", topic=topic)
#         # if the subscription doesn't already exist, this will create one in the
#         # project given by the env var GOOGLE_CLOUD_PROJECT
#         subscription.touch()

#         self.consumer = pittgoogle.pubsub.Consumer(
#             subscription=subscription,
#             msg_callback=self.handle_message,
#             batch_callback=self.handle_message_batch,
#             batch_maxn=batch_maxn,
#             batch_maxwait=batch_maxwait,
#             executor=ThreadPoolExecutor(
#                 max_workers=max_workers,
#                 initializer=self.worker_init,
#                 initargs=(
#                     self.schema,
#                     subscription.topic.name,
#                     self.logger,
#                     self.countlogger
#                 ),
#             ),
#         )

#     @staticmethod
#     def worker_init(classification_schema: dict, pubsub_topic: str,
#                     broker_logger: logging.Logger, broker_countlogger: logging.Logger ):
#

#    """Initializer for the ThreadPoolExecutor."""
#         global countlogger
#         global logger
#         global schema
#         global topic

#         countlogger = broker_countlogger
#         logger = broker_logger
#         schema = classification_schema
#         topic = pubsub_topic

#         logger.info( "In worker_init" )

#     @staticmethod
#     def handle_message(alert: pittgoogle.pubsub.Alert) -> pittgoogle.pubsub.Response:
#         """Callback that will process a single message. This will run in a background thread."""
#         global logger
#         global schema
#         global topic

#         logger.info( "In handle_message" )

#         message = {
#             "msg": fastavro.schemaless_reader(io.BytesIO(alert.bytes), schema),
#             "topic": topic,
#             # this is a DatetimeWithNanoseconds, a subclass of datetime.datetime
#             # https://googleapis.dev/python/google-api-core/latest/helpers.html
#             "timestamp": alert.metadata["publish_time"].astimezone(datetime.timezone.utc),
#             # there is no offset in pubsub
#             # if this cannot be null, perhaps the message id would work?
#             "msgoffset": alert.metadata["message_id"],
#         }

#         return pittgoogle.pubsub.Response(result=message, ack=True)

#     @staticmethod
#     def handle_message_batch(messagebatch: list) -> None:
#         """Callback that will process a batch of messages. This will run in the main thread."""
#         global logger
#         global countlogger

#         logger.info( "In handle_message_batch" )
#         # import pdb; pdb.set_trace()

#         added = BrokerMessage.load_batch(messagebatch, logger=logger)
#         countlogger.info(
#             f"...added {added['addedmsgs']} messages, "
#             f"{added['addedclassifiers']} classifiers, "
#             f"{added['addedclassifications']} classifications. "
#         )

#     def poll(self):
#         # this blocks indefinitely or until a fatal error
#         # use Control-C to exit
#         self.consumer.stream( pipe=self.pipe, heartbeat=60 )


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

    def __init__( self, configfile, barf='', verbose=False, logtag=None, shutdown_graceperiod=20 ):
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
        self.verbose = verbose
        self.logtag = logtag

        # This is the grace period between when the main process tells launched broker to die and
        #   when it returns.
        # I chose 20s because (a)
        self.shutdown_graceperiod=20


    def _launch_broker( self, brokerinfo ):
        # Ignore signals; the main process will tell us to die when we need to
        signal.signal( signal.SIGTERM, lambda sig, stack: True )
        signal.signal( signal.SIGINT, lambda sig, stack: True )

        bc = brokerinfo['class']( brokerinfo['server'],
                                  brokerinfo['groupid'],
                                  topics=brokerinfo['topics'],
                                  updatetopics=brokerinfo['updatetopics'],
                                  extraconfig=brokerinfo['extraconfig'],
                                  pipe=brokerinfo['childpipe'],
                                  mongodb_collection=brokerinfo['collection'],
                                  loggername=brokerinfo['loggername'],
                                  loggername_prefix=brokerinfo['loggername_prefix']
                                 )
        bc.poll( restart_time=brokerinfo['restart_time'],
                 max_restarts=brokerinfo['max_restarts'],
                 notopic_sleeptime=brokerinfo['notopic_sleeptime']
                )


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

        config = yaml.safe_load( open( self.config ) )
        # ****
        # logger.debug( f"Loaded config: {config}" )
        # ****

        schemafile = config[ 'schemafile' ]

        brokers = []
        clsmap = { 'BrokerConsumer': BrokerConsumer }

        # Parse the config for all brokers before launching anything, so that if we get an exception
        #   we won't have started subprocesses.
        for broker in config[ 'brokers' ]:
            cls = clsmap[ broker['class'] ]
            name = broker['name']
            server = broker['server'].replace( "{barf}", self.barf )
            topics = [ t.replace("{barf}", self.barf) for t in broker['topics'] ]
            groupid = broker['groupid'].replace( "{barf}", self.barf )
            collection = broker['collection'].replace( "{barf}", self.barf ) if 'collection' in broker else None
            loggername = broker['loggername'].replace( "{barf}", self.barf )
            loggername_prefix = broker['loggername_prefix'].replace( "{barf}", self.barf )
            schm = schemafile if 'schemafile' not in broker else broker['schemafile']
            updatetopics = False if 'updatetopics' not in broker else broker['updatetopics']
            restart_time = datetime.timedelta( minutes=(broker['restart_time_min'] if 'restart_time_min' in broker
                                                        else 30 ) )
            max_restarts = broker['max_restarts'] if 'max_restarts' in broker else None
            notopic_sleeptime = broker['notopic_sleeptime_sec'] if 'notopic_sleeptime_sec' in broker else 10
            extraconfig = {} if 'extraconfig' not in broker else broker['extraconfig']
            brokerinfo = { 'class': cls,
                           'name': name,
                           'server': server,
                           'topics': topics,
                           'groupid': groupid,
                           'schemafile': schm,
                           'updatetopics': updatetopics,
                           'restart_time': restart_time,
                           'max_restarts': max_restarts,
                           'notopic_sleeptime': notopic_sleeptime,
                           'extraconfig': extraconfig,
                           'collection': collection,
                           'loggername': loggername,
                           'loggername_prefix': loggername_prefix }
            brokers.append( brokerinfo )

        for broker in brokers:
            logger.info( f"Launching a {broker['class']} looking at server {broker['server']} "
                         f"with group id {broker['groupid']} listening to topics {broker['topics']}"
                         f"{' (will be updated)' if updatetopics else ''}, "
                         f"saving to collection {broker['collection']}" )
            parentconn, childconn = multiprocessing.Pipe()
            broker['pipe'] = parentconn
            broker['childpipe'] = childconn
            proc = multiprocessing.Process( target=lambda: self._launch_broker( brokerinfo ) )
            broker['process'] = proc
            broker['lastheartbeat'] = time.monotonic()
            proc.start()

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
        while not self.mustdie:
            try:
                pipelist = [ b['pipe'] for b in brokers ]
                _whichpipe = multiprocessing.connection.wait( pipelist, timeout=heartbeatwait )
                # ****
                # logger.debug( f"broker pipe wait timed out, got: {_whichpipe}" )
                # ****

                brokerstorestart = set()
                for broker in brokers:
                    try:
                        while broker['pipe'].poll():
                            msg = broker['pipe'].recv()
                            if ( 'message' not in msg ) or ( msg['message'] != 'ok' ):
                                logger.error( f"Got unexpected message from {broker['name']}, will restart. "
                                              f"(Message={msg}" )
                                brokerstorestart.add( broker )
                            else:
                                logger.debug( f"Got heartbeat from {broker['name']}" )
                                broker['lastheartbeat'] = time.monotonic()
                    except Exception as ex:
                        logger.error( f"Got exception listening for heartbeat from {broker['name']}; will restart." )
                        logger.debug( str(ex) )
                        brokerstorestart.add( broker )

                for broker in brokers:
                    # ****
                    # logger.debug( f"At {time.monotonic()} broker {broker['name']} "
                    #               f"heartbeat = {broker['lastheartbeat']}" )
                    # ****
                    dt = time.monotonic() - broker['lastheartbeat']
                    if dt > toolongsilent:
                        logger.error( f"It's been {dt:.0f} seconds since last heartbeat from {broker['name']}; "
                                      f"will restart." )
                        brokerstorestart.add( broker )

                for broker in brokerstorestart:
                    logger.warning( f"Killing and restarting process for {broker['name']}" )
                    broker['process'].kill()
                    broker['pipe'].close()
                    del broker['process']
                    parentconn, childconn = multiprocessing.Pipe()
                    broker['pipe'] = parentconn
                    broker['childpipe'] = childconn
                    proc = multiprocessing.Process( target=lambda: self._launch_broker( broker ) )
                    broker['process'] = proc
                    broker['lastheartbeat'] = time.monotonic()
                    proc.start()

            except Exception as ex:
                logger.exception( ex )
                logger.error( "brokerconsumer main process got an exception, going to shut down" )
                self.mustdie = True

        logger.warning( f"Shutting down.  Sending die to all processes and waiting {self.shutdown_graceperiod}s" )
        for broker in brokers:
            broker['pipe'].send( { "command": "die" } )
        time.sleep( self.shutdown_graceperiod )
        logger.warning( "Exiting" )
        return


# ======================================================================
def main():
    parser = argparse.ArgumentParser( 'brokerconsumer',
                                      description="Listen to broker streams and save broker messages",
                                      formatter_class=argparse.ArgumentDefaultsHelpFormatter )
    parser.add_argument( 'config', help='YAML file with config of brokers to listen to' )
    parser.add_argument( 'collection', default=None,
                         help="Collection in mongo database to store alerts; defaults to $MONGODB_DEFAULT_COLLECTION" )
    parser.add_argument( '-b', '--barf', default='abcdef',
                         help=( "String of random characters for group and topic names.  (Used in tests.)"
                                "Will have no effect if you never put {barf} in your config file." ) )
    parser.add_argument( '-v', '--verbose', default=False, action='store_true',
                         help="Show a few more log messages in the main process." )
    args = parser.parse_args()

    mongodb_host = os.getenv( "MONGODB_HOST" )
    mongodb_dbname = os.getenv( "MONGODB_DBNAME" )
    mongodb_collection = args.collection if args.collection is not None else os.getenv( "MONGODB_DEFAULT_COLLECTION" )
    mongodb_user = os.getenv( "MONGODB_ALERT_WRITER_USER" )
    mongodb_password = os.getenv( "MONGODB_ALERT_WRITER_PASSWD" )
    if any ( [ i is None for i in [ mongodb_host, mongodb_dbname, mongodb_collection,
                                    mongodb_user, mongodb_password ] ] ):
        raise ValueError( "Must set all the following env vars: MONGODB_HOST, MONGODB_DBNAME, MONGODB_COLLECTION, "
                          "MONGODB_ALERT_WRITER_USER, MONGODB_ALERT_WRITER_PASSWD" )

    bcl = BrokerConsumerLauncher( args.config, barf=args.barf, verbose=args.verbose )
    bcl()


# ======================================================================
if __name__ == "__main__":
    main()
