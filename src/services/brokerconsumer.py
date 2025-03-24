import sys
import os
import io
import re
import time
import datetime
import traceback
import pathlib
import urllib
import logging

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

    LOGGING : logs to two different places.  ROB TODO.

    """

    def __init__( self, server, groupid, topics=None, updatetopics=False, extraconfig={},
                  schemaless=True, schemafile=None, pipe=None, loggername="BROKER", loggername_prefix='',
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

          scheamless : bool, default True
            If True, expecting schemaless avro messages.  If False,
            expecting embedded schema.  Ignored if you pass a handler to
            poll.  Currently can't handle False.

          schemafile : Path or str, default ...
            Only used if you poke inside this object and use its
            internal variables.  Your handler will need schema, but this
            class usually doesn't.  So, don't worry about it too much.
            (Defaults to the BrokerMessage schema at the location it lives
            in the docker environment we usually run in.)

          pipe : multiprocessing.Pipe or None
            If not None, a call to poll will regularly send hearbeats to
            this Pipe.  It will also poll the pipe for messages.
            (Currentl;y ,the only message it will handle is a request to
            die.)

          loggername : str, default "BROKER"
            Used in creating log files and in headers of log messages

          loggername_prefix : str, default ""
            Used in headers of log messages

          mongodb_host : str, default $MONGOHOST
            The host where Mongo is running

          mongodb_dbname : None
            Required.  The name of the mongodb running on the mongo server

          mongodb_collection : None
            Required.  The collection to write alerts to in the mongo database.

          mongodb_user : str, default $MONGODB_ALERT_WRITER
            Username that can write alerts to mongodb_collection

          mongodb_password : str, default $MONGODB_ALERT_WRITER_PASSWORD
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

        self.countlogger.info( f"************ Starting Brokerconsumer for {loggername} ****************" )

        self.pipe = pipe
        self.server = server
        self.groupid = groupid
        self.topics = topics
        self._updatetopics = updatetopics
        self.extraconfig = extraconfig

        self.schemaless = schemaless
        if not self.schemaless:
            self.countlogger.error( "CRASHING.  I only know how to handle schemaless streams." )
            raise RuntimeError( "I only know how to handle schemaless streams" )
        self.schemafile = schemafile

        self.nmessagesconsumed = 0

        if ( mongodb_dbname is None ) or ( mongodb_collection ) is None:
            raise RuntimeError( "mongodb_dbname and mongodb_collection are required" )
        self.mongodb_dbname = mongodb_dbname
        self.mongodb_collection = mongodb_collection
        self.mongohost = mongodb_host if mongodb_host is not None else os.getenv( "MONGOHOST" )
        if self.mongohost is None:
            raise ValueError( "Error, must specify mongodb_host or set env var MONGOHOST" )
        self.mongousername = mongodb_user if mongodb_user is not None else os.getenv( "MONGODB_ALERT_WRITER" )
        if self.mongousername is None:
            raise ValueError( "Error, must specify mongodb_user or set env var MONGODB_ALERT_WRITER" )
        self.mongopassword = ( mongodb_password if mongodb_password is not None
                               else os.getenv( "MONGODB_ALERT_WRITER_PASSWORD" ) )
        if self.mongopassword is None:
            raise ValueError( "Error, must specify mongodb_password or set env var MONGODB_ALERT_WRITER_PASSWORD" )

        self.mongohost = urllib.parse.quote_plus( self.mongohost )
        self.mongodb_dbname = urllib.parse_quote_plus( self.mongodb_name )
        self.mongodb_collection = urllib.parse_quote_plus( self.mongodb_collection )
        self.monogousername = urllib.parse.quote_plus( self.mongousername )
        self.mongopassword = urllib.parse.quote_plus( self.mongopassword )

        self.logger.info( f"Writing broker messages to monogdb {self.mongodb_dbname} "
                          f"collection {self.mongodb_collection}" )


    def create_connection( self, reset=False ):
        countdown = 5
        if self._reset:
            self.countlogger.info( "*************** Resetting to start of broker kafka stream ***************" )
        else:
            self.countlogger.info( "*************** Connecting to kafka stream without reset  ***************" )
        while countdown >= 0:
            try:
                self.consumer = KafkaConsumer( self.server, self.groupid, self.schemafile,
                                               self.topics, reset=reset,
                                               extraconsumerconfig=self.extraconfig,
                                               consume_nmsgs=1000, consume_timeout=1, nomsg_sleeptime=5,
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
                                   'msg': alert } )

        nadded = self.mongodb_store( messagebatch )
        self.countlogger.info( f"...added {nadded} messages to mongodb {self.mongodb_dbname} "
                               f"collection {self.mongodb_collection}" )


    def mongodb_store(self, messagebatch=None):
        if messagebatch is None:
            return 0
        connstr = ( f"mongodb://{self.mongousername}:{self.mongopassword}@{self.mongohost}:27017/"
                    f"?authSource={self.mongodb_dbname}" )
        self.logger.debug( f"mongodb connection string {connstr}" )
        client = MongoClient( connstr )
        db = getattr( client, self.mongodb_dbname )
        collection = db[ self.mongodb_collection ]
        results = collection.insert_many( messagebatch )
        return len( results.inserted_ids )


    def poll( self, reset=False, restart_time=datetime.timedelta(minutes=30) ):
        """Poll the server, saving consumed messages to the Mongo DB.

        Will keep running indefinitely.

        Parameters
        ----------
          reset: bool, default False
            If True, reset the topics the first time we connect to the
            server.  Usually you want to pick up where you left off (as
            specified by the groupid you gave at object construction.)

          restart_time: datetime.timedelta
            Only query the kafka server for this long before closing and
            reopening the connection.  This is just a standard "turn it
            off and back on" cruft-cleaning mechanism.

        """


        self.create_connection( reset )
        while True:
            if self._updatetopics:
                self.update_topics()
            strio = io.StringIO("")
            if len(self.consumer.topics) == 0:
                self.logger.info( "No topics, will wait 10s and reconnect." )
                time.sleep(10)
            else:
                self.logger.info( f"Subscribed to topics: {self.consumer.topics}; starting poll loop." )
                self.countlogger.info( f"Subscribed to topics: {self.consumer.topics}; starting poll loop." )
                try:
                    happy = self.consumer.poll_loop( handler=self.handle_message_batch,
                                                     max_consumed=None, max_runtime=restart_time,
                                                     pipe=self.pipe )
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

            if self.pipe.poll():
                msg = self.pipe.recv()
                if ( 'command' in msg ) and ( msg['command'] == 'die' ):
                    self.logger.info( "No topics, but also exiting broker poll due to die command." )
                    self.countlogger.info( "No topics, but also existing broker poll due to die command." )
                    self.close_connection()
                    return
            strio.write( "Reconnecting to server.\n" )
            self.logger.info( strio.getvalue() )
            self.countlogger.info( strio.getvalue() )
            # Only want to reset the at most first time we connect!  If
            # we disconnect and reconnect in the loop below, we want to
            # pick up where we left off.
            self.close_connection( reset=False )
            if self._updatetopics:
                self.topics = None
            self.create_connection()


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

    """Initializer for the ThreadPoolExecutor."""
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
