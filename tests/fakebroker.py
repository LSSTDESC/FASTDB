import sys
import io
import math
import pathlib
import logging
import argparse
import time
import datetime
import functools
import signal
import multiprocessing
import numpy
import confluent_kafka
import fastavro

from kafka_consumer import KafkaConsumer

_rundir = pathlib.Path( __file__ ).parent

# This next thing is used as a default
_schema_namespace = "fastdb_test_0.1"


# ======================================================================

class Classifier:
    def __init__( self, brokername, brokerversion, classifiername, classifierparams,
                  kafkaserver="brahms.lbl.gov:9092", topic="somebody-didnt-replace-a-default",
                  alertschema=None, brokermessageschema=None, logger=None ):

        if logger is None:
            raise ValueError( "I need a logger." )
        self.logger = logger
        self.brokername = brokername
        self.brokerversion = brokerversion
        self.classifiername = classifiername
        self.classifierparams = classifierparams
        self.kafkaserver = kafkaserver
        self.topic = topic
        self.alertschema = alertschema
        self.brokermessageschema = brokermessageschema

        self.nclassified = 0
        self.logevery = 10
        self.nextlog = self.logevery

        self.makeproducertime = 0.
        self.avreadtime = 0.
        self.classifytime = 0.
        self.determineprobstime = 0.
        self.avwritetime = 0.
        self.producetime = 0.
        self.flushtime = 0.
        self.runtime = 0.
        self.last_classify_time = None

        logger.info( f"Classifier {self.__class__.__name__} sending to topic {self.topic} on {self.kafkaserver}" )

    def determine_types_and_probabilities( self, alert ):
        """Given an alert (a dict in the format of the fastdb test simulated AP alert schema), return a list of
        two-element tuples that is (classId, probability)."""
        raise RuntimeError( "Need to implement this function in a subclass!" )


    def log_status( self ):
        strio = io.StringIO()
        strio.write( f"{self.classifiername} has classified {self.nclassified} alerts" )
        if self.last_classify_time is not None:
            strio.write( f" as of {self.last_classify_time.isoformat(sep=' ',timespec='seconds')}" )
        strio.write( "\n" )
        strio.write( f"     Runtime (discounting pulling alerts): {self.runtime:.2f}\n" )
        strio.write( f"        makeproducertime: {self.makeproducertime:.2f}\n" )
        strio.write( f"        classifytime: {self.classifytime:.2f}\n" )
        strio.write( f"           avreadtime: {self.avreadtime:.2f}\n" )
        strio.write( f"           determineprobstime: {self.determineprobstime:.2f}\n" )
        strio.write( f"           avwritetime: {self.avwritetime:.2f}\n" )
        strio.write( f"           producetime: {self.producetime:.2f}\n" )
        strio.write( f"        flushtime: {self.flushtime:.2f}" )
        self.logger.info( strio.getvalue() )


    def classify_alerts( self, messages ):
        t0 = time.perf_counter()
        producer = confluent_kafka.Producer( { 'bootstrap.servers': self.kafkaserver,
                                               'batch.size': 131072,
                                               'linger.ms': 50 } )
        t1 = time.perf_counter()
        for msg in messages:
            t2 = time.perf_counter()
            alert = fastavro.schemaless_reader( io.BytesIO(msg), self.alertschema )
            alert['classifications'] = []
            t3 = time.perf_counter()
            probs = self.determine_types_and_probabilities( alert )
            for prob in probs:
                alert['classifications'].append( { "classId": prob[0],
                                                   "probability": prob[1] } )
            t4 = time.perf_counter()
            outdata = io.BytesIO()
            fastavro.write.schemaless_writer( outdata, self.brokermessageschema, alert )
            t5 = time.perf_counter()
            producer.produce( self.topic, outdata.getvalue() )
            t6 = time.perf_counter()

            self.avreadtime += t3 - t2
            self.determineprobstime += t4 - t3
            self.avwritetime += t5 - t4
            self.producetime += t6 - t5

        t7 = time.perf_counter()
        producer.flush()
        t8 = time.perf_counter()

        self.makeproducertime += t1 - t0
        self.classifytime += t7 - t1
        self.flushtime += t8 - t7
        self.runtime += t8 - t0
        self.last_classify_time = datetime.datetime.now()

        self.nclassified += len(messages)
        if ( self.nclassified > self.nextlog ):
            self.log_status()
            self.nextlog = self.logevery * ( math.floor( self.nclassified / self.logevery ) + 1 )


# ======================================================================

class NugentClassifier(Classifier):
    def __init__( self, *args, **kwargs ):
        super().__init__( "FakeBroker", "v1.0", "NugentClassifier", "100%", **kwargs )

    def determine_types_and_probabilities( self, alert ):
        return [ ( 2222, 1.0 ) ]


# ======================================================================

class RandomSNType(Classifier):
    def __init__( self, *args, **kwargs ):
        super().__init__( "FakeBroker", "v1.0", "RandomSNType", "Perfect", **kwargs )
        self.rng = numpy.random.default_rng()

    def determine_types_and_probabilities( self, alert ):
        totprob = 0.
        types = [ 2222, 2223, 2224, 2225, 2226,
                  2232, 2233, 2234, 2235,
                  2243, 2244, 2245, 2246,
                  2322, 2323, 2324, 2325, 2326,
                  2332 ]
        retval = []
        self.rng.shuffle( types )
        ranprobs = self.rng.random( len(types) )
        for prob, sntype in zip( ranprobs, types ):
            # I am not clever enough to come up with a single numpy
            #   operation to perform this calculation without a for
            #   loop.  Even though it made me feel dirty, I asked
            #   chatgpt... and what it gave me was wrong.  (The total
            #   probability did not sum to 1.)  That made me feel good,
            #   because there's nothing like the dopamine hit of having
            #   your biases superficially confirmed.
            # I tried CBorg, the AI portal thingy that LBNL has and it
            #   wrote a whole long mess that gave the wrong answer, but
            #   recognized it was wrong (wrapping the code in comments
            #   about "for educational purposes"), and ended up just
            #   saying I had to use my for loop.  (WELL!  It thought it
            #   was helping by suggesting I put the for loop in a
            #   function.  Great.  Thanks.)
            thisprob = prob * ( 1. - totprob )
            totprob += thisprob
            retval.append( ( sntype, thisprob ) )
        # SLSN seems to be the default type....
        retval.append( ( 2242, 1.-totprob ) )
        return retval


# ======================================================================

class FakeBroker:
    def __init__( self,
                  source,
                  source_topics,
                  dest,
                  dest_topic,
                  group_id="rknop-test",
                  alert_schema=f"/fastdb/share/avsc/{_schema_namespace}.Alert.avsc",
                  brokermessage_schema=f"/fastdb/share/avsc/{_schema_namespace}.BrokerMessage.avsc",
                  runtime=datetime.timedelta(minutes=10),
                  consume_nmsgs=1000,
                  notopic_sleeptime=10,
                  reset=False,
                  verbose=False ):
        self.logger = logging.getLogger( "fakebroker" )
        self.logger.propagate = False
        if not self.logger.hasHandlers():
            _logout = logging.StreamHandler( sys.stderr )
            self.logger.addHandler( _logout )
            _formatter = logging.Formatter( '[%(asctime)s - fakebroker - %(levelname)s] - %(message)s',
                                            datefmt='%Y-%m-%d %H:%M:%S' )
            _logout.setFormatter( _formatter )
        self.logger.setLevel( logging.DEBUG if verbose else logging.INFO )

        self.source = source
        self.source_topics = source_topics
        self.dest = dest
        self.dest_topic = dest_topic
        self.group_id = group_id
        self.reset = reset
        self.runtime = runtime
        self.consume_nmsgs = consume_nmsgs
        self.notopic_sleeptime=notopic_sleeptime

        self.consumer_consume_time_offset = 0
        self.consumer_handle_time_offset = 0

        self.alert_schema = alert_schema
        alertschemaobj = fastavro.schema.parse_schema( fastavro.schema.load_schema( alert_schema ) )
        brokermsgschema = fastavro.schema.parse_schema( fastavro.schema.load_schema( brokermessage_schema ) )
        self.classifiers = [ NugentClassifier( kafkaserver=self.dest, topic=self.dest_topic,
                                               alertschema=alertschemaobj, brokermessageschema=brokermsgschema,
                                               logger=self.logger ),
                             RandomSNType(  kafkaserver=self.dest, topic=self.dest_topic,
                                            alertschema=alertschemaobj, brokermessageschema=brokermsgschema,
                                            logger=self.logger )
                            ]
        self.classifier_procs = []
        self.classifier_pipes = []

    def handle_message_batch( self, msgs ):
        # I'm a bad person, I'm not looking at error flags or anything
        msgs = [ m.value() for m in msgs ]
        for pipe in self.classifier_pipes:
            pipe.send( { "command": "handle", "msgs": msgs } )

        ndids = 0
        while ndids < len( self.classifier_pipes ):
            readies = multiprocessing.connection.wait( self.classifier_pipes )
            for ready in readies:
                msg = ready.recv()
                if msg != "did":
                    self.logger.error( f"Got unexpected message from subprocess: {msg}" )
                    self.shutdown()
                ndids += 1

    def log_cfer_status( self, consumer ):
        self.logger.info( f"FakeBroker consume time = {self.consumer_consume_time_offset+consumer.consume_time:.2f}, "
                          f"handle time = {self.consumer_handle_time_offset+consumer.handle_time:.2f}" )
        for pipe in self.classifier_pipes:
            pipe.send( { "command": "log" } )

    def classifier_runner( self, classifier, pipe ):
        done = False
        while not done:
            pipe.poll()
            msg = pipe.recv()
            if not isinstance( msg, dict ) or ( 'command' not in msg ):
                self.logger.error( f"Subprocess for {classifier.classifiername} got misunderstood message {msg}" )
                continue
            if msg['command'] == 'die':
                done = True
            elif msg['command'] == 'log':
                classifier.log_status()
            elif msg['command'] == 'handle':
                classifier.classify_alerts( msg['msgs'] )
                pipe.send( "did" )
            else:
                self.logger.error( f"Subprocess for {classifier.classifiername} got unknown command "
                                   f"{msg['command']}" )
        self.logger.info( f"Subprocess for {classifier.classifiername} exiting." )

    def shutdown( self ):
        self.logger.info( "FakeBroker shutting down." )
        for pipe in self.classifier_pipes:
            pipe.send( { "command": "die" } )
        for proc in self.classifier_procs:
            proc.join()
        self.classifier_pipes = []
        self.classifier_procs = []
        self.logger.info( "Fakebroker done." )
        # This is a little bit scary, because the kafka loop might
        #   be in the middle of consuming messages.  Scary athread
        #   stuff.  Maybe I need more subproesses within
        #   subprocesses.
        sys.exit( 0 )

    def __call__( self ):
        # Launch the handler processes
        # WARNING.  Will call sys.exit(0) when done!  Normal usage
        #   is to run FakeBroker either from a subproces,
        #   or from the command line where the last
        #   thing main() (below) does is call this function.
        for cfer in self.classifiers:
            mypipe, theirpipe = multiprocessing.Pipe()
            crun = functools.partial( self.classifier_runner, cfer, theirpipe )
            proc = multiprocessing.Process( target=crun )
            proc.start()
            self.classifier_procs.append( proc )
            self.classifier_pipes.append( mypipe )

        signal.signal( signal.SIGTERM, lambda sig, stack: self.shutdown() )
        signal.signal( signal.SIGINT, lambda sig, stack: self.shutdown() )

        self.logger.info( "Fakebroker starting, looking for source topics" )
        consumer = None
        while True:
            subbed = []
            if consumer is not None:
                consumer.close()
            consumer = KafkaConsumer( self.source, self.group_id, self.alert_schema,
                                      consume_nmsgs=self.consume_nmsgs, logger=self.logger )
            # Wait for the topic to exist, and only then subscribe
            while len(subbed) == 0:
                topics = consumer.topic_list()
                self.logger.debug( f"Topics seen on server: {topics}" )
                for topic in self.source_topics:
                    if topic in topics:
                        subbed.append( topic )
                if len(subbed) > 0:
                    self.logger.debug( f"Subscribing to topics {subbed}" )
                    if len(subbed) < len( self.source_topics ):
                        missing = [ i for i in self.source_topics if i not in subbed ]
                        self.logger.debug( f"(Didn't see topics: {missing})" )
                    consumer.subscribe( subbed, reset=self.reset )
                else:
                    self.logger.warning( f"No topics in {self.source_topics} exists, sleeping "
                                     f"{self.notopic_sleeptime}s and trying again." )
                    time.sleep( self.notopic_sleeptime )

            self.logger.info( "Fakebroker starting poll loop" )
            stopafternsleeps = 1 if len(subbed) < len(self.source_topics) else None
            maint_func = functools.partial( self.log_cfer_status, consumer )
            consumer.poll_loop( handler=self.handle_message_batch,
                                stopafternsleeps=stopafternsleeps,
                                stopafter=self.runtime,
                                maint_func=maint_func, maint_timeout=60 )
            self.consumer_consume_time_offset += consumer.consume_time
            self.consumer_handle_time_offset += consumer.handle_time


# ======================================================================

def main():
    parser = argparse.ArgumentParser( description="Pretend to be an LSST broker",
                                      formatter_class=argparse.ArgumentDefaultsHelpFormatter )
    parser.add_argument( "--source", default="brahms.lbl.gov:9092",
                         help="Server to pull simulated LSST AP alerts from" )
    parser.add_argument( "-t", "--source-topics", nargs='+', required=True, help="Topics on source server" )
    parser.add_argument( "-g", "--group-id", default="rknop-test",
                         help="Group ID to use on source server" )
    parser.add_argument( "-r", "--reset", action='store_true', default=False,
                         help="Reset to beginning of source stream?" )
    parser.add_argument( "-n", "--consume-nmsgs", type=int, default=1000,
                         help="Number of LSST AP alerts to attempt to consume at once" )
    parser.add_argument( "--dest", default="brahms.lbl.gov:9092",
                         help="Server to push broker message alerts to" )
    parser.add_argument( "-u", "--dest-topic", required=True, help="Topic on dest server" )
    parser.add_argument( "-s", "--alert-schema", default=f"/fastdb/share/avsc/{_schema_namespace}.Alert.avsc",
                         help="File with AP alert schema" )
    parser.add_argument( "-b", "--brokermessage-schema",
                         default=f"/fastdb/share/avsc/{_schema_namespace}.BrokerMessage.avsc",
                         help="File with broker message alert schema" )
    parser.add_argument( "-v", "--verbose", default=False, action="store_true",
                         help="Show a lot of debug log messages" )

    args = parser.parse_args()

    broker = FakeBroker( args.source, args.source_topics, args.dest, args.dest_topic,
                         group_id=args.group_id, alert_schema=args.alert_schema,
                         brokermessage_schema=args.brokermessage_schema, reset=args.reset,
                         verbose=args.verbose )
    broker()


# ======================================================================

if __name__ == "__main__":
    main()
