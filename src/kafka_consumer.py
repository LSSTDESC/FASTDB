import sys
import io
import time
import datetime
import json
import argparse
import collections
import logging
import atexit

import fastavro
import confluent_kafka

_logger = logging.getLogger( __file__ )
_logout = logging.StreamHandler( sys.stderr )
_logger.addHandler( _logout )
_formatter = logging.Formatter( '[%(asctime)s - %(levelname)s] - %(message)s',
                                datefmt='%Y-%m-%d %H:%M:%S' )
_logout.setFormatter( _formatter )
_logger.propagate = False
_logger.setLevel( logging.INFO )


def _do_nothing( *args, **kwargs ):
    pass


def _close_kafka_consumer( obj ):
    obj.close()


class KafkaConsumer:
    """Consume messages from a kafka server using a confluent_kafka.Consumer."""

    def __init__( self, server, groupid, schema=None, topics=None, reset=False,
                  extraconsumerconfig={},
                  consume_nmsgs=100, consume_timeout=1, nomsg_sleeptime=1,
                  logger=_logger ):
        """Constructor.

        Parameters
        ----------
          server : str
            The url of the kafka server

          groupid : str
            The group id to send to the server.  Servers remember which
            messages a given groupid has consumed.

          schema : str or Path, or None
            Path to the avro schema to load.  Only needed if you
            use the echoing message handler.

          topics : list of str, default None
            Topics to subscribe to; if [] or None, does no initial subscription.

          reset : bool, default False
            Reset topics to earliest available message?  Ignored if
            topics is False.

          extraconsumerconfig: {}
            Additional config to pass to the confluent_kafka.Consumer
            constructor.  (Do not include bootstrap.servers,
            auto.offset.reset, or group.id; those are automatically
            constructed and sent.)

          consume_nmsgs: int, default 100
            Number of messages to try to consume at once

          consume_timeout: float, default 1
            Timeout for kafka consumption.  (You want to keep this
            short; if there are fewer than consume_nmsgs available, then
            it will wait this long.)

          nomsg_sleeptime: float, default 1
            If there are no messages on the server, sleep things long
            before asking again.

          logger: logging.Logger (optional)

        """

        self.logger = logger
        self.tot_handled = 0
        if topics is None:
            self.topics = []
        elif isinstance( topics, str ):
            self.topics = [ topics ]
        elif isinstance( topics, collections.abc.Sequence ):
            self.topics = list( topics )
        else:
            raise TypeError( f"topics must be either a string or a list, not a {type(topics)}" )

        self.schema = fastavro.schema.load_schema( schema ) if schema is not None else None
        self.consume_nmsgs = consume_nmsgs
        self.consume_timeout = consume_timeout
        self.nomsg_sleeptime = nomsg_sleeptime

        self.consume_time = 0
        self.handle_time = 0

        consumerconfig = { 'bootstrap.servers': server,
                           'auto.offset.reset': 'earliest',
                           'group.id': groupid }
        consumerconfig.update( extraconsumerconfig )
        self.logger.debug( f"Initializing Kafka consumer with\n{json.dumps(consumerconfig, indent=4)}" )
        self.logger.debug( f"Topics given at KafkaConsumer init: {self.topics}" )
        self.consumer = confluent_kafka.Consumer( consumerconfig )
        atexit.register( _close_kafka_consumer, self )

        self.subscribed = False
        self.subscribe( self.topics, reset=reset )


    def close( self ):
        if self.consumer is not None:
            self.consumer.close()
            self.consumer = None

    def __del__( self ):
        self.close()


    def subscribe( self, topics, reset=False ):
        """Subscribe to topics.

        Will query the server for existing topics, and only subscribe to
        those that actually exist.

        I *think* this replaces existing subscriptions with the ones you
        ask for.

        Paramters
        ---------
          topics : str or list
            Topics on the server to subscribe to.

          reset : bool, default False
            If True, will reset the offset for all partitions on the
            server for each topic to the low watermark.  This is useful
            for testing, but in normal usage you don't want to do this,
            you want the server to remember where your groupid was.

        """
        if topics is None:
            self.topics = []
        elif isinstance( topics, str ):
            self.topics = [ topics ]
        elif isinstance( topics, collections.abc.Sequence ):
            self.topics = list( topics )
        else:
            raise TypeError( f"topics must be either a string or a list, not a {type(topics)}" )

        self.logger.debug( "Asking server for topics" )
        servertopics = self.topic_list()
        self.logger.debug( f"Topics found on server: {servertopics}" )
        subtopics = []
        for topic in self.topics:
            if topic not in servertopics:
                self.logger.warning( f'Topic {topic} not on server, not subscribing' )
            else:
                subtopics.append( topic )
        self.topics = subtopics

        if self.topics is not None and len(self.topics) > 0:
            self.logger.info( f'Subscribing to topics: {", ".join( topics )}' )
            self.consumer.subscribe( topics, on_assign=self._sub_reset_callback if reset else self._sub_callback )
        else:
            self.logger.warning( 'No existing topics given, not subscribing.' )


    def _sub_callback( self, consumer, partitions ):
        self.subscribed = True
        ofp = io.StringIO()
        ofp.write( "Consumer subscribed.  Assigned partitions:\n" )
        self._dump_assignments( ofp, self._get_positions( partitions ) )
        self.logger.debug( ofp.getvalue() )
        ofp.close()

    def _sub_reset_callback( self, consumer, partitions ):
        for partition in partitions:
            lowmark, _highmark = consumer.get_watermark_offsets( partition )
            partition.offset = lowmark
        consumer.assign( partitions )
        self._sub_callback( consumer, partitions )

    # I've had trouble using this.
    # Have had better luck passing reset=True to subscribe.
    def reset_to_start( self, topic ):
        self.logger.info( f'Resetting partitions for topic {topic}\n' )
        # Poll once to make sure things are connected
        msg = self.consume_one_message( timeout=4, handler=_do_nothing )
        self.logger.debug( "got throwaway message" if msg is not None else "didn't get throwaway message" )
        # Now do the reset
        partitions = self.consumer.list_topics( topic ).topics[topic].partitions
        self.logger.debug( f"Found {len(partitions)} partitions for topic {topic}" )
        # partitions is a kmap
        if len(partitions) > 0:
            partlist = []
            for i in range(len(partitions)):
                self.logger.info( f'...resetting partition {i}' )
                curpart = confluent_kafka.TopicPartition( topic, i )
                lowmark, highmark = self.consumer.get_watermark_offsets( curpart )
                self.logger.debug( f'Partition {curpart.topic} has id {curpart.partition} '
                                   f'and current offset {curpart.offset}; lowmark={lowmark} '
                                   f'and highmark={highmark}' )
                curpart.offset = lowmark
                # curpart.offset = confluent_kafka.OFFSET_BEGINNING
                if lowmark < highmark:
                    self.consumer.seek( curpart )
                partlist.append( curpart )
            self.logger.info( 'Committing partition offsets.' )
            self.consumer.commit( offsets=partlist, asynchronous=False )
        else:
            self.logger.info( "Resetting partitions: no partitions found, hope that means we're already reset...!" )


    def topic_list( self ):
        """Return a list of topic names available on the kafka server."""
        cluster_meta = self.consumer.list_topics()
        topics = [ n for n in cluster_meta.topics ]
        topics.sort()
        return topics

    def print_topics( self ):
        """Log to debug a list of topic names available on the kafka server."""
        topics = self.topic_list()
        topicstxt = '\n  '.join(topics)
        self.logger.debug( f"\nTopics:\n   {topicstxt}" )

    def _get_positions( self, partitions ):
        return self.consumer.position( partitions )

    def _dump_assignments( self, ofp, partitions ):
        ofp.write( f'{"Topic":<32s} {"partition":>9s} {"offset":>12s}\n' )
        for par in partitions:
            ofp.write( f"{par.topic:32s} {par.partition:9d} {par.offset:12d}\n" )
        ofp.write( "\n" )

    def print_assignments( self ):
        """Log to debug the topics, partitions, and offsets."""
        asmgt = self._get_positions( self.consumer.assignment() )
        ofp = io.StringIO()
        ofp.write( "Current partition assignments\n" )
        self._dump_assignments( ofp, asmgt )
        self.logger.debug( ofp.getvalue() )
        ofp.close()

    def poll_loop( self, handler=None, timeout=None, pipe=None, stopafter=datetime.timedelta(hours=1),
                   stopafternmessages=None, stopafternsleeps=None, maint_func=None, maint_timeout=60 ):
        """Calls handler with batches of messages.

        Parameters
        ----------
          handler : function or None
            A callback that's called with batches of messages (the list
            returned by confluent_kafka.Consumer.consume().  If None,
            will call a stupid built-in default that you probably don't
            want to use.

          timeout : float or None
            Timeout for confluent_kafka.Consumer.consume().  If not
            give, will use the consume_timeout passed at object reation.

          pipe : multiprocessing.Pipe
            If not None, the poll_loop will send regular heartbeats to
            this Pipe.  It will also poll the pipe for messages.
            (Currently, the only message it will receive is a request to
            die.)

          stopafter : datetime.timedelta or None, default datetime.timedelta( hours=1 )
            Quit polling after this much time has elapsed.

          stopafternsleeps : int, default None
            If not None, after the poll_loop receives no messages and
            sleeps this many consecutive times, return.  (Useful in our
            tests.)

          stopafternmessages : int, default None
            If not None, after poll_loop has consumed this many
            messages, it will return.  Note that it might consume more
            than this.  It consumes messages in batches, so if the
            previous batch didn't pass this, and the current batch goes
            beyond this, it will complete the current batch.  Make this
            0 to immediately exit after getting no messages from the
            server.

          maint_func : callable, default None
            A maintenance function that is called approximately every
            maint_timeout seconds.

          maint_timeout : int, default 60
            How often to call maint_func.  Ignored if maint_func is None.

        Returns
        -------
          True if number of messages consumed ≥ stopafternmessages or
          timedout, False if died due to die command sent to pipe.

        """

        timeout = timeout if timeout is not None else self.consume_timeout
        handler = handler if handler is not None else self.default_handle_message_batch
        t0 = datetime.datetime.now()
        next_maint_timeout = time.monotonic() + maint_timeout
        nsleeps = 0
        nconsumed = 0
        keepgoing = True
        retval = True
        while keepgoing:
            self.logger.debug( f"Trying to consume {self.consume_nmsgs} messages "
                               f"with timeout {timeout} sec...\n" )
            tperf0 = time.perf_counter()
            msgs = self.consumer.consume( self.consume_nmsgs, timeout=timeout )
            if len(msgs) == 0:
                if ( stopafternsleeps is not None ) and ( nsleeps >= stopafternsleeps ):
                    self.logger.debug( f"Stopping after {nsleeps} consecutive sleeps." )
                    keepgoing = False
                else:
                    self.logger.debug( f"...no messages, sleeping {self.nomsg_sleeptime} sec" )
                    time.sleep( self.nomsg_sleeptime )
                    nsleeps += 1
            else:
                tperf1 = time.perf_counter()
                self.logger.debug( f"...got {len(msgs)} messages" )
                nsleeps = 0
                handler( msgs )
                self.tot_handled += len( msgs )
                nconsumed += len( msgs )
                tperf2 = time.perf_counter()
                self.consume_time += tperf1 - tperf0
                self.handle_time += tperf2 - tperf1

            runtime = datetime.datetime.now() - t0
            if ( ( ( stopafternmessages is not None ) and ( nconsumed > stopafternmessages ) )
                 or
                 ( ( stopafter is not None ) and ( runtime > stopafter ) )
                ):
                keepgoing = False

            if ( maint_func is not None ) and ( time.monotonic() > next_maint_timeout ):
                self.logger.warning( "Calling maint_func" )
                maint_func()
                next_maint_timeout += maint_timeout

            if pipe is not None:
                pipe.send( { "message": "ok", "nconsumed": nconsumed,
                             "tot_handled": self.tot_handled, "runtime": runtime } )
                if pipe.poll():
                    msg = pipe.recv()
                    if ( 'command' in msg ) and ( msg['command'] == 'die' ):
                        self.logger.info( "Exiting poll loop due to die command." )
                        retval = False
                        keepgoing = False
                    else:
                        self.logger.error( f"Ignoring unknown message from pipe: {msg}" )

        self.logger.info( f"Stopping poll loop after consuming {nconsumed} messages during {runtime}" )
        return retval

    def consume_one_message( self, timeout=None, handler=None ):
        """Both calls handler and returns a batch of 1 message."""

        timeout = timeout if timeout is not None else self.consume_timeout
        handler = handler if handler is not None else self.default_handle_message_batch
        self.logger.debug( f"Trying to consume one message with timeout {timeout} sec...\n" )
        msg = self.consumer.poll( timeout )
        if msg is not None:
            if msg.error():
                raise RuntimeError( f"Kafka message returned error: {msg.error()}" )
            handler( [ msg ] )
        return msg

    def default_handle_message_batch( self, msgs ):
        """The default message handler just logs how many messages have been consumed."""
        self.logger.info( f'Got {len(msgs)}; have received {self.tot_handled} so far.' )

    def echoing_handle_message_batch( self, msgs ):
        """A handler that dumps the contents of each message it receives.

        Use this with care, it can be extremely verbose.
        """

        self.logger.info( f'Handling {len(msgs)} messages' )
        for msg in msgs:
            ofp = io.StringIO( f"Topic: {msg.topic()} ; Partition: {msg.partition()} ; "
                               f"Offset: {msg.offset()} ; Key: {msg.key()}\n" )
            alert = fastavro.schemaless_reader( io.BytesIO(msg.value()), self.schema )
            ofp.write( json.dumps( alert, indent=4, sort_keys=True ) )
            ofp.write( "\n" )
            self.logger.info( ofp.getvalue() )
            ofp.close()
        self.logger.info( f'Have handled {self.tot_handled} messages so far' )
        # self.print_assignments()


# ======================================================================

def main():
    parser = argparse.ArgumentParser( 'kafka_consumer',
                                      description="Read messages from a kafka server",
                                      formatter_class=argparse.ArgumentDefaultsHelpFormatter )
    parser.add_argument( 'server', help='URL of kafka server' )
    parser.add_argument( '-g', '--groupid', required=True, help='Group ID to send to server' )
    parser.add_argument( '-t', '--topic', required=True, help='Kafka topic to poll' )
    parser.add_argument( '-r', '--reset', default=False, action='store_true', help="Reset topic to low watermark." )
    parser.add_argument( '-s', '--schema', default=None, help='File with avro schema (only needed if -e is passed' )
    parser.add_argument( '-e', '--echo', default=False, action='store_true',
                         help="Echo messages to console.  (Default, just counts messages.)" )
    parser.add_argument( '-b', '--batch-size', type=int, default=1000,
                         help='Consume messages from the server in batches of this size.' )
    parser.add_argument( '--consume-timeout', type=int, default=1,
                         help="Timeout to use waiting for the server.  Usually no need to fiddle with this." )
    parser.add_argument( '--nomsg-sleeptime', type=int, default=1,
                         help="Timeout to use when server has no messages.  May want to make this longer." )
    parser.add_argument( '-n', '--num-messages', type=int, default=None,
                         help="Stop after receiving this many (or maybe a bit more) messages." )
    parser.add_argument( '-m', '--max-runtime', type=int, default=None,
                         help="Max number of seconds to run before quitting." )
    parser.add_argument( '--max-sleeps', type=int, default=None,
                         help="In the weeds; see stopafternsleeps on poll_loop" )
    parser.add_argument( '-v', default=False, action='store_true', help="Verbose logging." )
    args = parser.parse_args()

    if args.verbose:
        _logger.setLevel( logging.DEBUG )
    else:
        _logger.setLevel( logging.INFO )

    consumer = KafkaConsumer( args.server, args.groupid, args.schema, topics=args.topic,
                              reset=args.reset, consume_nmsgs=args.batch_size,
                              consume_timeout=args.consume_timeout, nomsg_sleeptime=args.nopmsg_sleeptime )
    handler = consumer.echoing_handle_message_batch if args.echo is not None else None
    stopafter = datetime.timedelta( seconds=args.max_runtime ) if args.max_runtime is not None else None
    consumer.poll_loop( handler=handler, stopafter=stopafter,
                        stopafternmessages=args.num_messages, stopafternsleeps=args.max_sleeps )
