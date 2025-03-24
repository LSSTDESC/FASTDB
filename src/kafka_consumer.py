import sys
import io
import time
import datetime
import json
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


class DateTimeEncoder( json.JSONEncoder ):
    def default( self, obj ):
        if isinstance( obj, datetime.datetime ):
            return str( obj.isoformat() )
        else:
            # Should I use super() here?
            return json.JSONEncoder.default( self, obj )


def _do_nothing( *args, **kwargs ):
    pass


class KafkaConsumer:
    def __init__( self, server, groupid, schema, topics=None, reset=False,
                  extraconsumerconfig={},
                  consume_nmsgs=100, consume_timeout=1, nomsg_sleeptime=1,
                  logger=_logger ):
        """Consume messages from a kfaka server.

        Parameters
        ----------
          server : str
            The url of the kafka server

          groupid : str
            The group id to send to the server

          schema : str or Path
            Path to the avro schema to load

          topics : list of str, default none
            Topics to subscribe to; if [] or None, does no initial subscription.

          reset : bool, default False
            Reset topics to earliest available message?

          extraconsumerconfig: {}

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

        self.schema = fastavro.schema.load_schema( schema )
        self.consume_nmsgs = consume_nmsgs
        self.consume_timeout = consume_timeout
        self.nomsg_sleeptime = nomsg_sleeptime

        consumerconfig = { 'bootstrap.servers': server,
                           'auto.offset.reset': 'earliest',
                           'group.id': groupid }
        consumerconfig.update( extraconsumerconfig )
        self.logger.debug( f"Initializing Kafka consumer with\n{json.dumps(consumerconfig, indent=4)}" )
        self.consumer = confluent_kafka.Consumer( consumerconfig )
        atexit.register( self.__del__ )

        self.subscribed = False
        self.subscribe( self.topics, reset=reset )


    def close( self ):
        if self.consumer is not None:
            self.consumer.close()
            self.consumer = None

    def __del__( self ):
        self.close()


    def subscribe( self, topics, reset=False ):
        if ( topics is not None ) and ( len( topics ) > 0 ):
            self.subscribed = False
            self.consumer.subscribe( topics, on_assign=self._sub_reset_callback if reset else self._sub_callback )
        else:
            self.logger.debug( "No topics given, not subscribing." )

    def _sub_callback( self, consumer, partitions ):
        self.subscribed = True
        ofp = io.StringIO()
        ofp.write( "Consumer subscribed.  Assigned partitions:\n" )
        self._dump_assignments( ofp, self._get_positions( partitions ) )
        self.logger.debug( ofp.getvalue() )

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
        cluster_meta = self.consumer.list_topics()
        topics = [ n for n in cluster_meta.topics ]
        topics.sort()
        return topics

    def print_topics( self ):
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
        asmgt = self._get_positions( self.consumer.assignment() )
        ofp = io.StringIO()
        ofp.write( "Current partition assignments\n" )
        self._dump_assignments( ofp, asmgt )
        self.logger.debug( ofp.getvalue() )
        ofp.close()

    def poll_loop( self, handler=None, timeout=None, stopafter=datetime.timedelta(hours=1),
                   stopafternsleeps=None, stoponnomessages=False ):
        """Calls handler with batches of messages."""
        if timeout is None:
            timeout = self.consume_timeout
        t0 = datetime.datetime.now()
        done = False
        nsleeps = 0
        while not done:
            self.logger.debug( f"Trying to consume {self.consume_nmsgs} messages "
                               f"with timeout {timeout} sec...\n" )
            msgs = self.consumer.consume( self.consume_nmsgs, timeout=timeout )
            if len(msgs) == 0:
                if ( stopafternsleeps is not None ) and ( nsleeps >= stopafternsleeps ):
                    self.logger.debug( f"Stopping after {nsleeps} consecutive sleeps." )
                    done = True
                if stoponnomessages:
                    self.logger.debug( "...no messages, ending poll_loop." )
                    done = True
                else:
                    self.logger.debug( f"...no messages, sleeping {self.nomsg_sleeptime} sec" )
                    time.sleep( self.nomsg_sleeptime )
                    nsleeps += 1
            else:
                self.logger.debug( f"...got {len(msgs)} messages" )
                nsleeps = 0
                if handler is not None:
                    handler( msgs )
                else:
                    self.default_handle_message_batch( msgs )
            if (not done) and ( datetime.datetime.now() - t0 ) >= stopafter:
                self.logger.debug( f"Ending poll loop after {stopafter} seconds of polling." )
                done = True

    def consume_one_message( self, timeout=None, handler=None ):
        """Both calls handler and returns a batch of 1 message."""
        if timeout is None:
            timeout = self.consume_timeout
        self.logger.debug( f"Trying to consume one message with timeout {timeout} sec...\n" )
        # msgs = self.consumer.consume( 1, timeout=self.consume_timeout )
        msg = self.consumer.poll( timeout )
        if msg is not None:
            if msg.error():
                raise RuntimeError( f"Kafka message returned error: {msg.error()}" )
            if handler is not None:
                handler( [ msg ] )
            else:
                self.default_handle_message_batch( [ msg ] )
        return msg

    def default_handle_message_batch( self, msgs ):
        self.logger.debug( f'Handling {len(msgs)} messages' )
        timestamp_name = { confluent_kafka.TIMESTAMP_NOT_AVAILABLE: "TIMESTAMP_NOT_AVAILABLE",
                           confluent_kafka.TIMESTAMP_CREATE_TIME: "TIMESTAMP_CREATE_TIME",
                           confluent_kafka.TIMESTAMP_LOG_APPEND_TIME: "TIMESTAMP_LOG_APPEND_TIME" }
        for msg in msgs:
            ofp = io.StringIO()
            ofp.write( f"{msg.topic()} {msg.partition()} {msg.offset()} {msg.key()}\n" )
            if msg.headers() is not None:
                ofp.write( "HEADERS:\n" )
                for key, value in msg.headers():
                    ofp.write( f"  {key} : {value}\n" )
            timestamp = msg.timestamp()
            ofp.write( f"Timestamp: {timestamp[1]} (type {timestamp_name[timestamp[0]]})\n" )
            ofp.write( "MESSAGE PAYLOAD:\n" )
            alert = fastavro.schemaless_reader( io.BytesIO(msg.value()), self.schema )
            # # They are datetime -- Convert to numbers
            # alert['elasticcPublishTimestamp'] = alert['elasticcPublishTimestamp'].timestamp()
            # alert['brokerIngestTimestamp'] = alert['brokerIngestTimestamp'].timestamp()
            ofp.write( json.dumps( alert, indent=4, sort_keys=True, cls=DateTimeEncoder ) )
            ofp.write( "\n" )
            self.logger.debug( ofp.getvalue() )
            ofp.close()
        self.tot_handled += len(msgs)
        self.logger.debug( f'Have handled {self.tot_handled} messages so far' )
        self.print_assignments()
