import sys
import io
import argparse
import signal
import confluent_kafka
import fastavro


class MsgConsumer:
    def __init__( self, server, groupid, topic, nmsgs=1, timeout=10, avroschema=None ):
        self.timeout = timeout
        self.nmsgs = nmsgs
        self.topic = topic
        self.consumer = confluent_kafka.Consumer( { 'bootstrap.servers': server,
                                                    'auto.offset.reset': 'earliest',
                                                    'group.id': groupid } )
        self.schema = avroschema


    def list_topics( self ):
        metadata = self.consumer.list_topics()
        print( "Topics on server:" )
        for topic in metadata.topics:
            print( f"  {topic}" )

    def close( self ):
        if self.consumer is not None:
            self.consumer.close()
        self.consumer = None


    def __del__( self ):
        self.close()


    def __call__( self, doprint=False, keep_going=False ):
        def assigned( consumer, partitions ):
            sys.stderr.write( f"Consumer assigned partitions: {partitions}\n" )

        self.consumer.subscribe( [ self.topic ], on_assign=assigned )

        done = False
        while not done:
            msgs = self.consumer.consume( self.nmsgs, timeout=self.timeout )
            print( f"Got {len(msgs)} messages:" )
            for i, msg in enumerate(msgs):
                if self.schema is not None:
                    import pdb; pdb.set_trace()
                    _parsed_msg = fastavro.schemaless_reader( io.BytesIO(msg.value()), self.schema )
                elif doprint:
                    print( f"{i:3d}: {msg.value().decode('utf-8')}" )
            done = not keep_going


def main():
    parser = argparse.ArgumentParser( "read_kafka_messages", description='read messages from a kafka server',
                                      formatter_class=argparse.ArgumentDefaultsHelpFormatter )
    parser.add_argument( "-s", "--server", default="kafka:9092", help="Kafka server to read from" )
    parser.add_argument( "-t", "--topic", default="test-topic", help="Topic to read from" )
    parser.add_argument( "-g", "--group-id", default="read-kafka-messages", help="Group ID" )
    parser.add_argument( "-n", "--max-num-messages", default=1, type=int,
                         help="Try to read this many messages" )
    parser.add_argument( "-p", "--print-messages", action='store_true', default=False,
                         help=( "Print messages (assuming they're UTF-8 strings); otherwise, just count. "
                                "Ignored if --avro-schema or --use-fastdb-alert-schema is given." ) )
    parser.add_argument( "-a", "--avro-schema", default=None,
                         help=( "File with avro schema to use to try to parse messages.  If not given, "
                                "no avro parsing is attempted." ) )
    parser.add_argument( "-u", "--use-fastdb-alert-schema", default=False, action='store_true',
                         help=( "Equivalent to passing the location for the fastdb project sim Alert schema "
                                "to --avro-schema.  Only works within fastdb docker environment." ) )
    parser.add_argument( "-w", "--timeout", default=10, type=int, help="Timeout after this many seconds" )
    parser.add_argument( "-k", "--keep_going", default=False, action='store_true',
                         help=( "Normally, after an attempt to consnume --max-num-messages times out, "
                                "the program will exit.  Specify this to keep looping and trying again." ) )
    parser.add_argument( "-l", "--list-topics", default=False, action="store_true",
                         help="Instead of trying to read messages, list topics on the server." )
    args = parser.parse_args()

    schema = None
    if args.use_fastdb_alert_schema:
        if args.avro_schema is not None:
            raise ValueError( "Can't specify both --avro-schema and --use-fastdb-alert-schema" )
        # Only importing here, because we want to be able to run this script outside
        #   of the fastdb Docker environment.
        import util
        schema = util.get_alert_schema()
    if args.avro_schema is not None:
        schema = fastavro.schema.parse_schema( fastavro.schema.load_schema( args.avro_schema ) )

    msgconsumer = MsgConsumer( args.server, args.group_id, args.topic,
                               nmsgs=args.max_num_messages, timeout=args.timeout,
                               avroschema=schema )

    # Make sure we'll close down gracefully if somebody CTRL-Cs us.
    signal.signal( signal.SIGINT, lambda signum, frame: msgconsumer.close() )
    signal.signal( signal.SIGTERM, lambda signum, frame: msgconsumer.close() )

    if args.list_topics:
        msgconsumer.list_topics()
    else:
        msgconsumer( doprint=args.print_messages, keep_going=args.keep_going )


if __name__ == "__main__":
    main()
