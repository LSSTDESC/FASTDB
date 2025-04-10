import confluent_kafka
import argparse


def main():
    parser = argparse.ArgumentParser( 'post_kafka_message', description='post a message to a kafka server',
                                      formatter_class=argparse.ArgumentDefaultsHelpFormatter )
    parser.add_argument( "-s", "--server", default="kafka:9092", help="Kafka server to post to" )
    parser.add_argument( "-t", "--topic", default="test-topic", help="Topic to post to" )
    parser.add_argument( "-m", "--message", default="test message", help="Message to send" )
    args = parser.parse_args()

    producer = confluent_kafka.Producer( { 'bootstrap.servers': args.server,
                                           'batch.size': 131072,
                                           'linger.ms': 50 } )
    producer.produce( args.topic, args.message.encode( 'utf-8' ) )
    producer.flush()


if __name__ == "__main__":
    main()
