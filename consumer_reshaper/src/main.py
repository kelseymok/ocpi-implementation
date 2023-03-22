from time import sleep

import boto3
from confluent_kafka import Consumer, KafkaError, KafkaException
import os
import logging

from data_writer import DataWriter
from transformer import Transformer

delay = os.environ.get("DELAY_START_SECONDS", 240)
sleep(int(delay))

group_id = "flattener"

conf = {
    'bootstrap.servers': os.environ.get("BOOTSTRAP_SERVERS", "localhost:9092"),
    'broker.address.family': 'v4',
    'group.id': group_id,
    'auto.offset.reset': 'smallest'
}

consumer = Consumer(conf)

running = True

transformer = Transformer()

host = os.environ.get("HOST", "localhost")
port = os.environ.get("PORT", "8000")
dynamodb_client = boto3.client(
    'dynamodb',
    region_name='local',
    endpoint_url=f"http://{host}:{port}",
    aws_access_key_id="X",
    aws_secret_access_key="X"
)

data_writer = DataWriter(client=dynamodb_client)

def basic_consume_loop(consumer, topics):
    try:
        consumer.subscribe(topics)

        while running:
            msg = consumer.poll(timeout=1.0)
            if msg is None: continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    logging.info('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                decoded_message = msg.value().decode("utf-8")
                priority_fields, metadata, data = transformer.process(decoded_message)
                data_writer.write(priority_fields=priority_fields, metadata=metadata, data=data)
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()


def shutdown():
    running = False


topics_to_consume = os.environ.get("TOPICS_TO_CONSUME", "").split(",")
basic_consume_loop(consumer, topics_to_consume)