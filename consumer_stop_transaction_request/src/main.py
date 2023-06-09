import json
from time import sleep

import boto3
from confluent_kafka import Consumer, KafkaError, KafkaException
import os
import logging

from data_reader import DataReader
from wrangler import Wrangler
from data_writer import DataWriter
import sqlalchemy


delay = os.environ.get("DELAY_START_SECONDS", 240)
sleep(int(delay))

group_id = "stop_transaction_request_reader"

conf = {
    'bootstrap.servers': os.environ.get("BOOTSTRAP_SERVERS", "localhost:9092"),
    'broker.address.family': 'v4',
    'group.id': group_id,
    'auto.offset.reset': 'smallest'
}

consumer = Consumer(conf)

running = True

dbname = os.environ.get("DBNAME")
dbuser = os.environ.get("DBUSER")
dbpassword = os.environ.get("DBPASS")
dbhost = os.environ.get("DBHOST")

engine = sqlalchemy.create_engine(f"postgresql://{dbuser}:{dbpassword}@{dbhost}:5432/{dbname}")
data_reader = DataReader(engine)

dynamodb_client = boto3.client('dynamodb', region_name='local', endpoint_url="http://localhost:8000", aws_access_key_id="X",
    aws_secret_access_key="X")

data_writer = DataWriter(client=dynamodb_client)
wrangler = Wrangler(data_reader=data_reader, data_writer=data_writer)

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
                message = json.loads(decoded_message)
                wrangler.process(message)
                # data_writer.write(m)
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()


def shutdown():
    running = False


topics_to_consume = os.environ.get("TOPICS_TO_CONSUME", "").split(",")
basic_consume_loop(consumer, topics_to_consume)