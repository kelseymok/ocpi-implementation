from time import sleep

from confluent_kafka import Consumer, KafkaError, KafkaException
import os
import logging

from data_writer import DataWriter
from flattener import Flattener

import psycopg2


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

flattener = Flattener()

dbname = os.environ.get("DBNAME")
dbuser = os.environ.get("DBUSER")
dbpassword = os.environ.get("DBPASS")
dbhost = os.environ.get("DBHOST")
psycopg2.connect(f"dbname={dbname} user={dbuser} password={dbpassword} host={dbhost}")
conn = psycopg2.connect(f"dbname={dbname} user={dbuser} password={dbpassword} host={dbhost}")
conn.autocommit = True
cur = conn.cursor()

data_writer = DataWriter(conn_cursor=cur)

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
                flattened_msg = flattener.process(decoded_message)
                for m in flattened_msg:
                    data_writer.write(m)
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()


def shutdown():
    running = False


topics_to_consume = os.environ.get("TOPICS_TO_CONSUME", "").split(",")
basic_consume_loop(consumer, topics_to_consume)