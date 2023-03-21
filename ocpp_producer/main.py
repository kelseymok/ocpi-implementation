import json
from typing import Dict
from time import sleep
import os
import logging
from confluent_kafka import Producer


# Wait for broker to come alive. This should rather be a healthcheck but there were some docker-compose issues.
delay = os.environ.get("DELAY_START_SECONDS", 120)
sleep(int(delay))

message_delay = os.environ.get("MESSAGE_DELAY", "2")

data_file = os.environ.get("DATA_FILE", "data/1678731740.json")

bootstrap_servers_value = os.environ.get("BOOTSTRAP_SERVERS")
logging.info(f"Bootstrap Servers: {bootstrap_servers_value}")


def convert_body_to_dict(x: Dict):
    x["body"] = json.loads(x["body"])
    return x

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

message_type_mapping = {
    2: "Request",
    3: "Response"
}

logging.info("Starting loop.")
while True:
    try:
        logging.info("Connecting...")
        producer = Producer({'bootstrap.servers': bootstrap_servers_value})

        logging.info("Connected to Broker!")

        with open(data_file) as f:
            producer.poll(0)
            input = json.load(f)
            data = [convert_body_to_dict(x) for x in input]
            for d in data:
                message_type = d["message_type"]
                action = d["action"]
                payload = json.dumps(d)
                topic = f"{action}{message_type_mapping[message_type]}"
                logging.info(f"Sending payload to topic {topic}")
                producer.produce(topic, payload.encode('utf-8'), callback=delivery_report)
                sleep(int(message_delay))

            producer.flush()

    except Exception as inst:
        logging.info("Broker not available.")
        sleep(10)



