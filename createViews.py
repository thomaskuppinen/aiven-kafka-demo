from kafka import KafkaProducer
from dotenv import load_dotenv
from time import time, sleep
from datetime import datetime

import json
import os
import uuid
import argparse



def sendEvents(topic, num_events):
    #create producer object, using sensitive values stored in .env file
    producer = KafkaProducer(
        bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_URL"),
        security_protocol="SSL",
        ssl_cafile=os.environ.get("KAFKA_SSL_CAFILE_LOC"),
        ssl_certfile=os.environ.get("KAFKA_SSL_CERT_LOC"),
        ssl_keyfile=os.environ.get("KAFKA_KEYFILE_LOC"),
        value_serializer=lambda v: json.dumps(v).encode('ascii')
        )
    # simple loop to create variable number of messages for kakfa queue representing a video player sending beacons every 1 second
    i =1
    while i <= num_events:
        sleep(1 - time() % 1)
        i +=1
        view = dict(assetId="SportsCenter", viewTime=datetime.now().isoformat(), userId="user123", playerState="PLAYING")
        keyData = dict( eventType="view", eventId=str(uuid.uuid1()))
        producer.send(
            topic,
            key=str(json.dumps(keyData)).encode('utf-8'),
            value=view
        )
    # send messages to topic
    producer.flush()
    return
# main function with some very basic parameter validation
def main():
    load_dotenv()
    parser = argparse.ArgumentParser(prog="createViews.py")
    parser.add_argument("-t", "--topic", help="Name of Kafka Topic", type=str)
    parser.add_argument("-v", "--views", help="Total Number of Views to send to Kafka Topic, default is 10", type=int, default=10, choices=range(1,100), metavar="[1-100]")
    args = parser.parse_args()

    sendEvents(args.topic, args.views)
    exit()

if __name__ == "__main__":
    main()


