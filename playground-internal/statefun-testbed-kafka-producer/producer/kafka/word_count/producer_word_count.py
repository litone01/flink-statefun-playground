#!/usr/bin/python3

from itertools import cycle
import os
import sys
import json
import signal
import time

import argparse

from confluent_kafka import Producer
from jsonpath_ng import parse

PWD = os.path.dirname(os.path.realpath(__file__))
   
def read_jsons(path: str):
    with open(path) as f:
        for line in f.read().splitlines():
            yield json.loads(line)

def create_requests(path: str, loop: bool, json_path):
    jsons = [js for js in read_jsons(path)]
    if loop:
        jsons = cycle(jsons)
    for js in jsons:
        matches = json_path.find(js)
        if len(matches) != 1:
            raise ValueError(f"Unable to find exactly one key at {js}, please check the correctness of your {json_path} value")
        key = matches[0]
        yield key.value, js


class KProducer(object):

    def __init__(self, broker, topic):
        self.producer = Producer({'bootstrap.servers': broker})
        self.topic = topic

    def send(self, key: str, value: str):
        key = key.encode('utf-8')
        value = value.encode('utf-8')
        self.producer.produce(topic=self.topic, key=key, value=value)
        self.producer.flush()


def produce(producer, delay_seconds: int, requests):
    for key, js in requests:
        value = json.dumps(js)
        producer.send(key=key, value=value)
        if delay_seconds > 0:
            time.sleep(delay_seconds)


def handler(number, frame):
    sys.exit(0)


def main(args):
    # setup an exit signal handler
    signal.signal(signal.SIGTERM, handler)
    signal.signal(signal.SIGINT, handler)
    # get the key,value request generators
    requests = create_requests(path=args.path, loop=args.loop, json_path=args.json_path)
    # produce forever
    while True:
        try:
            producer = KProducer(broker=args.host, topic=args.topic)
            produce(producer=producer, delay_seconds=args.delay, requests=requests)
            print("Done producing, good bye!", flush=True)
            return
        except SystemExit:
            print("Good bye!", flush=True)
            return
        except Exception as e:
            print(e)
            return


if __name__ == "__main__":
    # parse and set the arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="localhost:9092", type=str)
    parser.add_argument("--loop", default="false", type=lambda s: s.lower() == "true")
    parser.add_argument("--delay", default="2", type=int)
    parser.add_argument("--json_path", default="stream_id", type=parse)
    parser.add_argument("--path", default=os.path.join(PWD, "wordcount.json"), type=str)
    parser.add_argument("--topic", default="word-frequency", type=str)
    args = parser.parse_args()
    
    # run the main function
    main(args)
    
