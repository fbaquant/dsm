#!/usr/bin/env python3
import zmq
import time
import orjson as json

context = zmq.Context()
publisher = context.socket(zmq.PUB)
publisher.bind("tcp://*:5556")
print("Publisher bound on tcp://*:5556")

# Wait a moment to allow subscribers to connect
time.sleep(1)

while True:
    message = {
        "topic": "ORDERBOOK_COINBASE_TEST",
        "data": {"exchange": "COINBASE", "symbol": "TEST", "bids": [["100", "1"]], "asks": [["101", "1"]]}
    }
    publisher.send_json(message)
    print("Sent message:", message)
    time.sleep(2)