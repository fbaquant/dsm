#!/usr/bin/env python3
import zmq
import logging

# Configure logging: INFO level logs order book data, DEBUG level logs raw messages.
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO
)


def coinbase_subscriber():
    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    socket.connect("tcp://localhost:5556")  # Connect to ZeroMQ publisher
    # Subscribe to all messages because the topic is embedded in the JSON data
    socket.setsockopt_string(zmq.SUBSCRIBE, "")
    logging.info("Subscribed to all messages on tcp://localhost:5556")

    while True:
        try:
            message = socket.recv_json()
            logging.debug("Raw message received: %s", message)
            topic = message.get("topic", "Unknown Topic")
            data = message.get("data", {})

            # Only process messages with topics that start with the desired prefix.
            if not topic.startswith("ORDERBOOK_COINBASE_"):
                logging.debug("Ignoring message with topic: %s", topic)
                continue

            if "bids" in data and "asks" in data:
                best_bid = data["bids"][0][0] if data["bids"] else "N/A"
                best_ask = data["asks"][0][0] if data["asks"] else "N/A"
                logging.info("Order book received: Topic: %s, Exchange: %s, Symbol: %s, Best Bid: %s, Best Ask: %s",
                             topic, data.get("exchange", "Unknown"), data.get("symbol", "Unknown"), best_bid, best_ask)
            else:
                logging.warning("Received message without order book data: %s", message)
        except Exception as e:
            logging.error("Error processing message: %s", e)


if __name__ == "__main__":
    coinbase_subscriber()
