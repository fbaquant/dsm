#!/usr/bin/env python3
import hashlib
import json
import logging
import os
import threading
import time

import jwt
import websocket
import zmq

from config import CONFIG

# Configure logging with a less verbose output
logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)


class CoinbaseStreamer:
    """Streams real-time order book data from Coinbase Advanced Trade API via ZeroMQ."""

    def __init__(self, api_key, secret_key, ws_url, channel_names, zmq_port=5556):
        # Initialize ZeroMQ publisher on the desired port (5556 for the subscriber script)
        self.context = zmq.Context()
        self.publisher = self.context.socket(zmq.PUB)
        self.publisher.bind(f"tcp://*:{zmq_port}")  # Bind to TCP port 5556 for publishing
        logging.info("ZeroMQ publisher bound on port %s", zmq_port)

        self.api_key = api_key
        self.secret_key = secret_key
        self.ws_url = ws_url
        self.channel_names = channel_names

        # Internal order book data store
        self.order_book = {}
        # Define the product (symbol) to subscribe for – update if needed.
        self.symbol = "BTC-USD"

    def generate_coinbase_jwt(self, message, channel, products=[]):
        """
        Generate a JWT token for Coinbase authentication and add it to the message.
        """
        timestamp = int(time.time())

        payload = {
            "iss": "coinbase-cloud",
            "nbf": timestamp,
            "exp": timestamp + 120,
            "sub": self.api_key,
        }
        headers = {
            "kid": self.api_key,
            "nonce": hashlib.sha256(os.urandom(16)).hexdigest()
        }
        token = jwt.encode(payload, self.secret_key, algorithm="ES256", headers=headers)
        message['jwt'] = token
        logging.info("JWT generated for channel %s", channel)
        return message

    def subscribe_coinbase(self, ws, products, channel_name):
        """
        Subscribe to a Coinbase channel via the WebSocket.
        """
        message = {
            "type": "subscribe",
            "channel": channel_name,
            "product_ids": products
        }
        signed_message = self.generate_coinbase_jwt(message, channel_name, products)
        ws.send(json.dumps(signed_message))
        logging.info("Sent subscription message for products %s on channel %s", products, channel_name)

    def websocket_handler(self, ws, message):
        """
        Handle incoming WebSocket messages.
        """
        # Log receipt of a message without printing the full content
        logging.debug("Received a WebSocket message")
        try:
            data = json.loads(message)
            self._update_order_book(data)
        except Exception as e:
            logging.error("Error processing WebSocket message: %s", e)

    def _update_order_book(self, data):
        """
        Update the internal order book with the new data and publish the update.
        This implementation replaces the entire order book; in a real system you would
        merge incremental updates.
        """
        self.order_book = data
        self._publish_order_book()

    def _publish_order_book(self):
        """
        Publish the updated order book using ZeroMQ. The message contains:
         - "topic": a streaming topic string like "ORDERBOOK_COINBASE_BTC-USD"
         - "data": the order book details, including exchange and symbol information.
        """
        message = {
            "topic": f"ORDERBOOK_COINBASE_{self.symbol}",
            "data": {**self.order_book, "exchange": "COINBASE", "symbol": self.symbol}
        }
        self.publisher.send_json(message)

    def start_streaming(self):
        """
        Start the Coinbase WebSocket streaming connection.
        """
        ws_app = websocket.WebSocketApp(
            self.ws_url,
            on_open=lambda ws: self.subscribe_coinbase(ws, [self.symbol], self.channel_names["level2"]),
            on_message=self.websocket_handler
        )
        ws_thread = threading.Thread(target=ws_app.run_forever)
        ws_thread.daemon = True
        ws_thread.start()
        logging.info("Started WebSocket streaming for symbol: %s", self.symbol)
        return ws_thread

    def start(self):
        """
        Start the Coinbase streamer and keep the service running.
        """
        ws_thread = self.start_streaming()
        logging.info("Coinbase streamer is running. Press Ctrl+C to exit.")
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            logging.info("Stopping Coinbase streamer...")
            self.publisher.close()
            self.context.term()
            ws_thread.join()
            logging.info("Coinbase streamer stopped.")


if __name__ == "__main__":
    streamer = CoinbaseStreamer(
        api_key=CONFIG["exchanges"]["coinbase"]["api_key"],
        secret_key=CONFIG["exchanges"]["coinbase"]["secret_key"],
        ws_url=CONFIG["exchanges"]["coinbase"]["ws_url"],
        channel_names={"level2": "level2"},
        zmq_port=5556)
    streamer.start()
