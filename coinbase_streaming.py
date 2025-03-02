#!/usr/bin/env python3
import datetime
import hashlib
import logging
import os
import threading
import time
import queue

import jwt
import websocket
import zmq
from sortedcontainers import SortedDict  # For maintaining sorted order of bids/asks

try:
    import orjson as json_parser  # High-performance JSON parser if available
    def dumps(obj):
        return json_parser.dumps(obj).decode("utf-8")
    loads = json_parser.loads
except ImportError:
    import json as json_parser
    dumps = json_parser.dumps
    loads = json_parser.loads

from config import CONFIG

# Configure logging: DEBUG level for development.
logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)

# Constant for the subscription channel used by Coinbase (fixed as "level2")
CHANNEL = "level2"


class OrderBook:
    """
    Maintains the order book with separate sorted dictionaries for bids and asks.
    Bids use descending order (via negative keys) and asks use ascending order.
    """
    def __init__(self):
        self.bids = SortedDict(lambda x: -x)  # Descending order for bids
        self.asks = SortedDict()              # Ascending order for asks

    def update_order(self, price: float, quantity: float, side: str):
        """
        Update the order book with a new price and quantity.
        If quantity is 0.0, the price level is removed.
        """
        book = self.bids if side.lower() == "bid" else self.asks
        if quantity == 0.0:
            if price in book:
                del book[price]
        else:
            book[price] = quantity


class PublisherThread(threading.Thread):
    """
    Dedicated thread for publishing messages via ZeroMQ.
    Messages are queued to decouple publishing from WebSocket processing.
    """
    def __init__(self, publisher):
        super().__init__()
        self.publisher = publisher
        self.queue = queue.Queue()
        self.daemon = True
        self.running = True

    def run(self):
        # Check the queue every 0.05 seconds.
        while self.running:
            try:
                msg = self.queue.get(timeout=0.05)
                self.publisher.send_json(msg)
                self.queue.task_done()
                logging.debug("Published message: %s", msg.get("topic"))
            except queue.Empty:
                continue

    def publish(self, msg):
        self.queue.put(msg)

    def stop(self):
        self.running = False


class CoinbaseStreamer:
    """
    Streams real-time order book data from the Coinbase Advanced Trade API via ZeroMQ.
    Maintains a separate OrderBook instance for each subscribed symbol.
    Also logs the current best bid and ask for each symbol every second,
    and publishes three timestamps:
      - timeExchange: Time provided by the exchange (from data["timestamp"])
      - timeReceived: Time when the message was received (UTC, ISO 8601 with microseconds)
      - timePublished: Time when the order book was published (UTC, ISO 8601 with microseconds)
    """
    def __init__(self, api_key, secret_key, ws_url, symbols, topic_prefix, zmq_port):
        """
        Initialize the CoinbaseStreamer.
        Parameters:
            api_key (str): API key for Coinbase authentication.
            secret_key (str): Secret key for Coinbase authentication.
            ws_url (str): WebSocket URL for Coinbase streaming.
            symbols (list): List of product symbols (e.g., ["BTC-USD", "ETH-USD"]) to subscribe.
            zmq_port (int): Port number for ZeroMQ publisher.
        """
        self.context = zmq.Context()
        self.publisher = self.context.socket(zmq.PUB)
        self.publisher.bind(f"tcp://*:{zmq_port}")
        logging.info("ZeroMQ publisher bound on port %s", zmq_port)

        # Start the dedicated publisher thread.
        self.publisher_thread = PublisherThread(self.publisher)
        self.publisher_thread.start()

        self.api_key = api_key
        self.secret_key = secret_key
        self.ws_url = ws_url
        self.symbols = symbols
        self.topic_prefix = topic_prefix
        self.order_book = {symbol: OrderBook() for symbol in symbols}
        self.ws_thread = None   # Will hold the WebSocket thread reference
        self.ws_app = None      # Will hold the WebSocketApp instance

        # For periodic logging of the order book.
        self.logging_running = False
        self.logging_thread = None

    def generate_coinbase_jwt(self, message, channel, products=[]):
        """
        Generate a JWT token for Coinbase authentication and attach it to the message.
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
        logging.debug("JWT generated for channel %s", channel)
        return message

    def subscribe_coinbase(self, ws):
        """
        Subscribe to the Coinbase channel using the constant CHANNEL and the list of symbols.
        """
        message = {
            "type": "subscribe",
            "channel": CHANNEL,
            "product_ids": self.symbols
        }
        signed_message = self.generate_coinbase_jwt(message, CHANNEL, self.symbols)
        ws.send(dumps(signed_message))
        logging.info("Sent subscription message for products %s on channel %s", self.symbols, CHANNEL)

    def websocket_handler(self, ws, message):
        """
        Handle incoming WebSocket messages.
        """
        logging.debug("WebSocket message received: %s", message)
        if not isinstance(message, str):
            logging.debug("Received non-string message: %s", message)
            return
        try:
            # Record timeReceived in UTC with microseconds.
            timeReceived = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec='microseconds')
            data = loads(message)
            self._update_order_book(data, timeReceived)
        except Exception as e:
            logging.error("Error processing WebSocket message: %s", e)

    def _update_order_book(self, data, timeReceived):
        """
        Update the OrderBook instance based on incoming data.
        Data is expected to have keys: 'channel', 'client_id', 'timestamp', 'sequence_num', 'events'
        Each event in data["events"] has keys: type, product_id, updates.
        If the event type is "snapshot", clear the order book for that product.
        Then, for each update in event["updates"], update the order book using
        the keys 'side', 'price_level', and 'new_quantity'.
        Records timePublished (in UTC, ISO 8601 with microseconds) after processing.
        """
        timeExchange = data.get("timestamp")  # Provided by the exchange, expected in UTC
        if "events" in data:
            for event in data["events"]:
                product_id = event.get("product_id")
                if not product_id:
                    logging.debug("Event missing product_id; skipping event: %s", event)
                    continue
                if product_id not in self.order_book:
                    logging.debug("Event for product %s not in subscription list; skipping.", product_id)
                    continue

                order_book_instance = self.order_book[product_id]
                event_type = event.get("type", "").lower()
                if event_type == "snapshot":
                    order_book_instance.bids.clear()
                    order_book_instance.asks.clear()
                    logging.debug("Cleared order book for %s due to snapshot.", product_id)

                updates = event.get("updates", [])
                for upd in updates:
                    side = upd.get("side", "").lower()
                    price = upd.get("price_level")
                    quantity = upd.get("new_quantity")
                    if not (side and price is not None and quantity is not None):
                        logging.debug("Skipping update with insufficient data: %s", upd)
                        continue
                    try:
                        order_book_instance.update_order(float(price), float(quantity), side)
                    except Exception as e:
                        logging.error("Error updating order at price %s for product %s: %s", price, product_id, e)
                # Record timePublished in UTC with microseconds.
                timePublished = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec='microseconds')
                self._publish_order_book(product_id, timeExchange, timeReceived, timePublished)
        else:
            # Fallback branch (if 'events' key is not present)
            symbol = data.get("product_id") or data.get("instrument_id") or data.get("symbol")
            if not symbol:
                logging.debug("Received data without a recognized product identifier; skipping update. Data: %s", data)
                return
            if symbol not in self.order_book:
                logging.debug("Received data for symbol %s not in subscription list; skipping update.", symbol)
                return
            order_book_instance = self.order_book[symbol]
            if "bids" in data:
                for price, quantity in data["bids"]:
                    try:
                        order_book_instance.update_order(float(price), float(quantity), "bid")
                    except Exception as e:
                        logging.error("Error updating bid at price %s for symbol %s: %s", price, symbol, e)
            if "asks" in data:
                for price, quantity in data["asks"]:
                    try:
                        order_book_instance.update_order(float(price), float(quantity), "ask")
                    except Exception as e:
                        logging.error("Error updating ask at price %s for symbol %s: %s", price, symbol, e)
            timePublished = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec='microseconds')
            self._publish_order_book(symbol, timeExchange, timeReceived, timePublished)

    def _publish_order_book(self, symbol, timeExchange, timeReceived, timePublished):
        """
        Enqueue the updated order book for the specified symbol to be published via ZeroMQ.
        The published message now includes the three timestamps.
        """
        order_book_instance = self.order_book[symbol]
        published_data = {
            "bids": list(order_book_instance.bids.items()),
            "asks": list(order_book_instance.asks.items()),
            "timeExchange": timeExchange,
            "timeReceived": timeReceived,
            "timePublished": timePublished
        }
        message = {
            "topic": f"{self.topic_prefix}_{symbol}",
            "data": {**published_data, "exchange": "COINBASE", "symbol": symbol}
        }
        self.publisher_thread.publish(message)
        logging.debug("Enqueued order book update for symbol %s", symbol)

    def start_streaming(self):
        """
        Start the Coinbase WebSocket streaming connection in a separate thread.
        """
        self.ws_app = websocket.WebSocketApp(
            self.ws_url,
            on_open=lambda ws: self.subscribe_coinbase(ws),
            on_message=self.websocket_handler
        )
        self.ws_thread = threading.Thread(target=self.ws_app.run_forever)
        self.ws_thread.daemon = True
        self.ws_thread.start()
        logging.info("Started WebSocket streaming for symbols: %s", self.symbols)
        return self.ws_thread

    def _logging_loop(self):
        """
        Loop that logs the current best bid and ask for each symbol every 1 second.
        Logging is performed in a separate thread so that it does not slow down publishing.
        """
        logging.info("Starting periodic order book logging...")
        while self.logging_running:
            for symbol, order_book in self.order_book.items():
                try:
                    best_bid = order_book.bids.peekitem(0)[0] if order_book.bids else "N/A"
                except Exception as e:
                    best_bid = "N/A"
                    logging.error("Error retrieving best bid for %s: %s", symbol, e)
                try:
                    best_ask = order_book.asks.peekitem(0)[0] if order_book.asks else "N/A"
                except Exception as e:
                    best_ask = "N/A"
                    logging.error("Error retrieving best ask for %s: %s", symbol, e)
                logging.info("Order book for %s: Best Bid: %s, Best Ask: %s", symbol, best_bid, best_ask)
            time.sleep(1)
        logging.info("Stopped periodic order book logging.")

    def start_logging(self):
        """
        Start the order book logging thread.
        """
        self.logging_running = True
        self.logging_thread = threading.Thread(target=self._logging_loop)
        self.logging_thread.daemon = True
        self.logging_thread.start()

    def stop_logging(self):
        """
        Stop the order book logging thread.
        """
        self.logging_running = False
        if self.logging_thread is not None:
            self.logging_thread.join(timeout=2)

    def start(self, block=True):
        """
        Start the Coinbase streamer.
        If block is True, this method will block until a KeyboardInterrupt.
        If block is False, it will return immediately.
        """
        self.start_streaming()
        self.start_logging()
        logging.info("Coinbase streamer is running.")
        if block:
            try:
                while True:
                    time.sleep(1)
            except KeyboardInterrupt:
                self.end()

    def end(self):
        """
        Stop the Coinbase streamer and clean up resources.
        """
        logging.info("Stopping Coinbase streamer...")
        self.stop_logging()
        if self.ws_app is not None:
            self.ws_app.keep_running = False
            self.ws_app.close()
        self.publisher_thread.stop()
        self.publisher.close()
        self.context.term()
        if self.ws_thread is not None:
            self.ws_thread.join(timeout=2)
        logging.info("Coinbase streamer stopped.")


if __name__ == "__main__":
    # Example usage: subscribe to multiple symbols.
    symbols = ["BTC-USD", "ETH-USD"]
    streamer = CoinbaseStreamer(
        api_key=CONFIG["exchanges"]["coinbase"]["api_key"],
        secret_key=CONFIG["exchanges"]["coinbase"]["secret_key"],
        ws_url=CONFIG["exchanges"]["coinbase"]["ws_url"],
        symbols=symbols,
        topic_prefix=CONFIG["exchanges"]["coinbase"]["topic_prefix"],
        zmq_port=CONFIG["exchanges"]["coinbase"]["zmq_port"]
    )
    # Start in non-blocking mode so the main thread can continue.
    streamer.start(block=False)
    # Let it run for 60 seconds (adjust as needed), then shut it down.
    time.sleep(60)
    streamer.end()
