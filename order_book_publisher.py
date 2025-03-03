#!/usr/bin/env python3
"""
This module defines an abstract base class `Streamer` for exchange streamers and
child classes for Coinbase, Binance, OKX, and Bybit. Each streamer:
  - Connects to the exchange’s WebSocket,
  - Subscribes using an exchange-specific message,
  - Processes incoming messages to update a local order book,
  - Publishes the updated order book via ZeroMQ with three timestamps:
      * timeExchange: The time provided by the exchange,
      * timeReceived: The time when the message was received by our system (UTC, ISO 8601, microseconds),
      * timePublished: The time when the order book was published (UTC, ISO 8601, microseconds).

The code also includes a dedicated publisher thread for asynchronous publishing and
a periodic logging thread that logs the current best bid/ask for each symbol.
"""

import abc
import datetime
import hashlib
import logging
import os
import queue
import threading
import time

import jwt
import websocket
import zmq
from sortedcontainers import SortedDict  # Used for maintaining order book in sorted order

try:
    # Use orjson if available for faster JSON parsing/serialization.
    import orjson as json_parser


    def dumps(obj):
        return json_parser.dumps(obj).decode("utf-8")


    loads = json_parser.loads
except ImportError:
    import json as json_parser

    dumps = json_parser.dumps
    loads = json_parser.loads

from config import EXCHANGE

# Configure logging. Adjust the level (DEBUG for development, INFO for production).
logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)

# For Coinbase, we use the "level2" channel. (Other exchanges use different subscription methods.)
CHANNEL = "level2"


# =============================================================================
# Common Components
# =============================================================================

class OrderBook:
    """
    Maintains an order book with separate SortedDicts for bids and asks.
    Bids are stored in descending order (using a key function that negates the price)
    and asks in ascending order.
    """

    def __init__(self):
        self.bids = SortedDict(lambda x: -x)  # Highest bid at index 0
        self.asks = SortedDict()  # Lowest ask at index 0

    def update_order(self, price: float, quantity: float, side: str):
        """
        Update a price level in the order book.
        If quantity is 0.0, remove that level; otherwise, update (or add) the level.
        """
        book = self.bids if side.lower() == "bid" else self.asks
        if quantity == 0.0:
            if price in book:
                del book[price]
                logging.debug("Removed %s at price %s", side, price)
        else:
            book[price] = quantity
            logging.debug("Set %s at price %s to quantity %s", side, price, quantity)


class PublisherThread(threading.Thread):
    """
    Runs in a separate thread to publish messages via ZeroMQ.
    It decouples the publishing process from the main processing thread using a thread-safe queue.
    """

    def __init__(self, publisher):
        super().__init__()
        self.publisher = publisher
        self.queue = queue.Queue()
        self.daemon = True  # Ensure thread terminates when main program exits.
        self.running = True

    def run(self):
        # Check the queue frequently (every 0.05 seconds).
        while self.running:
            try:
                msg = self.queue.get(timeout=0.05)
                self.publisher.send_json(msg)
                self.queue.task_done()
                logging.debug("Published message: %s", msg.get("topic"))
            except queue.Empty:
                continue

    def publish(self, msg):
        # Add a message to the queue for publishing.
        self.queue.put(msg)

    def stop(self):
        self.running = False


# =============================================================================
# Abstract Base Class: Streamer
# =============================================================================

class Streamer(abc.ABC):
    """
    Abstract base class for exchange streamers.
    Provides common functionality for:
      - Managing the ZeroMQ publisher and order book.
      - Starting/stopping the WebSocket connection and logging threads.
      - Publishing the order book with three timestamps.
    Child classes must implement subscribe, websocket_handler, and update_order_book.
    """

    def __init__(self, ws_url, symbols, topic_prefix, zmq_port):
        self.ws_url = ws_url
        self.symbols = symbols
        self.topic_prefix = topic_prefix
        self.zmq_port = zmq_port

        # Set up ZeroMQ publisher.
        self.context = zmq.Context()
        self.publisher = self.context.socket(zmq.PUB)
        self.publisher.bind(f"tcp://*:{self.zmq_port}")
        logging.info("%s: ZeroMQ publisher bound on port %s", self.__class__.__name__, self.zmq_port)

        # Start the publisher thread.
        self.publisher_thread = PublisherThread(self.publisher)
        self.publisher_thread.start()

        # Initialize an order book for each symbol.
        self.order_book = {symbol: OrderBook() for symbol in symbols}
        self.ws_app = None
        self.ws_thread = None

        # For periodic logging.
        self.logging_running = False
        self.logging_thread = None

    @abc.abstractmethod
    def subscribe(self, ws):
        """
        Send a subscription message over the WebSocket.
        Child classes should implement this based on the exchange's API.
        """
        pass

    @abc.abstractmethod
    def websocket_handler(self, ws, message):
        """
        Process an incoming WebSocket message.
        Child classes should implement this based on the exchange's message format.
        """
        pass

    @abc.abstractmethod
    def update_order_book(self, data, timeReceived):
        """
        Update the local order book based on the received data.
        Must extract the exchange timestamp, process the updates, and publish the order book.
        """
        pass

    def _publish_order_book(self, symbol, timeExchange, timeReceived, timePublished):
        """
        Enqueue the updated order book for a symbol, along with three timestamps, for publishing via ZeroMQ.
        """
        order_book_instance = self.order_book[symbol]
        published_data = {
            "bids": list(order_book_instance.bids.items()),
            "asks": list(order_book_instance.asks.items()),
            "timeExchange": timeExchange,  # Timestamp from exchange
            "timeReceived": timeReceived,  # Timestamp when received (UTC)
            "timePublished": timePublished  # Timestamp after processing (UTC)
        }
        message = {
            "topic": f"{self.topic_prefix}_{symbol}",
            "data": {**published_data, "exchange": self.topic_prefix.replace("ORDERBOOK_", "").upper(),
                     "symbol": symbol}
        }
        self.publisher_thread.publish(message)
        logging.debug("%s: Enqueued order book update for symbol %s", self.__class__.__name__, symbol)

    def start_streaming(self):
        """
        Start the WebSocket connection in a separate thread.
        """
        self.ws_app = websocket.WebSocketApp(
            self.ws_url,
            on_open=lambda ws: self.subscribe(ws),
            on_message=self.websocket_handler
        )
        self.ws_thread = threading.Thread(target=self.ws_app.run_forever)
        self.ws_thread.daemon = True
        self.ws_thread.start()
        logging.info("%s: Started WebSocket streaming for symbols: %s", self.__class__.__name__, self.symbols)
        return self.ws_thread

    def _logging_loop(self):
        """
        Periodically log the current best bid and ask for each symbol every 1 second.
        Runs in a separate thread to avoid slowing down the main processing.
        """
        logging.info("%s: Starting periodic order book logging...", self.__class__.__name__)
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
                logging.info("%s: Order book for %s: Best Bid: %s, Best Ask: %s",
                             self.__class__.__name__, symbol, best_bid, best_ask)
            time.sleep(1)
        logging.info("%s: Stopped periodic order book logging.", self.__class__.__name__)

    def start_logging(self):
        """
        Start the periodic logging thread.
        """
        self.logging_running = True
        self.logging_thread = threading.Thread(target=self._logging_loop)
        self.logging_thread.daemon = True
        self.logging_thread.start()

    def stop_logging(self):
        """
        Stop the periodic logging thread.
        """
        self.logging_running = False
        if self.logging_thread:
            self.logging_thread.join(timeout=2)

    def start(self, block=True):
        """
        Start the streamer:
          - Launch the WebSocket streaming thread.
          - Launch the periodic logging thread.
          - If block is True, block the main thread until a KeyboardInterrupt.
        """
        self.start_streaming()
        self.start_logging()
        logging.info("%s streamer is running.", self.__class__.__name__)
        if block:
            try:
                while True:
                    time.sleep(1)
            except KeyboardInterrupt:
                self.end()

    def end(self):
        """
        Stop the streamer and clean up all resources.
        """
        logging.info("Stopping %s streamer...", self.__class__.__name__)
        self.stop_logging()
        if self.ws_app is not None:
            self.ws_app.keep_running = False
            self.ws_app.close()
        self.publisher_thread.stop()
        self.publisher.close()
        self.context.term()
        if self.ws_thread is not None:
            self.ws_thread.join(timeout=2)
        logging.info("%s streamer stopped.", self.__class__.__name__)


# =============================================================================
# Exchange-Specific Implementations
# =============================================================================

class CoinbaseStreamer(Streamer):
    """
    Implementation for Coinbase.
    Expected incoming data structure (for order book updates):
      - Keys: 'channel', 'client_id', 'timestamp', 'sequence_num', 'events'
      - Each event in 'events' has keys: type, product_id, updates.
      - Each update in event['updates'] has: side, price_level, new_quantity.
    """

    def generate_jwt(self, message, channel):
        timestamp = int(time.time())
        payload = {
            "iss": "coinbase-cloud",
            "nbf": timestamp,
            "exp": timestamp + 120,
            "sub": EXCHANGE["coinbase"]["api_key"],
        }
        headers = {
            "kid": EXCHANGE["coinbase"]["api_key"],
            "nonce": hashlib.sha256(os.urandom(16)).hexdigest()
        }
        token = jwt.encode(payload, EXCHANGE["coinbase"]["secret_key"], algorithm="ES256", headers=headers)
        message["jwt"] = token
        logging.debug("Coinbase: JWT generated for channel %s", channel)
        return message

    def subscribe(self, ws):
        # Coinbase uses the CHANNEL "level2" and expects a list of product IDs.
        message = {
            "type": "subscribe",
            "channel": CHANNEL,
            "product_ids": self.symbols
        }
        signed_message = self.generate_jwt(message, CHANNEL)
        ws.send(dumps(signed_message))
        logging.info("Coinbase: Sent subscription message for products %s on channel %s", self.symbols, CHANNEL)

    def websocket_handler(self, ws, message):
        logging.debug("Coinbase: WebSocket message received: %s", message)
        if not isinstance(message, str):
            logging.debug("Coinbase: Received non-string message: %s", message)
            return
        try:
            # Record timeReceived in UTC with microsecond precision.
            timeReceived = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec='microseconds')
            data = loads(message)
            self.update_order_book(data, timeReceived)
        except Exception as e:
            logging.error("Coinbase: Error processing WebSocket message: %s", e)

    def update_order_book(self, data, timeReceived):
        """
        Process Coinbase messages using the 'events' key.
        """
        timeExchange = data.get("timestamp")  # Assume this is in UTC
        if "events" in data:
            for event in data["events"]:
                product_id = event.get("product_id")
                if not product_id or product_id not in self.order_book:
                    logging.debug("Coinbase: Skipping event for product %s", product_id)
                    continue

                order_book_instance = self.order_book[product_id]
                event_type = event.get("type", "").lower()
                # For a snapshot, clear the order book.
                if event_type == "snapshot":
                    order_book_instance.bids.clear()
                    order_book_instance.asks.clear()
                    logging.debug("Coinbase: Cleared order book for %s due to snapshot.", product_id)

                updates = event.get("updates", [])
                for upd in updates:
                    side = upd.get("side", "").lower()
                    price = upd.get("price_level")
                    quantity = upd.get("new_quantity")
                    if not (side and price is not None and quantity is not None):
                        logging.debug("Coinbase: Skipping update with insufficient data: %s", upd)
                        continue
                    try:
                        order_book_instance.update_order(float(price), float(quantity), side)
                    except Exception as e:
                        logging.error("Coinbase: Error updating order at price %s for product %s: %s", price,
                                      product_id, e)
                # Record timePublished.
                timePublished = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec='microseconds')
                self._publish_order_book(product_id, timeExchange, timeReceived, timePublished)
        else:
            # Fallback if 'events' key is missing.
            symbol = data.get("product_id") or data.get("instrument_id") or data.get("symbol")
            if not symbol or symbol not in self.order_book:
                logging.debug("Coinbase: Skipping data for symbol %s", symbol)
                return
            order_book_instance = self.order_book[symbol]
            if "bids" in data:
                for price, quantity in data["bids"]:
                    try:
                        order_book_instance.update_order(float(price), float(quantity), "bid")
                    except Exception as e:
                        logging.error("Coinbase: Error updating bid at price %s for symbol %s: %s", price, symbol, e)
            if "asks" in data:
                for price, quantity in data["asks"]:
                    try:
                        order_book_instance.update_order(float(price), float(quantity), "ask")
                    except Exception as e:
                        logging.error("Coinbase: Error updating ask at price %s for symbol %s: %s", price, symbol, e)
            timePublished = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec='microseconds')
            self._publish_order_book(symbol, timeExchange, timeReceived, timePublished)


class BinanceStreamer(Streamer):
    """
    Implementation for Binance.
    Expected message format:
      {
         "e": "depthUpdate",
         "E": 1648336797123,  # Event time in ms
         "s": "BTCUSDT",
         "b": [["85700.00", "0.5"], ...],  # Bids
         "a": [["85710.00", "1.2"], ...]   # Asks
      }
    """

    def subscribe(self, ws):
        # Binance uses a subscription message via JSON.
        message = {
            "method": "SUBSCRIBE",
            "params": [f"{symbol.lower()}@depth@100ms" for symbol in self.symbols],
            "id": 1
        }
        ws.send(dumps(message))
        logging.info("Binance: Sent subscription message: %s", message)

    def websocket_handler(self, ws, message):
        logging.debug("Binance: WebSocket message received: %s", message)
        if not isinstance(message, str):
            logging.debug("Binance: Received non-string message: %s", message)
            return
        try:
            timeReceived = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec='microseconds')
            data = loads(message)
            self.update_order_book(data, timeReceived)
        except Exception as e:
            logging.error("Binance: Error processing WebSocket message: %s", e)

    def update_order_book(self, data, timeReceived):
        if data.get("e") != "depthUpdate":
            logging.debug("Binance: Ignoring event type: %s", data.get("e"))
            return
        # Convert event time (in ms) to an ISO timestamp.
        timeExchange = datetime.datetime.fromtimestamp(data.get("E") / 1000, datetime.timezone.utc).isoformat(
            timespec='microseconds')
        symbol = data.get("s")
        if not symbol or symbol not in self.order_book:
            logging.debug("Binance: Skipping data for symbol %s", symbol)
            return
        order_book_instance = self.order_book[symbol]
        if "b" in data:
            for price, quantity in data["b"]:
                try:
                    order_book_instance.update_order(float(price), float(quantity), "bid")
                except Exception as e:
                    logging.error("Binance: Error updating bid at price %s for %s: %s", price, symbol, e)
        if "a" in data:
            for price, quantity in data["a"]:
                try:
                    order_book_instance.update_order(float(price), float(quantity), "ask")
                except Exception as e:
                    logging.error("Binance: Error updating ask at price %s for %s: %s", price, symbol, e)
        timePublished = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec='microseconds')
        self._publish_order_book(symbol, timeExchange, timeReceived, timePublished)

    def _publish_order_book(self, symbol, timeExchange, timeReceived, timePublished):
        order_book_instance = self.order_book[symbol]
        published_data = {
            "bids": list(order_book_instance.bids.items()),
            "asks": list(order_book_instance.asks.items()),
            "timeExchange": timeExchange,
            "timeReceived": timeReceived,
            "timePublished": timePublished
        }
        message = {
            "topic": f"ORDERBOOK_BINANCE_{symbol}",
            "data": {**published_data, "exchange": "BINANCE", "symbol": symbol}
        }
        self.publisher_thread.publish(message)
        logging.debug("Binance: Enqueued order book update for symbol %s", symbol)


class OkxStreamer(Streamer):
    """
    Implementation for OKX.
    Expected message format:
      {
         "arg": {"channel": "books5", "instId": "BTC-USD"},
         "data": [{
             "asks": [["85700.00", "1"], ...],
             "bids": [["85690.00", "0.5"], ...],
             "ts": "1648336797123"
         }]
      }
    """

    def subscribe(self, ws):
        message = {
            "op": "subscribe",
            "args": [f"books5:{symbol}" for symbol in self.symbols]
        }
        ws.send(dumps(message))
        logging.info("OKX: Sent subscription message: %s", message)

    def websocket_handler(self, ws, message):
        logging.debug("OKX: WebSocket message received: %s", message)
        if not isinstance(message, str):
            logging.debug("OKX: Received non-string message: %s", message)
            return
        try:
            timeReceived = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec='microseconds')
            data = loads(message)
            self.update_order_book(data, timeReceived)
        except Exception as e:
            logging.error("OKX: Error processing WebSocket message: %s", e)

    def update_order_book(self, data, timeReceived):
        if "data" not in data or not data["data"]:
            logging.debug("OKX: No data in message; skipping")
            return
        data_item = data["data"][0]
        ts = data_item.get("ts")
        if ts:
            timeExchange = datetime.datetime.fromtimestamp(float(ts) / 1000, datetime.timezone.utc).isoformat(
                timespec='microseconds')
        else:
            timeExchange = "N/A"
        symbol = data.get("arg", {}).get("instId")
        if not symbol or symbol not in self.order_book:
            logging.debug("OKX: Symbol %s not in subscription list; skipping", symbol)
            return
        order_book_instance = self.order_book[symbol]
        if "bids" in data_item:
            for price, quantity in data_item["bids"]:
                try:
                    order_book_instance.update_order(float(price), float(quantity), "bid")
                except Exception as e:
                    logging.error("OKX: Error updating bid at price %s for %s: %s", price, symbol, e)
        if "asks" in data_item:
            for price, quantity in data_item["asks"]:
                try:
                    order_book_instance.update_order(float(price), float(quantity), "ask")
                except Exception as e:
                    logging.error("OKX: Error updating ask at price %s for %s: %s", price, symbol, e)
        timePublished = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec='microseconds')
        self._publish_order_book(symbol, timeExchange, timeReceived, timePublished)

    def _publish_order_book(self, symbol, timeExchange, timeReceived, timePublished):
        order_book_instance = self.order_book[symbol]
        published_data = {
            "bids": list(order_book_instance.bids.items()),
            "asks": list(order_book_instance.asks.items()),
            "timeExchange": timeExchange,
            "timeReceived": timeReceived,
            "timePublished": timePublished
        }
        message = {
            "topic": f"ORDERBOOK_OKX_{symbol}",
            "data": {**published_data, "exchange": "OKX", "symbol": symbol}
        }
        self.publisher_thread.publish(message)
        logging.debug("OKX: Enqueued order book update for symbol %s", symbol)


class BybitStreamer(Streamer):
    """
    Implementation for Bybit.
    Expected message format:
      {
         "topic": "orderBookL2_25.BTCUSD",
         "data": [{
             "price": "85700.00",
             "side": "Buy",
             "size": "0.5"
         }],
         "ts": 1648336797123
      }
    """

    def subscribe(self, ws):
        # Construct subscription message for Bybit; symbols without dash.
        message = {
            "op": "subscribe",
            "args": [f"orderBookL2_25.{symbol.replace('-', '')}" for symbol in self.symbols]
        }
        ws.send(dumps(message))
        logging.info("Bybit: Sent subscription message: %s", message)

    def websocket_handler(self, ws, message):
        logging.debug("Bybit: WebSocket message received: %s", message)
        if not isinstance(message, str):
            logging.debug("Bybit: Received non-string message: %s", message)
            return
        try:
            timeReceived = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec='microseconds')
            data = loads(message)
            self.update_order_book(data, timeReceived)
        except Exception as e:
            logging.error("Bybit: Error processing WebSocket message: %s", e)

    def update_order_book(self, data, timeReceived):
        if "data" not in data or not data["data"]:
            logging.debug("Bybit: No data in message; skipping")
            return
        ts = data.get("ts")
        if ts:
            timeExchange = datetime.datetime.fromtimestamp(float(ts) / 1000, datetime.timezone.utc).isoformat(
                timespec='microseconds')
        else:
            timeExchange = "N/A"
        # Extract symbol from topic (e.g., "orderBookL2_25.BTCUSD")
        topic = data.get("topic", "")
        parts = topic.split(".")
        symbol = parts[1] if len(parts) > 1 else None
        if not symbol or symbol not in self.order_book:
            logging.debug("Bybit: Symbol %s not in subscription list; skipping", symbol)
            return
        order_book_instance = self.order_book[symbol]
        for entry in data["data"]:
            side = entry.get("side", "").lower()
            # Map 'buy' to 'bid' and 'sell' to 'ask'
            if side == "buy":
                side = "bid"
            elif side == "sell":
                side = "ask"
            price = entry.get("price")
            quantity = entry.get("size")
            if not (side and price is not None and quantity is not None):
                logging.debug("Bybit: Skipping entry with insufficient data: %s", entry)
                continue
            try:
                order_book_instance.update_order(float(price), float(quantity), side)
            except Exception as e:
                logging.error("Bybit: Error updating order at price %s for symbol %s: %s", price, symbol, e)
        timePublished = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec='microseconds')
        self._publish_order_book(symbol, timeExchange, timeReceived, timePublished)

    def _publish_order_book(self, symbol, timeExchange, timeReceived, timePublished):
        order_book_instance = self.order_book[symbol]
        published_data = {
            "bids": list(order_book_instance.bids.items()),
            "asks": list(order_book_instance.asks.items()),
            "timeExchange": timeExchange,
            "timeReceived": timeReceived,
            "timePublished": timePublished
        }
        message = {
            "topic": f"ORDERBOOK_BYBIT_{symbol}",
            "data": {**published_data, "exchange": "BYBIT", "symbol": symbol}
        }
        self.publisher_thread.publish(message)
        logging.debug("Bybit: Enqueued order book update for symbol %s", symbol)


# =============================================================================
# Main: Select which streamer to run.
# =============================================================================

if __name__ == "__main__":
    # Instantiate each streamer with its own ZeroMQ port.
    coinbase_streamer = CoinbaseStreamer(
        ws_url=EXCHANGE["coinbase"]["ws_url"],
        symbols=["BTC-USD", "ETH-USD"],
        topic_prefix=EXCHANGE["coinbase"]["topic_prefix"],
        zmq_port=EXCHANGE["coinbase"]["zmq_port"]
    )

    binance_streamer = BinanceStreamer(
        ws_url=EXCHANGE["binance"]["ws_url"],
        symbols=["BTCUSDT", "ETHUSDT"],
        topic_prefix=EXCHANGE["binance"]["topic_prefix"],
        zmq_port=EXCHANGE["binance"]["zmq_port"]
    )

    # Start each streamer in non-blocking mode.
    coinbase_thread = threading.Thread(target=coinbase_streamer.start, kwargs={'block': False})
    binance_thread = threading.Thread(target=binance_streamer.start, kwargs={'block': False})

    coinbase_thread.start()
    binance_thread.start()

    # Let them run for a while (e.g., 60 seconds).
    time.sleep(60)

    # End both streamers.
    coinbase_streamer.end()
    binance_streamer.end()

    # Join threads (optional, to ensure clean shutdown)
    coinbase_thread.join()
    binance_thread.join()
