#!/usr/bin/env python3
"""
position_publisher.py

This script implements an order update streamer framework using an abstract base class,
OrderStreamer, and four child classes for Coinbase, Binance, OKX, and Bybit.

For each exchange, the system:
  - Connects to the exchange’s order update WebSocket.
  - Records every raw order update message into a SQLite database.
  - Processes order updates to update aggregated positions.
  - Publishes only the current position for each symbol via ZeroMQ.

Timestamps used:
  - timeExchange: Provided by the exchange (or derived from the message).
  - timeReceived: Local UTC time when the message is received (ISO 8601, microseconds).
  - timePublished: Local UTC time immediately after processing (ISO 8601, microseconds).

All required parameters (ws_url, api_key, secret_key, symbols, topic_prefix, zmq_port, db_file) are initialized explicitly.
"""

import abc
import datetime
import hashlib
import json
import logging
import os
import queue
import sqlite3
import threading
import time
import zmq
import jwt
import websocket

from config import EXCHANGE

# Configure logging.
logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.DEBUG)

# For Coinbase, we use the "level2" channel. (Other exchanges use different subscription methods.)
CHANNEL = "user"


# =============================================================================
# PublisherThread: Publishes messages via ZeroMQ using a thread-safe Queue.
# =============================================================================
class PublisherThread(threading.Thread):
    def __init__(self, publisher):
        super().__init__()
        self.publisher = publisher
        self.queue = queue.Queue()  # Use queue.Queue() from the standard library.
        self.daemon = True
        self.running = True

    def run(self):
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


# =============================================================================
# DBRecorderMixin: Records every received raw update to a SQLite database.
# =============================================================================
class DBRecorderMixin:
    def __init__(self, db_file=None):
        self.db_file = db_file
        self.db_conn = None
        if self.db_file:
            # Allow the connection to be used across threads.
            self.db_conn = sqlite3.connect(
                self.db_file, check_same_thread=False,
                detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES
            )
            self._init_db()

    def _init_db(self):
        c = self.db_conn.cursor()
        c.execute('''
            CREATE TABLE IF NOT EXISTS position (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT,
                exchange TEXT,
                symbol TEXT,
                raw TEXT
            )
        ''')
        self.db_conn.commit()

    def _record_update(self, data):
        if self.db_conn:
            c = self.db_conn.cursor()
            ts = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec='microseconds')
            exchange = self.__class__.__name__.replace("OrderStreamer", "").upper()
            # Try to extract symbol from known fields.
            symbol = "Unknown"
            if "orders" in data and data["orders"]:
                symbol = data["orders"][0].get("product_id", "Unknown")
            elif "s" in data:
                symbol = data.get("s")
            elif "arg" in data and data["arg"].get("instId"):
                symbol = data["arg"]["instId"]
            raw = json.dumps(data)
            c.execute('''
                INSERT INTO position (timestamp, exchange, symbol, raw)
                VALUES (?, ?, ?, ?)
            ''', (ts, exchange, symbol, raw))
            self.db_conn.commit()

    def _close_db(self):
        if self.db_conn:
            self.db_conn.close()


# =============================================================================
# Abstract OrderStreamer Class
# =============================================================================
class PositionStreamer(abc.ABC, DBRecorderMixin):
    """
    Abstract base class for order update streamers.

    Parameters:
      - ws_url: WebSocket endpoint URL.
      - api_key: API key.
      - secret_key: API secret.
      - symbols: List of product symbols.
      - topic_prefix: ZeroMQ topic prefix for published messages.
      - zmq_port: ZeroMQ port to bind.
      - db_file: SQLite DB file to record all raw updates.
    """

    def __init__(self, ws_url, api_key, secret_key, symbols, topic_prefix, zmq_port, db_file=None):
        DBRecorderMixin.__init__(self, db_file)
        self.ws_url = ws_url
        self.api_key = api_key
        self.secret_key = secret_key
        self.symbols = symbols
        self.topic_prefix = topic_prefix
        self.zmq_port = zmq_port

        # Set up ZeroMQ publisher.
        self.context = zmq.Context()
        self.publisher = self.context.socket(zmq.PUB)
        self.publisher.bind(f"tcp://*:{self.zmq_port}")
        logging.info("%s: ZeroMQ publisher bound on port %s", self.__class__.__name__, self.zmq_port)
        self.publisher_thread = PublisherThread(self.publisher)
        self.publisher_thread.start()

        self.ws_app = None
        self.ws_thread = None

        # Maintain a dictionary for aggregated positions per symbol.
        self.positions = {}

    @abc.abstractmethod
    def subscribe(self, ws):
        """Send exchange-specific subscription message over the WebSocket."""
        pass

    @abc.abstractmethod
    def websocket_handler(self, ws, message):
        """Process incoming WebSocket messages."""
        pass

    @abc.abstractmethod
    def update_order_updates(self, data, timeReceived):
        """
        Process incoming order update data:
          - Record raw data to the DB.
          - Update aggregated position.
          - Publish only the position update.
        """
        pass

    def _publish_position(self, symbol):
        """Publish the current aggregated position for a symbol."""
        current_position = self.positions.get(symbol, 0)
        topic = f"POSITION_{self.__class__.__name__.replace('OrderStreamer', '').upper()}_{symbol}"
        message = {
            "topic": topic,
            "data": {
                "symbol": symbol,
                "position": current_position,
                "exchange": self.__class__.__name__.replace("OrderStreamer", "").upper()
            }
        }
        self.publisher_thread.publish(message)
        logging.debug("%s: Published position for %s: %s", self.__class__.__name__, symbol, current_position)

    def _update_position(self, symbol, order_update):
        """
        Update the aggregated position for a symbol based on order update data.
        Adjust field names as needed per exchange.
        """
        try:
            # For example, use "filled_size" (Coinbase) or "z" (Binance cumulative filled quantity).
            if "filled_size" in order_update:
                qty = float(order_update.get("filled_size", 0))
            elif "z" in order_update:
                qty = float(order_update.get("z", 0))
            else:
                qty = 0
        except Exception:
            qty = 0
        side = order_update.get("side", "").lower()
        if side in ["buy", "b"]:
            self.positions[symbol] = self.positions.get(symbol, 0) + qty
        elif side in ["sell", "s"]:
            self.positions[symbol] = self.positions.get(symbol, 0) - qty

    def start_streaming(self):
        """Start the WebSocket connection in a separate thread."""
        self.ws_app = websocket.WebSocketApp(
            self.ws_url,
            on_open=lambda ws: self.subscribe(ws),
            on_message=self.websocket_handler
        )
        self.ws_thread = threading.Thread(target=self.ws_app.run_forever)
        self.ws_thread.daemon = True
        self.ws_thread.start()
        logging.info("%s: Started WebSocket streaming.", self.__class__.__name__)
        return self.ws_thread

    def start(self, block=True):
        self.start_streaming()
        logging.info("%s streamer is running.", self.__class__.__name__)
        if block:
            try:
                while True:
                    time.sleep(1)
            except KeyboardInterrupt:
                self.end()

    def end(self):
        logging.info("Stopping %s streamer...", self.__class__.__name__)
        if self.ws_app is not None:
            self.ws_app.keep_running = False
            self.ws_app.close()
        self.publisher_thread.stop()
        self.publisher.close()
        self.context.term()
        if self.ws_thread is not None:
            self.ws_thread.join(timeout=2)
        self._close_db()
        logging.info("%s streamer stopped.", self.__class__.__name__)


# =============================================================================
# Child Class: CoinbaseOrderStreamer
# =============================================================================
class CoinbasePositionStreamer(PositionStreamer):
    """
    Coinbase order streamer implementation.

    According to Coinbase Advanced Trade User Channel documentation,
    a message includes:
      - "timestamp": Exchange-provided timestamp.
      - "orders": Array of order update objects (each with "order_id", "product_id", "status", "side", "filled_size", etc.).
      - Optionally, "positions" may be provided.

    In this implementation, all raw messages are recorded into the DB. The aggregated
    position is updated from order updates and then published.
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
        ws.send(json.dumps(signed_message))
        logging.info("Coinbase: Sent subscription message for products %s on channel %s", self.symbols, CHANNEL)

    def websocket_handler(self, ws, message):
        logging.debug("CoinbaseOrderStreamer: Received message: %s", message)
        if not isinstance(message, str):
            logging.debug("CoinbaseOrderStreamer: Ignoring non-string message.")
            return
        try:
            timeReceived = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec='microseconds')
            data = json.loads(message)
            self._record_update(data)
            self.update_order_updates(data, timeReceived)
        except Exception as e:
            logging.error("CoinbaseOrderStreamer: Error processing message: %s", e)

    def update_order_updates(self, data, timeReceived):
        timeExchange = data.get("timestamp")
        events = data.get("events", [])
        for event in events:
            orders = event.get("orders", [])
            if orders:
                for order in orders:
                    symbol = order.get("product_id", self.symbols[0])
                    self._update_position(symbol, order)
            # You can also process positions from the "positions" field if available.
        # After processing, publish the current aggregated position for each symbol.
        for symbol in self.symbols:
            self._publish_position(symbol)


# =============================================================================
# Child Class: BinanceOrderStreamer
# =============================================================================
class BinancePositionStreamer(PositionStreamer):
    """
    Binance order streamer implementation.

    Expected message format (executionReport):
      {
         "e": "executionReport",
         "E": 1648336797123,   # Event time in ms
         "s": "BTCUSDT",       # Symbol
         "i": 123456789,       # Order ID
         "S": "BUY" or "SELL", # Side
         "z": "0.5",           # Cumulative filled quantity
         ... (other fields)
      }
    """

    def subscribe(self, ws):
        params = [f"{symbol.lower()}@executionReport" for symbol in self.symbols]
        message = {
            "method": "SUBSCRIBE",
            "params": params,
            "id": 1
        }
        ws.send(json.dumps(message))
        logging.info("BinanceOrderStreamer: Sent subscription message: %s", message)

    def websocket_handler(self, ws, message):
        logging.debug("BinanceOrderStreamer: Received message: %s", message)
        if not isinstance(message, str):
            logging.debug("BinanceOrderStreamer: Ignoring non-string message.")
            return
        try:
            timeReceived = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec='microseconds')
            data = json.loads(message)
            self._record_update(data)
            self.update_order_updates(data, timeReceived)
        except Exception as e:
            logging.error("BinanceOrderStreamer: Error processing message: %s", e)

    def update_order_updates(self, data, timeReceived):
        if data.get("e") != "executionReport":
            logging.debug("BinanceOrderStreamer: Ignoring event type: %s", data.get("e"))
            return
        timeExchange = datetime.datetime.fromtimestamp(data.get("E") / 1000, datetime.timezone.utc).isoformat(
            timespec='microseconds')
        order_id = str(data.get("i", "Unknown"))
        symbol = data.get("s")
        self._update_position(symbol, data)
        timePublished = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec='microseconds')
        self._publish_position(symbol)


# =============================================================================
# Child Class: OkxOrderStreamer
# =============================================================================
class OkxPositionStreamer(PositionStreamer):
    """
    OKX order streamer implementation.

    Expected message format:
      {
         "arg": {"channel": "orders", "instId": "BTC-USD"},
         "data": [{
             "ordId": "123456",
             "side": "buy",         # or "sell"
             "filledQty": "0.5",     # Filled quantity
             ... (other fields)
         }],
         "ts": "1648336797123"
      }
    """

    def subscribe(self, ws):
        args = [f"orders:{symbol}" for symbol in self.symbols]
        message = {
            "op": "subscribe",
            "args": args
        }
        ws.send(json.dumps(message))
        logging.info("OkxOrderStreamer: Sent subscription message: %s", message)

    def websocket_handler(self, ws, message):
        logging.debug("OkxOrderStreamer: Received message: %s", message)
        if not isinstance(message, str):
            logging.debug("OkxOrderStreamer: Ignoring non-string message.")
            return
        try:
            timeReceived = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec='microseconds')
            data = json.loads(message)
            self._record_update(data)
            self.update_order_updates(data, timeReceived)
        except Exception as e:
            logging.error("OkxOrderStreamer: Error processing message: %s", e)

    def update_order_updates(self, data, timeReceived):
        if "data" not in data or not data["data"]:
            logging.debug("OkxOrderStreamer: No order update data; skipping.")
            return
        data_item = data["data"][0]
        ts = data_item.get("ts")
        if ts:
            timeExchange = datetime.datetime.fromtimestamp(float(ts) / 1000, datetime.timezone.utc).isoformat(
                timespec='microseconds')
        else:
            timeExchange = "N/A"
        symbol = data.get("arg", {}).get("instId")
        if not symbol:
            logging.debug("OkxOrderStreamer: No symbol found; skipping.")
            return
        self._update_position(symbol, data_item)
        timePublished = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec='microseconds')
        self._publish_position(symbol)


# =============================================================================
# Child Class: BybitOrderStreamer
# =============================================================================
class BybitPositionStreamer(PositionStreamer):
    """
    Bybit order streamer implementation.

    Expected message format:
      {
         "topic": "orderUpdate.BTCUSD",
         "data": [{
             "orderId": "abc123",
             "orderStatus": "Filled",
             "side": "Buy",  # or "Sell"
             "cumQty": "0.5",  # Cumulative filled quantity
             ... (other fields)
         }],
         "ts": 1648336797123
      }
    """

    def subscribe(self, ws):
        args = [f"orderUpdate.{symbol.replace('-', '')}" for symbol in self.symbols]
        message = {
            "op": "subscribe",
            "args": args
        }
        ws.send(json.dumps(message))
        logging.info("BybitOrderStreamer: Sent subscription message: %s", message)

    def websocket_handler(self, ws, message):
        logging.debug("BybitOrderStreamer: Received message: %s", message)
        if not isinstance(message, str):
            logging.debug("BybitOrderStreamer: Ignoring non-string message.")
            return
        try:
            timeReceived = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec='microseconds')
            data = json.loads(message)
            self._record_update(data)
            self.update_order_updates(data, timeReceived)
        except Exception as e:
            logging.error("BybitOrderStreamer: Error processing message: %s", e)

    def update_order_updates(self, data, timeReceived):
        if "data" not in data or not data["data"]:
            logging.debug("BybitOrderStreamer: No order update data; skipping.")
            return
        ts = data.get("ts")
        if ts:
            timeExchange = datetime.datetime.fromtimestamp(float(ts) / 1000, datetime.timezone.utc).isoformat(
                timespec='microseconds')
        else:
            timeExchange = "N/A"
        topic = data.get("topic", "")
        parts = topic.split(".")
        symbol = parts[1] if len(parts) > 1 else None
        if not symbol:
            logging.debug("BybitOrderStreamer: Unable to extract symbol from topic; skipping.")
            return
        self._update_position(symbol, data["data"][0])
        timePublished = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec='microseconds')
        self._publish_position(symbol)


# =============================================================================
# Main: Running the Chosen OrderStreamer
# =============================================================================
if __name__ == "__main__":
    import sys

    # Choose the exchange via command-line argument (default to "bybit" if not provided)
    exchange_choice = sys.argv[1] if len(sys.argv) > 1 else "bybit"
    streamers = {
        "coinbase": CoinbasePositionStreamer,
        "binance": BinancePositionStreamer,
        "okx": OkxPositionStreamer,
        "bybit": BybitPositionStreamer
    }
    exchange_config = EXCHANGE.get(exchange_choice)
    if not exchange_config:
        logging.error("No configuration found for exchange: %s", exchange_choice)
        sys.exit(1)

    # Set default product IDs if not provided.
    if exchange_choice == "coinbase":
        product_ids = ["BTC-USD", "ETH-USD"]
    elif exchange_choice == "binance":
        product_ids = ["BTCUSDT", "ETHUSDT"]
    elif exchange_choice == "okx":
        product_ids = ["BTC-USD", "ETH-USD"]
    elif exchange_choice == "bybit":
        product_ids = ["BTCUSD", "ETHUSD"]

    exchange_config["product_ids"] = product_ids

    # Instantiate the chosen streamer.
    # We optionally offset the ZeroMQ port (e.g., +10) for order updates.
    streamer = streamers[exchange_choice](
        ws_url=exchange_config["ws_url"],
        api_key=exchange_config["api_key"],
        secret_key=exchange_config["secret_key"],
        symbols=product_ids,
        topic_prefix=f"ORDER_UPDATE_{exchange_choice.upper()}",
        zmq_port=exchange_config["zmq_port"] + 10,
        db_file="position.db"
    )
    streamer.start(block=False)
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        streamer.end()
