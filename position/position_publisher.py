#!/usr/bin/env python3

import abc
import datetime
import hashlib
import json
import logging
import os
import time

import jwt

from config import EXCHANGE_CONFIG
from archive.tcp_publisher import Publisher


# =============================================================================
# Abstract PositionPublisher Class
# =============================================================================
class PositionPublisher(Publisher):

    def __init__(self, ws_url, api_key, secret_key, symbols, exchange, zmq_port):
        super().__init__(ws_url, api_key, secret_key, symbols, exchange, zmq_port)

        # Maintain a dictionary for aggregated positions per symbol.
        self.positions = {}

    @abc.abstractmethod
    def update_order_updates(self, data, timeReceived):
        pass

    def publish_position(self, symbol, timeExchange, timeReceived, timePublished):
        current_position = self.positions.get(symbol, 0)
        published_data = {
            "position": current_position,
            "timeExchange": timeExchange,
            "timeReceived": timeReceived,
            "timePublished": timePublished
        }
        message = {
            "topic": f"POSITION_{self.exchange}_{symbol}",
            "data": {
                **published_data,
                "symbol": symbol,
                "exchange": self.exchange
            }
        }
        self.publisher_thread.publish(message)
        logging.debug("%s: Published position for %s: %s", self.__class__.__name__, symbol, current_position)

    def update_position(self, symbol, order_update):
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

    def logging_loop(self):
        """
        Periodically log the aggregated position for each symbol.
        """
        logging.info("%s: Starting periodic position logging...", self.__class__.__name__)
        while self.logging_running:
            for symbol in self.symbols:
                pos = self.positions.get(symbol, 0)
                logging.info("%s: Position for %s: %s", self.__class__.__name__, symbol, pos)
            time.sleep(1)
        logging.info("%s: Stopped periodic position logging.", self.__class__.__name__)


# =============================================================================
# Child Class: CoinbaseOrderStreamer
# =============================================================================
class CoinbasePositionPublisher(PositionPublisher):
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
            "sub": self.api_key,
        }
        headers = {
            "kid": self.api_key,
            "nonce": hashlib.sha256(os.urandom(16)).hexdigest()
        }
        token = jwt.encode(payload, self.secret_key, algorithm="ES256", headers=headers)
        message["jwt"] = token
        logging.debug("Coinbase: JWT generated for channel %s", channel)
        return message

    def subscribe(self, ws):
        # Coinbase uses the CHANNEL "level2" and expects a list of product IDs.
        message = {
            "type": "subscribe",
            "channel": "level2",
            "product_ids": self.symbols
        }
        signed_message = self.generate_jwt(message, "level2")
        ws.send(json.dumps(signed_message))
        logging.info("%s: Sent subscription message for products %s on channel %s", self.__class__.__name__, self.symbols, "level2")

    def websocket_handler(self, ws, message):
        logging.debug("%s: Received message: %s", self.__class__.__name__, message)
        if not isinstance(message, str):
            logging.debug("%s: Ignoring non-string message.", self.__class__.__name__, )
            return
        try:
            timeReceived = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec='microseconds')
            data = json.loads(message)
            self.update_order_updates(data, timeReceived)
        except Exception as e:
            logging.error("%s: Error processing message: %s",self.__class__.__name__,  e)

    def update_order_updates(self, data, timeReceived):
        timeExchange = data.get("timestamp")
        events = data.get("events", [])
        for event in events:
            orders = event.get("orders", [])
            if orders:
                for order in orders:
                    symbol = order.get("product_id", self.symbols[0])
                    self.update_position(symbol, order)
            # You can also process positions from the "positions" field if available.
        # After processing, publish the current aggregated position for each symbol.
        timePublished = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec='microseconds')
        for symbol in self.symbols:
            self.publish_position(symbol, timeExchange, timeReceived, timePublished)


# =============================================================================
# Child Class: BinanceOrderStreamer
# =============================================================================
class BinancePositionPublisher(PositionPublisher):
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
        self.update_position(symbol, data)
        timePublished = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec='microseconds')
        self.publish_position(symbol, timeExchange, timeReceived, timePublished)


# =============================================================================
# Child Class: OkxOrderStreamer
# =============================================================================
class OkxPositionPublisher(PositionPublisher):
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
        self.update_position(symbol, data_item)
        timePublished = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec='microseconds')
        self.publish_position(symbol, timeExchange, timeReceived, timePublished)


# =============================================================================
# Child Class: BybitOrderStreamer
# =============================================================================
class BybitPositionPublisher(PositionPublisher):
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
        self.update_position(symbol, data["data"][0])
        timePublished = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec='microseconds')
        self.publish_position(symbol, timeExchange, timeReceived, timePublished)


# =============================================================================
# Main: Running the Chosen OrderStreamer
# =============================================================================
if __name__ == "__main__":
    import sys

    # Choose the exchange via command-line argument (default to "bybit" if not provided)
    exchange_choice = sys.argv[1] if len(sys.argv) > 1 else "bybit"
    streamers = {
        "coinbase": CoinbasePositionPublisher,
        "binance": BinancePositionPublisher,
        "okx": OkxPositionPublisher,
        "bybit": BybitPositionPublisher
    }
    exchange_config = EXCHANGE_CONFIG.get(exchange_choice)
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
        topic_prefix=f"POSITION_{exchange_choice.upper()}",
        zmq_port=exchange_config["zmq_port"] + 10,
    )
    streamer.start(block=False)
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        streamer.end()
