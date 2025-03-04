#!/usr/bin/env python3
"""
This module defines an abstract base class `OrderBookPublisher` for exchange order book publishers and
child classes for Coinbase, Binance, OKX, and Bybit. Each order book publisher:
  - Connects to the exchange’s WebSocket,
  - Subscribes using an exchange-specific message,
  - Processes incoming messages to update a local order book,
  - Publishes the updated order book via ZeroMQ with three timestamps:
      * timeExchange: The time provided by the exchange,
      * timeReceived: The time when the message was received by our system (UTC, ISO 8601, microseconds),
      * timePublished: The time when the order book was published (UTC, ISO 8601, microseconds).

Additionally, the code includes a dedicated publisher thread for asynchronous publishing and
a periodic logging thread that logs the current best bid/ask for each symbol.
"""

import abc
import datetime
import hashlib
import logging
import os
import threading
import time

import jwt
from sortedcontainers import SortedDict  # Maintain order book in sorted order

from core.publisher import Publisher  # Base publisher for ZeroMQ publishing

# Try to use orjson for faster JSON processing. Fallback to standard json if unavailable.
try:
    import orjson as json_parser


    def dumps(obj):
        """Serialize an object to a JSON formatted str using orjson."""
        return json_parser.dumps(obj).decode("utf-8")


    loads = json_parser.loads
except ImportError:
    import json as json_parser

    dumps = json_parser.dumps
    loads = json_parser.loads

from config import EXCHANGE_CONFIG


# =============================================================================
# Order Book Components
# =============================================================================
class OrderBook:
    """
    Maintains the order book for a symbol with separate sorted structures for bids and asks.
    """

    def __init__(self):
        # For bids, higher prices come first. For asks, lower prices come first.
        self.bids = SortedDict(lambda price: -price)  # Highest bid at index 0
        self.asks = SortedDict()  # Lowest ask at index 0

    def update_order(self, price: float, quantity: float, side: str):
        """
        Update a price level in the order book.

        Args:
            price (float): The price level to update.
            quantity (float): The quantity at this price level. If 0.0, the level is removed.
            side (str): "bid" or "ask" indicating which side of the book.
        """
        book = self.bids if side.lower() == "bid" else self.asks
        if quantity == 0.0:
            if price in book:
                del book[price]
                logging.debug("Removed %s at price %s", side, price)
        else:
            book[price] = quantity
            logging.debug("Set %s at price %s to quantity %s", side, price, quantity)


# =============================================================================
# Abstract Base Class: OrderBookPublisher
# =============================================================================
class OrderBookPublisher(Publisher):
    """
    Abstract publisher that handles order book updates for multiple symbols.
    Inherits from a base Publisher class that manages ZeroMQ publishing.
    """

    def __init__(self, ws_url, api_key, secret_key, symbols, exchange, zmq_port):
        """
        Initialize the OrderBookPublisher.

        Args:
            ws_url (str): WebSocket endpoint URL.
            api_key (str): API key for authentication.
            secret_key (str): Secret key for authentication.
            symbols (list): List of symbols to subscribe to.
            exchange (str): Prefix for ZeroMQ topics.
            zmq_port (int): Port number for ZeroMQ publisher.
        """
        super().__init__(ws_url, api_key, secret_key, symbols, exchange, zmq_port)

        # Create an order book instance for each symbol.
        self.order_book = {symbol: OrderBook() for symbol in symbols}

        # Initialize logging control flags.
        self.logging_running = False
        self.logging_thread = None

    @abc.abstractmethod
    def update_order_book(self, data, timeReceived):
        """
        Process incoming data to update the order book.

        Args:
            data (dict): The incoming message data from the exchange.
            timeReceived (str): Timestamp when the message was received (ISO 8601, UTC).
        """
        pass

    def publish_order_book(self, symbol, timeExchange, timeReceived, timePublished):
        """
        Package the current order book data along with timestamps and enqueue it for publishing.

        Args:
            symbol (str): The symbol to publish.
            timeExchange (str): Timestamp from the exchange.
            timeReceived (str): Timestamp when the data was received.
            timePublished (str): Timestamp after processing.
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
            "topic": f"ORDERBOOK_{self.exchange}_{symbol}",
            "data": {
                **published_data,
                "symbol": symbol,
                "exchange": self.exchange
            }
        }
        self.publisher_thread.publish(message)
        logging.debug("%s: Enqueued order book update for symbol %s", self.__class__.__name__, symbol)

    def logging_loop(self):
        """
        Periodically log the best bid and ask for each symbol in the order book.
        """
        logging.info("%s: Starting periodic order book logging...", self.__class__.__name__)
        while self.logging_running:
            for symbol, order_book in self.order_book.items():
                # Retrieve best bid (highest) and best ask (lowest).
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


# =============================================================================
# Exchange-Specific Implementations
# =============================================================================
class CoinbaseOrderBookPublisher(OrderBookPublisher):
    """
    Order book publisher implementation for Coinbase.

    Expected incoming data structure for order book updates:
      - Keys: 'channel', 'client_id', 'timestamp', 'sequence_num', 'events'
      - Each event in 'events' contains keys: type, product_id, updates.
      - Each update in event['updates'] has: side, price_level, new_quantity.
    """

    def generate_jwt(self, message, channel):
        """
        Generate a JSON Web Token (JWT) for authentication with Coinbase.

        Args:
            message (dict): The subscription message to sign.
            channel (str): The channel being subscribed to.

        Returns:
            dict: The message updated with the JWT.
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
        message["jwt"] = token
        logging.debug("%s: JWT generated for channel %s", self.exchange, channel)
        return message

    def subscribe(self, ws):
        """
        Subscribe to the Coinbase order book channel using the "level2" channel.

        Args:
            ws (websocket.WebSocket): Active WebSocket connection.
        """
        message = {
            "type": "subscribe",
            "channel": "level2",
            "product_ids": self.symbols
        }
        signed_message = self.generate_jwt(message, "level2")
        ws.send(dumps(signed_message))
        logging.info("%s: Sent subscription message for products %s on channel %s", self.exchange, self.symbols, "level2")

    def websocket_handler(self, ws, message):
        """
        Handle incoming WebSocket messages from Coinbase.

        Args:
            ws (websocket.WebSocket): Active WebSocket connection.
            message (str): Incoming message as a string.
        """
        logging.debug("%s: WebSocket message received: %s", self.exchange, message)
        if not isinstance(message, str):
            logging.debug("%s: Received non-string message: %s", self.exchange, message)
            return

        try:
            # Record when the message was received (UTC, microsecond precision).
            timeReceived = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec='microseconds')
            data = loads(message)
            self.update_order_book(data, timeReceived)
        except Exception as e:
            logging.error("%s: Error processing WebSocket message: %s", self.exchange, e)

    def update_order_book(self, data, timeReceived):
        """
        Process Coinbase order book update messages and update the local order book.

        Args:
            data (dict): Message data from Coinbase.
            timeReceived (str): Timestamp when the message was received.
        """
        timeExchange = data.get("timestamp")  # Exchange-provided timestamp (assumed UTC)
        if "events" in data:
            for event in data["events"]:
                product_id = event.get("product_id")
                if not product_id or product_id not in self.order_book:
                    logging.debug("%s: Skipping event for product %s", self.exchange, product_id)
                    continue

                order_book_instance = self.order_book[product_id]
                event_type = event.get("type", "").lower()

                # If event is a snapshot, clear the current order book.
                if event_type == "snapshot":
                    order_book_instance.bids.clear()
                    order_book_instance.asks.clear()
                    logging.debug("%s: Cleared order book for %s due to snapshot.", self.exchange, product_id)

                updates = event.get("updates", [])
                for upd in updates:
                    side = upd.get("side", "").lower()
                    price = upd.get("price_level")
                    quantity = upd.get("new_quantity")
                    if not (side and price is not None and quantity is not None):
                        logging.debug("%s: Skipping update with insufficient data: %s", self.exchange, upd)
                        continue
                    try:
                        order_book_instance.update_order(float(price), float(quantity), side)
                    except Exception as e:
                        logging.error("%s: Error updating order at price %s for product %s: %s",
                                      self.exchange, price, product_id, e)
                # Publish the updated order book.
                timePublished = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec='microseconds')
                self.publish_order_book(product_id, timeExchange, timeReceived, timePublished)
        else:
            # Fallback for messages without an "events" key.
            symbol = data.get("product_id") or data.get("instrument_id") or data.get("symbol")
            if not symbol or symbol not in self.order_book:
                logging.debug("%s: Skipping data for symbol %s", self.exchange, symbol)
                return
            order_book_instance = self.order_book[symbol]
            if "bids" in data:
                for price, quantity in data["bids"]:
                    try:
                        order_book_instance.update_order(float(price), float(quantity), "bid")
                    except Exception as e:
                        logging.error("%s: Error updating bid at price %s for symbol %s: %s", self.exchange, price, symbol, e)
            if "asks" in data:
                for price, quantity in data["asks"]:
                    try:
                        order_book_instance.update_order(float(price), float(quantity), "ask")
                    except Exception as e:
                        logging.error("%s: Error updating ask at price %s for symbol %s: %s", self.exchange, price, symbol, e)
            timePublished = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec='microseconds')
            self.publish_order_book(symbol, timeExchange, timeReceived, timePublished)


class BinanceOrderBookPublisher(OrderBookPublisher):
    """
    Order book publisher implementation for Binance.

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
        """
        Subscribe to Binance depth updates.

        Args:
            ws (websocket.WebSocket): Active WebSocket connection.
        """
        message = {
            "method": "SUBSCRIBE",
            "params": [f"{symbol.lower()}@depth@100ms" for symbol in self.symbols],
            "id": 1
        }
        ws.send(dumps(message))
        logging.info("%s: Sent subscription message: %s", self.exchange, message)

    def websocket_handler(self, ws, message):
        """
        Handle incoming WebSocket messages from Binance.

        Args:
            ws (websocket.WebSocket): Active WebSocket connection.
            message (str): Incoming message as a string.
        """
        logging.debug("%s: WebSocket message received: %s", self.exchange, message)
        if not isinstance(message, str):
            logging.debug("%s: Received non-string message: %s", self.exchange, message)
            return
        try:
            timeReceived = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec='microseconds')
            data = loads(message)
            self.update_order_book(data, timeReceived)
        except Exception as e:
            logging.error("%s: Error processing WebSocket message: %s", self.exchange, e)

    def update_order_book(self, data, timeReceived):
        """
        Update the Binance order book based on depth update messages.

        Args:
            data (dict): Message data from Binance.
            timeReceived (str): Timestamp when the message was received.
        """
        if data.get("e") != "depthUpdate":
            logging.debug("%s: Ignoring event type: %s", self.exchange, data.get("e"))
            return
        # Convert event time (ms) to an ISO-formatted UTC timestamp.
        timeExchange = datetime.datetime.fromtimestamp(
            data.get("E") / 1000, datetime.timezone.utc
        ).isoformat(timespec='microseconds')
        symbol = data.get("s")
        if not symbol or symbol not in self.order_book:
            logging.debug("%s: Skipping data for symbol %s", self.exchange, symbol)
            return
        order_book_instance = self.order_book[symbol]
        # Process bid updates.
        if "b" in data:
            for price, quantity in data["b"]:
                try:
                    order_book_instance.update_order(float(price), float(quantity), "bid")
                except Exception as e:
                    logging.error("%s: Error updating bid at price %s for %s: %s", self.exchange, price, symbol, e)
        # Process ask updates.
        if "a" in data:
            for price, quantity in data["a"]:
                try:
                    order_book_instance.update_order(float(price), float(quantity), "ask")
                except Exception as e:
                    logging.error("%s: Error updating ask at price %s for %s: %s", self.exchange, price, symbol, e)
        timePublished = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec='microseconds')
        self.publish_order_book(symbol, timeExchange, timeReceived, timePublished)

    def publish_order_book(self, symbol, timeExchange, timeReceived, timePublished):
        """
        Publish the Binance order book update for a given symbol.
        This method overrides the base _publish_order_book to include exchange-specific topic naming.

        Args:
            symbol (str): The symbol to publish.
            timeExchange (str): Exchange timestamp.
            timeReceived (str): Received timestamp.
            timePublished (str): Published timestamp.
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
            "topic": f"ORDERBOOK_{self.exchange}_{symbol}",
            "data": {**published_data, "exchange": "BINANCE", "symbol": symbol}
        }
        self.publisher_thread.publish(message)
        logging.debug("%s: Enqueued order book update for symbol %s", self.exchange, symbol)


class OkxOrderBookPublisher(OrderBookPublisher):
    """
    Order book publisher implementation for OKX.

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
        """
        Subscribe to OKX books updates.

        Args:
            ws (websocket.WebSocket): Active WebSocket connection.
        """
        message = {
            "op": "subscribe",
            "args": [f"books5:{symbol}" for symbol in self.symbols]
        }
        ws.send(dumps(message))
        logging.info("%s: Sent subscription message: %s", self.exchange, message)

    def websocket_handler(self, ws, message):
        """
        Handle incoming WebSocket messages from OKX.

        Args:
            ws (websocket.WebSocket): Active WebSocket connection.
            message (str): Incoming message as a string.
        """
        logging.debug("%s: WebSocket message received: %s", self.exchange, message)
        if not isinstance(message, str):
            logging.debug("%s: Received non-string message: %s", self.exchange, message)
            return
        try:
            timeReceived = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec='microseconds')
            data = loads(message)
            self.update_order_book(data, timeReceived)
        except Exception as e:
            logging.error("%s: Error processing WebSocket message: %s", self.exchange, e)

    def update_order_book(self, data, timeReceived):
        """
        Update the OKX order book based on incoming data.

        Args:
            data (dict): Message data from OKX.
            timeReceived (str): Timestamp when the message was received.
        """
        if "data" not in data or not data["data"]:
            logging.debug("%s: No data in message; skipping", self.exchange)
            return
        data_item = data["data"][0]
        ts = data_item.get("ts")
        if ts:
            timeExchange = datetime.datetime.fromtimestamp(
                float(ts) / 1000, datetime.timezone.utc
            ).isoformat(timespec='microseconds')
        else:
            timeExchange = "N/A"
        symbol = data.get("arg", {}).get("instId")
        if not symbol or symbol not in self.order_book:
            logging.debug("%s: Symbol %s not in subscription list; skipping", self.exchange, symbol)
            return
        order_book_instance = self.order_book[symbol]
        # Process bid updates.
        if "bids" in data_item:
            for price, quantity in data_item["bids"]:
                try:
                    order_book_instance.update_order(float(price), float(quantity), "bid")
                except Exception as e:
                    logging.error("%s: Error updating bid at price %s for %s: %s", self.exchange, price, symbol, e)
        # Process ask updates.
        if "asks" in data_item:
            for price, quantity in data_item["asks"]:
                try:
                    order_book_instance.update_order(float(price), float(quantity), "ask")
                except Exception as e:
                    logging.error("%s: Error updating ask at price %s for %s: %s", self.exchange, price, symbol, e)
        timePublished = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec='microseconds')
        self.publish_order_book(symbol, timeExchange, timeReceived, timePublished)

    def publish_order_book(self, symbol, timeExchange, timeReceived, timePublished):
        """
        Publish the OKX order book update for a given symbol.

        Args:
            symbol (str): The symbol to publish.
            timeExchange (str): Exchange timestamp.
            timeReceived (str): Received timestamp.
            timePublished (str): Published timestamp.
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
            "topic": f"ORDERBOOK_{self.exchange}_{symbol}",
            "data": {**published_data, "exchange": self.exchange, "symbol": symbol}
        }
        self.publisher_thread.publish(message)
        logging.debug("%s: Enqueued order book update for symbol %s", self.exchange, symbol)


class BybitOrderBookPublisher(OrderBookPublisher):
    """
    Order book publisher implementation for Bybit.

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
        """
        Subscribe to Bybit order book updates.

        Args:
            ws (websocket.WebSocket): Active WebSocket connection.
        """
        # Bybit requires symbols without dashes.
        message = {
            "op": "subscribe",
            "args": [f"orderBookL2_25.{symbol.replace('-', '')}" for symbol in self.symbols]
        }
        ws.send(dumps(message))
        logging.info("%s: Sent subscription message: %s", self.exchange, message)

    def websocket_handler(self, ws, message):
        """
        Handle incoming WebSocket messages from Bybit.

        Args:
            ws (websocket.WebSocket): Active WebSocket connection.
            message (str): Incoming message as a string.
        """
        logging.debug("%s: WebSocket message received: %s", self.exchange, message)
        if not isinstance(message, str):
            logging.debug("%s: Received non-string message: %s", self.exchange, message)
            return
        try:
            timeReceived = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec='microseconds')
            data = loads(message)
            self.update_order_book(data, timeReceived)
        except Exception as e:
            logging.error("%s: Error processing WebSocket message: %s", self.exchange, e)

    def update_order_book(self, data, timeReceived):
        """
        Update the Bybit order book based on incoming data.

        Args:
            data (dict): Message data from Bybit.
            timeReceived (str): Timestamp when the message was received.
        """
        if "data" not in data or not data["data"]:
            logging.debug("%s: No data in message; skipping", self.exchange)
            return
        ts = data.get("ts")
        if ts:
            timeExchange = datetime.datetime.fromtimestamp(
                float(ts) / 1000, datetime.timezone.utc
            ).isoformat(timespec='microseconds')
        else:
            timeExchange = "N/A"
        # Extract symbol from topic (e.g., "orderBookL2_25.BTCUSD")
        topic = data.get("topic", "")
        parts = topic.split(".")
        symbol = parts[1] if len(parts) > 1 else None
        if not symbol or symbol not in self.order_book:
            logging.debug("%s: Symbol %s not in subscription list; skipping", self.exchange, symbol)
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
                logging.debug("%s: Skipping entry with insufficient data: %s", self.exchange, entry)
                continue
            try:
                order_book_instance.update_order(float(price), float(quantity), side)
            except Exception as e:
                logging.error("%s: Error updating order at price %s for symbol %s: %s", self.exchange, price, symbol, e)
        timePublished = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec='microseconds')
        self.publish_order_book(symbol, timeExchange, timeReceived, timePublished)

    def publish_order_book(self, symbol, timeExchange, timeReceived, timePublished):
        """
        Publish the Bybit order book update for a given symbol.

        Args:
            symbol (str): The symbol to publish.
            timeExchange (str): Exchange timestamp.
            timeReceived (str): Received timestamp.
            timePublished (str): Published timestamp.
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
            "topic": f"ORDERBOOK_{self.exchange}_{symbol}",
            "data": {**published_data, "exchange": self.exchange, "symbol": symbol}
        }
        self.publisher_thread.publish(message)
        logging.debug("%s: Enqueued order book update for symbol %s", self.exchange, symbol)


# =============================================================================
# Main: Select which streamer(s) to run.
# =============================================================================
if __name__ == "__main__":
    # Instantiate streamers with their respective configurations and ZeroMQ ports.
    coinbase_streamer = CoinbaseOrderBookPublisher(
        ws_url=EXCHANGE_CONFIG["coinbase"]["ws_url"],
        api_key=EXCHANGE_CONFIG["coinbase"]["api_key"],
        secret_key=EXCHANGE_CONFIG["coinbase"]["secret_key"],
        symbols=["BTC-USD", "ETH-USD"],
        exchange=EXCHANGE_CONFIG["coinbase"]["exchange"],
        zmq_port=EXCHANGE_CONFIG["coinbase"]["orderbook_port"]
    )

    binance_streamer = BinanceOrderBookPublisher(
        ws_url=EXCHANGE_CONFIG["binance"]["ws_url"],
        symbols=["BTCUSDT", "ETHUSDT"],
        api_key=EXCHANGE_CONFIG["binance"]["api_key"],
        secret_key=EXCHANGE_CONFIG["binance"]["secret_key"],
        exchange=EXCHANGE_CONFIG["binance"]["exchange"],
        zmq_port=EXCHANGE_CONFIG["binance"]["orderbook_port"]
    )

    # Start each streamer in non-blocking mode using separate threads.
    coinbase_thread = threading.Thread(target=coinbase_streamer.start, kwargs={'block': False})
    binance_thread = threading.Thread(target=binance_streamer.start, kwargs={'block': False})

    coinbase_thread.start()
    binance_thread.start()

    # Let the streamers run for a specified period (e.g., 60 seconds).
    time.sleep(120)

    # Cleanly stop both streamers.
    coinbase_streamer.end()
    binance_streamer.end()

    # Optionally join the threads to ensure a clean shutdown.
    coinbase_thread.join()
    binance_thread.join()
