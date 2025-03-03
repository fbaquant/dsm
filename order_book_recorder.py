#!/usr/bin/env python3
"""
order_book_recorder.py

Subscribes to order book updates published via ZeroMQ and records the full order book into a SQLite database.
We simulate an InfluxDB3-like client API so that later you can switch to InfluxDB3 with minimal changes.

Configuration values (such as ZMQ port and topic prefix) are expected to be provided in config.py.
"""

import datetime
import json
import logging
import sqlite3
import threading
import time
import zmq

try:
    import orjson as json_parser
    def dumps(obj):
        return json_parser.dumps(obj).decode("utf-8")
    loads = json_parser.loads
except ImportError:
    import json as json_parser
    dumps = json_parser.dumps
    loads = json_parser.loads

from config import EXCHANGE, DATABASE

# For this example, we assume we’re recording Coinbase order book updates.
EXCHANGE_CONFIG = EXCHANGE["coinbase"]
# Use the InfluxDB section of config.py even though we are using SQLite.
INFLUX_CONFIG = DATABASE

# ZeroMQ settings from config.
ZMQ_PORT = EXCHANGE_CONFIG["zmq_port"]         # e.g. 5559
TOPIC_PREFIX = EXCHANGE_CONFIG["topic_prefix"] # e.g. "ORDERBOOK_COINBASE_"

# Configure logging.
logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.DEBUG)

# =============================================================================
# SQLiteDBClient: A minimal replacement for InfluxDBClient.
# =============================================================================

class SQLiteDBClient:
    def __init__(self, db_file):
        self.db_file = db_file
        self.conn = sqlite3.connect(
            self.db_file,
            check_same_thread=False,
            detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES
        )
        self.create_table()

    def create_table(self):
        # Create an order_book table if it doesn't exist.
        c = self.conn.cursor()
        c.execute('''
        CREATE TABLE IF NOT EXISTS order_book (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            exchange TEXT,
            symbol TEXT,
            bids TEXT,
            asks TEXT,
            timeExchange TEXT,
            timeReceived TEXT,
            timePublished TEXT,
            raw TEXT
        )
        ''')
        self.conn.commit()

    def write(self, bucket, org, record):
        # For simulation, we ignore bucket and org.
        rec = record.to_dict()
        timestamp = rec.get("time")
        fields = rec.get("fields", {})
        tags = rec.get("tags", {})
        exchange = tags.get("exchange", "Unknown")
        symbol = tags.get("symbol", "Unknown")
        bids = fields.get("bids", "")
        asks = fields.get("asks", "")
        timeExchange = fields.get("timeExchange", "")
        timeReceived = fields.get("timeReceived", "")
        timePublished = fields.get("timePublished", "")
        raw = fields.get("raw", "")
        c = self.conn.cursor()
        c.execute('''
        INSERT INTO order_book (timestamp, exchange, symbol, bids, asks, timeExchange, timeReceived, timePublished, raw)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (timestamp, exchange, symbol, bids, asks, timeExchange, timeReceived, timePublished, raw))
        self.conn.commit()

    def close(self):
        self.conn.close()


# =============================================================================
# Minimal Point class to simulate InfluxDB's API.
# =============================================================================

class Point:
    def __init__(self, measurement):
        self.measurement = measurement
        self._tags = {}
        self._fields = {}
        self._time = None

    def tag(self, key, value):
        self._tags[key] = value
        return self

    def field(self, key, value):
        self._fields[key] = value
        return self

    def time(self, t, precision):
        # For our simulation, simply store the ISO 8601 string.
        self._time = t.isoformat()
        return self

    def to_dict(self):
        return {
            "measurement": self.measurement,
            "tags": self._tags,
            "fields": self._fields,
            "time": self._time
        }

class WritePrecision:
    MS = "ms"

# =============================================================================
# OrderBookRecorder Class
# =============================================================================

class OrderBookRecorder:
    """
    Subscribes to order book updates via ZeroMQ and records the full order book
    into SQLite using a simulated InfluxDB client API.
    """
    def __init__(self, zmq_port, topic_prefix, influx_config, db_file="order_book.db"):
        self.zmq_port = zmq_port
        self.topic_prefix = topic_prefix

        # Set up ZeroMQ subscriber.
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.SUB)
        self.socket.connect(f"tcp://localhost:{self.zmq_port}")
        self.socket.setsockopt_string(zmq.SUBSCRIBE, "")
        self.socket.setsockopt(zmq.RCVTIMEO, 1000)
        logging.info("OrderBookRecorder: Connected to ZeroMQ on port %s", self.zmq_port)

        # Set up SQLiteDBClient (our temporary storage simulating InfluxDB).
        self.influx_client = SQLiteDBClient(db_file)
        self.bucket = influx_config["bucket"]
        self.org = influx_config["org"]

        self.running = False

    def record(self):
        """
        Main loop: receive order book updates from ZeroMQ, and write the full order book
        along with three timestamps into SQLite.
        """
        logging.info("OrderBookRecorder: Starting recording loop...")
        while self.running:
            try:
                message = self.socket.recv_json()
                logging.debug("OrderBookRecorder: Raw message received: %s", message)
                topic = message.get("topic", "")
                if not topic.startswith(self.topic_prefix):
                    logging.debug("OrderBookRecorder: Ignoring message with topic: %s", topic)
                    continue

                data = message.get("data", {})
                exchange = data.get("exchange", "Unknown")
                symbol = data.get("symbol", "Unknown")
                # Record full order book as JSON strings.
                bids = data.get("bids", [])
                asks = data.get("asks", [])
                bids_json = json.dumps(bids)
                asks_json = json.dumps(asks)
                # Also record the entire raw update.
                raw = json.dumps(data)

                # Extract timestamps.
                timeExchange = data.get("timeExchange", "N/A")
                timeReceived = data.get("timeReceived", "N/A")
                timePublished = data.get("timePublished", "N/A")

                # Convert timePublished to a datetime object.
                if timePublished and timePublished != "N/A":
                    try:
                        dt_timePublished = datetime.datetime.fromisoformat(timePublished.replace("Z", "+00:00"))
                    except Exception:
                        dt_timePublished = datetime.datetime.now(datetime.timezone.utc)
                else:
                    dt_timePublished = datetime.datetime.now(datetime.timezone.utc)

                # Build a Point object (simulate InfluxDB point).
                point = Point("order_book") \
                    .tag("exchange", exchange) \
                    .tag("symbol", symbol) \
                    .field("bids", bids_json) \
                    .field("asks", asks_json) \
                    .field("raw", raw) \
                    .field("timeExchange", timeExchange) \
                    .field("timeReceived", timeReceived) \
                    .field("timePublished", timePublished) \
                    .time(dt_timePublished, WritePrecision.MS)
                self.influx_client.write(bucket=self.bucket, org=self.org, record=point)
                logging.info("OrderBookRecorder: Recorded full order book for %s", symbol)
            except zmq.error.Again:
                continue
            except Exception as e:
                logging.error("OrderBookRecorder: Error recording message: %s", e)
                time.sleep(1)
        logging.info("OrderBookRecorder: Recording loop exiting.")

    def start(self):
        """
        Start the recording process in a separate thread.
        """
        self.running = True
        self.recording_thread = threading.Thread(target=self.record)
        self.recording_thread.daemon = True
        self.recording_thread.start()
        logging.info("OrderBookRecorder: Recording thread started.")

    def stop(self):
        """
        Stop the recording process and clean up resources.
        """
        logging.info("OrderBookRecorder: Stopping recorder...")
        self.running = False
        self.recording_thread.join(timeout=2)
        self.socket.close()
        self.context.term()
        self.influx_client.close()
        logging.info("OrderBookRecorder: Recorder stopped.")


if __name__ == "__main__":
    # Use the exchange configuration from config.py.
    # For example, for Coinbase:


    recorder = OrderBookRecorder(zmq_port=EXCHANGE["coinbase"]["zmq_port"],
                                 topic_prefix=EXCHANGE["coinbase"]["topic_prefix"],
                                 influx_config=INFLUX_CONFIG)
    recorder.start()
    # Let it run for 60 seconds.
    time.sleep(60)
    recorder.stop()
