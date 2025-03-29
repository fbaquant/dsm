import abc
import json
import logging
import queue
import socket
import threading
import time
import uuid
import asyncio
from nats.aio.client import Client as NATS

import websocket

# Configure logging.
logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)


import abc
import asyncio
import json
import logging
import threading
import time
from nats.aio.client import Client as NATS

logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)


class Publisher(abc.ABC):
    def __init__(self, ws_url, api_key, secret_key, symbols, exchange, udp_port=None):
        self.ws_url = ws_url
        self.api_key = api_key
        self.secret_key = secret_key
        self.symbols = symbols
        self.exchange = exchange

        # NATS event loop setup in background thread
        self.nats_client = NATS()
        self.loop = asyncio.new_event_loop()
        self.loop_thread = threading.Thread(target=self._run_loop, daemon=True)
        self.loop_thread.start()
        future = asyncio.run_coroutine_threadsafe(
            self.nats_client.connect(servers=["nats://localhost:4222"]), self.loop
        )
        future.result()  # Wait for connection

        logging.info("%s connected to NATS for exchange: %s", self.__class__.__name__, self.exchange)

        # Start publisher thread
        self.publisher_thread = PublisherThread(self.nats_client, self.exchange, self.loop)
        self.publisher_thread.start()

        # Placeholders
        self.ws_app = None
        self.ws_thread = None
        self.logging_running = False
        self.logging_thread = None

    def _run_loop(self):
        asyncio.set_event_loop(self.loop)
        self.loop.run_forever()

    @abc.abstractmethod
    def subscribe(self, ws):
        pass

    @abc.abstractmethod
    def websocket_handler(self, ws, message):
        pass

    @abc.abstractmethod
    def logging_loop(self):
        pass

    def start_streaming(self):
        import websocket
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

    def start(self, block=True):
        self.start_streaming()
        self.start_logging()
        logging.info("%s publisher is running.", self.__class__.__name__)
        if block:
            try:
                while True:
                    time.sleep(1)
            except KeyboardInterrupt:
                self.end()

    def end(self):
        logging.info("Stopping %s publisher...", self.__class__.__name__)
        self.stop_logging()
        if self.ws_app is not None:
            self.ws_app.keep_running = False
            self.ws_app.close()
        self.publisher_thread.stop()
        if self.ws_thread is not None:
            self.ws_thread.join(timeout=2)

        # Stop NATS loop
        self.loop.call_soon_threadsafe(self.loop.stop)
        self.loop_thread.join(timeout=2)
        logging.info("%s publisher stopped.", self.__class__.__name__)

    def start_logging(self):
        self.logging_running = True
        self.logging_thread = threading.Thread(target=self.logging_loop)
        self.logging_thread.daemon = True
        self.logging_thread.start()

    def stop_logging(self):
        self.logging_running = False
        if self.logging_thread:
            self.logging_thread.join(timeout=2)



import queue

class PublisherThread(threading.Thread):
    def __init__(self, nats_client, exchange, loop):
        super().__init__()
        self.nats_client = nats_client
        self.exchange = exchange
        self.loop = loop
        self.queue = queue.Queue()
        self.running = True
        self.daemon = True

    def run(self):
        while self.running:
            try:
                msg = self.queue.get(timeout=0.05)
                symbol = msg["data"].get("symbol")
                if not symbol:
                    continue
                subject = f"{self.exchange}.{symbol}.orderbook"
                payload = json.dumps(msg["data"]).encode("utf-8")
                asyncio.run_coroutine_threadsafe(
                    self.nats_client.publish(subject, payload),
                    self.loop
                )
                self.queue.task_done()
            except queue.Empty:
                continue

    def publish(self, msg):
        self.queue.put(msg)

    def stop(self):
        self.running = False

