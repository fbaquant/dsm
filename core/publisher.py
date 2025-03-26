import abc
import json
import logging
import queue
import socket
import threading
import time
import uuid

import websocket

# Configure logging.
logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)


class Publisher(abc.ABC):
    """
    Abstract base class for a data and publisher that streams messages
    via WebSocket and broadcasts them via UDP.
    """

    def __init__(self, ws_url, api_key, secret_key, symbols, exchange, udp_port):
        self.ws_url = ws_url
        self.api_key = api_key
        self.secret_key = secret_key
        self.symbols = symbols
        self.exchange = exchange
        self.udp_port = udp_port

        # Set up a UDP socket
        self.udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 1048576)  # 1MB send buffer
        self.udp_target = ("255.255.255.255", self.udp_port)  # Broadcast on local network
        self.udp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

        logging.info("%s broadcasting on port %s", self.__class__.__name__, self.udp_port)

        # Start the publisher thread.
        self.publisher_thread = PublisherThread(self.udp_socket, self.udp_target)
        self.publisher_thread.start()

        # Placeholders for the WebSocket app and related threads.
        self.ws_app = None
        self.ws_thread = None
        self.logging_running = None
        self.logging_thread = None

    @abc.abstractmethod
    def subscribe(self, ws):
        """
        Send a subscription message over the WebSocket.
        Child classes should implement this method based on the exchange's API.

        Args:
            ws (websocket.WebSocket): The active WebSocket connection.
        """
        pass

    @abc.abstractmethod
    def websocket_handler(self, ws, message):
        """
        Process an incoming WebSocket message.
        Child classes should implement this method based on the exchange's message format.

        Args:
            ws (websocket.WebSocket): The active WebSocket connection.
            message (str): The incoming message data.
        """
        pass

    @abc.abstractmethod
    def logging_loop(self):
        """
        Run a logging loop for internal publisher status or message logging.
        Child classes should implement the specific logging logic.
        """
        pass

    def start_streaming(self):
        """
       Initialize and start the WebSocket streaming in a dedicated thread.

       Returns:
           threading.Thread: The thread running the WebSocket streaming.
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

    def start(self, block=True):
        """
        Start the publisher by initiating the WebSocket stream and logging loop.
        Optionally block the main thread to keep the process running.

        Args:
            block (bool): Whether to block the main thread. Defaults to True.
        """
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
        self.udp_socket.close()
        if self.ws_thread is not None:
            self.ws_thread.join(timeout=2)
        logging.info("%s publisher stopped.", self.__class__.__name__)

    def start_logging(self):
        """
        Start the logging thread that runs the custom logging_loop.
        """
        self.logging_running = True
        self.logging_thread = threading.Thread(target=self.logging_loop)
        self.logging_thread.daemon = True  # Ensure thread exits when main program terminates.
        self.logging_thread.start()

    def stop_logging(self):
        """
        Stop the logging thread gracefully.
        """
        self.logging_running = False
        if self.logging_thread:
            self.logging_thread.join(timeout=2)


class PublisherThread(threading.Thread):

    def __init__(self, udp_socket, udp_target):
        super().__init__()
        self.udp_socket = udp_socket
        self.udp_target = udp_target
        self.queue = queue.Queue()  # ✅ Restoring to Queue() for thread safety
        self.daemon = True
        self.running = True
        self.chunk_size = 4000  # Safe UDP chunk size (~4 KB)

    def run(self):
        while self.running:
            try:
                msg = self.queue.get(timeout=0.05)  # ✅ Restoring timeout (avoids busy loop)
                json_msg = json.dumps(msg).encode("utf-8")
                message_id = str(uuid.uuid4())

                # Split message into chunks
                chunks = [json_msg[i:i + self.chunk_size] for i in range(0, len(json_msg), self.chunk_size)]
                num_chunks = len(chunks)

                if num_chunks > 1:
                    print(f"🔹 [DEBUG] Splitting message into {num_chunks} chunks: {msg.get('topic')}")

                for i, chunk in enumerate(chunks):
                    chunk_metadata = json.dumps({
                        "id": message_id,
                        "seq": i,
                        "total": num_chunks,
                        "data": chunk.decode("utf-8")
                    }).encode("utf-8")

                    self.udp_socket.sendto(chunk_metadata, self.udp_target)

                self.queue.task_done()  # ✅ Ensures messages are fully processed

            except queue.Empty:
                continue  # ✅ Avoid excessive CPU usage while keeping low latency

    def publish(self, msg):
        """
        Add a message to the publishing queue.

        Args:
            msg (dict): The message to be published.
        """
        self.queue.put(msg)  # ✅ Blocking queue prevents data loss

    def stop(self):
        """
        Stop the publishing thread.
        """
        self.running = False
