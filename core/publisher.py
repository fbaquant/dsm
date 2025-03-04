import abc
import logging
import queue
import threading
import time

import websocket
import zmq

# Configure logging.
logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)


class Publisher(abc.ABC):
    """
    Abstract base class for a data publisher that streams WebSocket data and publishes it over ZeroMQ.

    Attributes:
        ws_url (str): The WebSocket URL endpoint.
        api_key (str): API key for authentication.
        secret_key (str): Secret key for authentication.
        symbols (list): List of symbols to subscribe to.
        exchange (str): Prefix for message topics.
        zmq_port (int): The port number for ZeroMQ publisher binding.
    """

    def __init__(self, ws_url, api_key, secret_key, symbols, exchange, zmq_port):
        """
        Initialize the Publisher with WebSocket and ZeroMQ configurations.

        Args:
            ws_url (str): WebSocket endpoint URL.
            api_key (str): API key for authentication.
            secret_key (str): Secret key for authentication.
            symbols (list): List of symbols to subscribe to.
            exchange (str): exchange for topic names.
            zmq_port (int): Port number for the ZeroMQ publisher.
        """
        self.ws_url = ws_url
        self.api_key = api_key
        self.secret_key = secret_key
        self.symbols = symbols
        self.exchange = exchange
        self.zmq_port = zmq_port

        # Set up ZeroMQ publisher.
        self.context = zmq.Context()
        self.publisher = self.context.socket(zmq.PUB)
        self.publisher.bind(f"tcp://*:{self.zmq_port}")
        logging.info("%s: ZeroMQ publisher bound on port %s", self.__class__.__name__, self.zmq_port)

        # Start the ZeroMQ publisher thread.
        self.publisher_thread = PublisherThread(self.publisher)
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
        """
        Cleanly stop the publisher, including the WebSocket, logging loop, and ZeroMQ publisher.
        """
        logging.info("Stopping %s publisher...", self.__class__.__name__)
        self.stop_logging()
        if self.ws_app is not None:
            self.ws_app.keep_running = False
            self.ws_app.close()
        self.publisher_thread.stop()
        self.publisher.close()
        self.context.term()
        if self.ws_thread is not None:
            self.ws_thread.join(timeout=2)
        logging.info("%s publisher stopped.", self.__class__.__name__)

    def start_logging(self):
        """
        Start the logging thread that runs the custom _logging_loop.
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
    """
    Thread class for handling message publishing via ZeroMQ in the background.

    Attributes:
        publisher (zmq.Socket): The ZeroMQ publisher socket.
        queue (queue.Queue): Queue to hold messages to be published.
        running (bool): Flag to control the thread's running state.
    """

    def __init__(self, publisher):
        """
        Initialize the PublisherThread with the ZeroMQ publisher socket.

        Args:
            publisher (zmq.Socket): The ZeroMQ publisher socket.
        """
        super().__init__()
        self.publisher = publisher
        self.queue = queue.Queue()
        self.daemon = True  # Ensure thread terminates when main program exits.
        self.running = True

    def run(self):
        """
        Main loop for the thread that continuously checks the queue for new messages
        and publishes them over ZeroMQ.
        """
        while self.running:
            try:
                msg = self.queue.get(timeout=0.05)
                self.publisher.send_json(msg)
                self.queue.task_done()
                logging.debug("Published message: %s", msg.get("topic"))
            except queue.Empty:
                continue

    def publish(self, msg):
        """
        Add a message to the publishing queue.

        Args:
            msg (dict): The message to be published. Expected to be JSON-serializable.
        """
        self.queue.put(msg)

    def stop(self):
        """
        Stop the publishing thread.
        """
        self.running = False
