import socket
import json
import logging
import threading
import time

from config import EXCHANGE_CONFIG

# Configure logging
logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)


class Subscriber:
    def __init__(self, exchange, udp_port):
        """
        Initialize the subscriber:
          - Binds to the specified UDP port.
          - Stores received message chunks for reconstruction.
        """
        self.exchange = exchange
        self.udp_port = udp_port
        self.udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.udp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1048576)  # 1MB receive buffer
        self.udp_socket.bind(("", self.udp_port))
        self.running = False
        self.thread = None
        self.message_fragments = {}
        logging.info("Subscriber initialized and listening on UDP port %s", self.udp_port)

    def subscribe_loop(self):
        """
        Main loop for receiving and processing messages.
        Runs in a dedicated thread.
        """
        logging.info("Subscription loop started.")
        while self.running:
            try:
                data, addr = self.udp_socket.recvfrom(8192)  # Receive UDP packet
                chunk = json.loads(data.decode("utf-8"))  # Parse chunk metadata
                message_id = chunk["id"]
                seq = chunk["seq"]
                total = chunk["total"]
                chunk_data = chunk["data"]

                # Initialize storage for this message ID if not already present
                if message_id not in self.message_fragments:
                    self.message_fragments[message_id] = [None] * total

                # Store chunk in correct sequence position
                self.message_fragments[message_id][seq] = chunk_data

                # Check if all chunks are received
                if all(part is not None for part in self.message_fragments[message_id]):
                    full_message = "".join(self.message_fragments[message_id])  # Reassemble full message
                    message = json.loads(full_message)  # Parse final JSON
                    topic = message.get("topic", "Unknown Topic")
                    data = message.get("data", {})
                    logging.info("Received complete message from %s: %s", addr, topic)

                    if "bidPrices" in data and "askPrices" in data:
                        best_bid = data["bidPrices"][0] if data.get("bidPrices") else "N/A"
                        best_ask = data["askPrices"][0] if data.get("askPrices") else "N/A"
                        best_bid_size = data["bidSizes"][0] if data.get("bidSizes") else "N/A"
                        best_ask_size = data["askSizes"][0] if data.get("askSizes") else "N/A"
                        timeExchange = data.get("timeExchange", "N/A")
                        timeReceived = data.get("timeReceived", "N/A")
                        timePublished = data.get("timePublished", "N/A")
                        logging.info(
                            "Order book received: Topic: %s, Exchange: %s, Symbol: %s, Best Bid: %s (%s), Best Ask: %s (%s), timeExchange: %s, timeReceived: %s, timePublished: %s",
                            topic,
                            data.get("exchange", "Unknown"),
                            data.get("symbol", "Unknown"),
                            best_bid,
                            best_bid_size,
                            best_ask,
                            best_ask_size,
                            timeExchange,
                            timeReceived,
                            timePublished
                        )
                    del self.message_fragments[message_id]  # Cleanup after successful reconstruction
            except json.JSONDecodeError:
                logging.error("Received malformed JSON data, discarding packet.")
            except Exception as e:
                logging.error("Error processing message: %s", e)
                time.sleep(0.5)
        logging.info("Subscription loop exiting.")

    def start(self):
        """
        Start the subscription in a separate thread and log the start.
        """
        self.running = True
        self.thread = threading.Thread(target=self.subscribe_loop)
        self.thread.daemon = True
        self.thread.start()
        logging.info("Subscription started.")

    def end(self):
        """
        Stop the subscription:
          - Set the running flag to False.
          - Join the thread.
          - Close the socket.
          - Log that the subscription has ended.
        """
        logging.info("Stopping subscription...")
        self.running = False
        if self.thread is not None:
            self.thread.join(timeout=2)
        self.udp_socket.close()
        logging.info("Subscription ended.")


if __name__ == "__main__":
    exchange = "bybit"
    udp_port = EXCHANGE_CONFIG[exchange]["orderbook_port"]  # Replace with the actual UDP port
    subscriber = Subscriber(exchange, udp_port)

    subscriber.start()
    time.sleep(5)  # Run the subscriber for a set duration
    subscriber.end()
