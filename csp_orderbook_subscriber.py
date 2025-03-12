import json
import socket
import threading
from datetime import datetime, timedelta

import csp
from csp import ts
from csp.impl.adaptermanager import AdapterManagerImpl
from csp.impl.pushadapter import PushInputAdapter
from csp.impl.wiring import py_push_adapter_def


class MyData(csp.Struct):
    symbol: str
    bids: list
    asks: list
    time_exchange: str
    time_received: str
    time_published: str


class MyAdapterManager:
    def __init__(self, interval: timedelta, udp_port: int):
        """
        Normally one would pass properties of the manager here, ie filename,
        message bus, etc.
        """
        print("MyAdapterManager::__init__")
        self._interval = interval
        self._udp_port = udp_port

    def subscribe(self, symbol, push_mode=csp.PushMode.NON_COLLAPSING):
        """User facing API to subscribe to a timeseries stream from this adapter manager."""
        return MyPushAdapter(self, symbol, push_mode=push_mode)

    def _create(self, engine, memo):
        """This method will get called at engine build time, at which point the graph-time manager representation
        will create the actual implementation that will be used at runtime.
        """
        print("MyAdapterManager::_create")
        return MyAdapterManagerImpl(engine, self._interval, self._udp_port)


class MyAdapterManagerImpl(AdapterManagerImpl):
    def __init__(self, engine, interval, udp_port):
        print("MyAdapterManagerImpl::__init__")
        super().__init__(engine)

        self._interval = interval
        self._inputs = {}

        self._udp_port = udp_port
        self._udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._udp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._udp_socket.bind(("", self._udp_port))

        self._running = False
        self._thread = None

    def start(self, starttime, endtime):
        """Start the data processing thread that listens for UDP messages."""
        print("MyAdapterManagerImpl::start")
        self._running = True
        self._thread = threading.Thread(target=self._run)
        self._thread.start()

    def stop(self):
        """Stop the UDP listener and clean up resources."""
        print("MyAdapterManagerImpl::stop")
        if self._running:
            self._running = False
            self._thread.join()
            self._udp_socket.close()

    def register_input_adapter(self, symbol, adapter):
        """Registers input adapters when they are created as part of the engine."""
        if symbol not in self._inputs:
            self._inputs[symbol] = []
        self._inputs[symbol].append(adapter)

    def process_next_sim_timeslice(self, now):
        """For realtime adapters, we return None since there is no simulation timeslice."""
        return None

    def _run(self):
        """Thread function: Listens for UDP messages and processes them."""
        print(f"Listening for UDP data on port {self._udp_port}...")
        while self._running:
            try:
                message, _ = self._udp_socket.recvfrom(8192)
                decoded_message = message.decode("utf-8")

                # Debugging: Print received raw message
                print(f"Received raw message: {decoded_message}")

                # Attempt to parse JSON
                try:
                    json_msg = json.loads(decoded_message)
                except json.JSONDecodeError:
                    print(f"Received malformed JSON message: {decoded_message}")
                    continue  # Skip processing if message is not valid JSON

                # Debugging: Print parsed message
                print(f"Parsed JSON message: {json_msg}")

                # Ensure the parsed message is actually a dictionary
                if isinstance(json_msg, dict):
                    self.process_message(json_msg)
                else:
                    print(f"Unexpected data format (expected dict, got {type(json_msg)}): {json_msg}")

            except Exception as e:
                print(f"Error receiving UDP message: {e}")

    def process_message(self, msg):
        """Process received UDP message and push data to adapters."""
        if not isinstance(msg, dict):
            print(f"Skipping non-dictionary message: {msg}")
            return

        # Extract and parse the inner 'data' field which is still a JSON string
        try:
            if isinstance(msg.get("data"), str):  # Check if 'data' is a string
                parsed_data = json.loads(msg["data"])  # Deserialize again
            else:
                parsed_data = msg.get("data", {})
        except json.JSONDecodeError:
            print(f"Error decoding nested JSON from 'data': {msg.get('data')}")
            return

        # Now extract the actual order book data inside "parsed_data"
        if not isinstance(parsed_data, dict) or "data" not in parsed_data:
            print(f"Invalid 'parsed_data' format: {parsed_data}")
            return

        order_book = parsed_data["data"]  # The actual order book JSON
        if not isinstance(order_book, dict):
            print(f"Invalid order book format: {order_book}")
            return

        # Extract necessary fields
        symbol = order_book.get("symbol")
        bids = order_book.get("bids", [])
        asks = order_book.get("asks", [])
        time_exchange = order_book.get("timeExchange", "N/A")
        time_received = order_book.get("timeReceived", "N/A")
        time_published = order_book.get("timePublished", "N/A")

        print(
            f"Processed Order Book: Symbol={symbol}, Best Bid={bids[:2]}, Best Ask={asks[:2]}, TimeExchange={time_exchange}")

        if symbol in self._inputs:
            my_data = MyData(
                symbol=symbol,
                bids=bids,
                asks=asks,
                time_exchange=time_exchange,
                time_received=time_received,
                time_published=time_published
            )
            for adapter in self._inputs[symbol]:
                adapter.push_tick(my_data)


class MyPushAdapterImpl(PushInputAdapter):
    def __init__(self, manager_impl, symbol):
        print(f"MyPushAdapterImpl::__init__ {symbol}")
        manager_impl.register_input_adapter(symbol, self)
        super().__init__()


MyPushAdapter = py_push_adapter_def("MyPushAdapter", MyPushAdapterImpl, ts[MyData], MyAdapterManager, symbol=str)


@csp.graph
def my_graph():
    print("Start of graph building")

    adapter_manager = MyAdapterManager(timedelta(seconds=0.75), udp_port=5566)
    symbols = ["BTCUSDT", "ETHUSDT"]

    for symbol in symbols:
        data = adapter_manager.subscribe(symbol, csp.PushMode.LAST_VALUE)
        csp.print(symbol + " last_value", data)

        data = adapter_manager.subscribe(symbol, csp.PushMode.BURST)
        csp.print(symbol + " burst", data)

        data = adapter_manager.subscribe(symbol, csp.PushMode.NON_COLLAPSING)
        csp.print(symbol + " non_collapsing", data)

    print("End of graph building")


def main():
    csp.run(my_graph, starttime=datetime.utcnow(), endtime=timedelta(seconds=5), realtime=True)


if __name__ == "__main__":
    main()
