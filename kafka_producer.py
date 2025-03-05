from kafka import KafkaProducer
import json

class CryptoKafkaProducer:
    def __init__(self, bootstrap_servers='localhost:9092'):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

    def send_data(self, exchange, symbol, data):
        topic = f"{exchange.lower()}-{symbol.lower()}"
        self.producer.send(topic, data)
        self.producer.flush()  # 즉시 전송

    def close(self):
        self.producer.close()

# 예시 사용법
if __name__ == '__main__':
    producer = CryptoKafkaProducer()

    # 웹소켓에서 받은 데이터 예시
    websocket_data = {
        'price': 45000,
        'volume': 0.1,
        'timestamp': 1708765932
    }

    exchange = 'Binance'
    symbol = 'BTC'

    producer.send_data(exchange, symbol, websocket_data)
    producer.close()
