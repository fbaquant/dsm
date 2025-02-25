import unittest
from unittest.mock import MagicMock
from kafka_producer import CryptoKafkaProducer

class TestKafkaProducer(unittest.TestCase):

    def setUp(self):
        """테스트를 위한 Kafka Producer 인스턴스 생성"""
        self.producer = CryptoKafkaProducer(bootstrap_servers='localhost:9092')
        self.producer.producer = MagicMock()  # KafkaProducer를 Mock 객체로 대체

    def test_send_data(self):
        """Kafka Producer가 올바르게 데이터를 보내는지 테스트"""
        exchange = "Binance"
        symbol = "BTC"
        data = {"price": 45000, "volume": 0.1, "timestamp": 1708765932}

        expected_topic = "binance-btc"

        # 메시지 전송 실행
        self.producer.send_data(exchange, symbol, data)

        # Mock 객체를 활용하여 send()가 올바르게 호출되었는지 검증
        self.producer.producer.send.assert_called_with(expected_topic, data)

    def tearDown(self):
        """테스트 종료 후 Producer 종료"""
        self.producer.close()

if __name__ == '__main__':
    unittest.main()
