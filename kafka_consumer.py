from confluent_kafka import Consumer

# Kafka Consumer 설정
consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'crypto-consumer-group',
    'auto.offset.reset': 'earliest'
})

# 특정 Topic 구독
consumer.subscribe(["binance-btc"])

# 메시지 소비
while True:
    msg = consumer.poll(1.0)  # 1초 대기 후 메시지 확인
    if msg is None:
        continue
    print(f"받은 메시지: {msg.value().decode('utf-8')}")
