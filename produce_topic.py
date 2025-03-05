from confluent_kafka.admin import AdminClient, NewTopic

# Kafka Admin 설정
admin_client = AdminClient({'bootstrap.servers': 'localhost:9092'})

# 생성할 Topic 목록 정의
topic_list = [NewTopic("binance-btc", num_partitions=3, replication_factor=1)]

# Topic 생성
admin_client.create_topics(topic_list)

print("Kafka Topic 생성 완료!")
