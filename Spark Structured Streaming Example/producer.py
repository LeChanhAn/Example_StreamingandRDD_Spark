import time
import json
from kafka import KafkaProducer

# Kết nối Kafka
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

topic_name = "student_topic"
data_list = [
    {"student_name": "nguyenXXan", "graduation_year": "2023", "major": "math"},
    {"student_name": "tranXXbinh", "graduation_year": "2024", "major": "physics"},
    {"student_name": "leXXcuong", "graduation_year": "2025", "major": "cs"}
]

print("Dang gui du lieu (Ctrl+C de dung)...")
while True:
    for data in data_list:
        producer.send(topic_name, value=data)
        print(f"Sent: {data['student_name']}")
        time.sleep(1)