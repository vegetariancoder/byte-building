from kafka import KafkaProducer
from faker import Faker
import json
import random
import time

# Create faker object
fake = Faker()

# Create Kafka producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092', 'localhost:9093'],
    value_serializer=lambda m: json.dumps(m).encode('utf-8')  # Encoding happens here
)

# Event types
event_type = ['click', 'search', 'view', 'add_to_cart', 'purchase']

# Event generator
def event_generator():
    return {
        "user_id": fake.uuid4(),
        "user_name": fake.name(),
        "event_type": random.choice(event_type),
        "page_id": fake.uri_path(),
        "timestamp": fake.iso8601(),
        "device_info": fake.user_agent()
    }

# Kafka topic
KAFKA_TOPIC = 'my-first-topic'

# Continuously produce events
while True:
    event = event_generator()
    producer.send(KAFKA_TOPIC, event)  # send dict directly, serializer will handle it
    producer.flush()
    print(f"Sent event: {event}")
    time.sleep(5)
