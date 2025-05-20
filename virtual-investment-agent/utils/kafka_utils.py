from kafka import KafkaProducer
import json

def create_kafka_producer(bootstrap_servers):
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

def send_to_kafka(producer, topic, message):
    producer.send(topic, value=message)
