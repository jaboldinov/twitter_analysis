from kafka import KafkaConsumer
import json
import credentials
import utils
import time

topic_name = 'twitter'
kafka_url = credentials.kafka_url

# create a connection to the kafka broker
consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=[kafka_url],
    auto_offset_reset='latest',
    enable_auto_commit=True,
    auto_commit_interval_ms=5000,
    fetch_max_bytes=128,
    max_poll_records=100,
    value_deserializer=lambda x: json.loads(x.decode('utf-8')))


def get_consumer():
    return consumer