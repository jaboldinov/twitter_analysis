import tweepy
from kafka import KafkaProducer
import credentials

kafka_url = credentials.kafka_url
producer = KafkaProducer(bootstrap_servers=kafka_url)
topic_name = 'twitter'


class TweetListener(tweepy.Stream):

    def on_data(self, data):
        producer.send(topic_name, value=data)

        return True

    def on_error(self, status_code):
        if status_code == 420:
            # returning False in on_data disconnects the stream
            return False
