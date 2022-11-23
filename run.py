#!/usr/bin/python3

import time
import pandas as pd
import threading
import json
import utils
from kafka_producer import TweetListener
import kafka_consumer
import spark_session_starter
import credentials
from sql_connector import Connector
from LDA import LDA

consumer_key = credentials.consumer_key
consumer_secret = credentials.consumer_secret
access_token = credentials.access_token
access_token_secret = credentials.access_token_secret
keywords = credentials.keywords
topic_name = 'twitter'
kafka_url = credentials.kafka_url


# create kafka producer
def start_producer():
    twitter_stream = TweetListener(consumer_key, consumer_secret, access_token, access_token_secret)
    twitter_stream.filter(track=keywords, languages=['en'])


if __name__ == '__main__':
    # start kafka producer in the background
    producer_thread = threading.Thread(target=start_producer)
    producer_thread.start()
    consumer = kafka_consumer.get_consumer()

    # connect spark to kafka in the background
    connector = spark_session_starter.SparkConnector()
    pipeline = connector.get_pipeline()
    spark = connector.get_spark()
    # connect to the MySQL database
    mysql = Connector()
    # drop old tables
    mysql.drop_table(credentials.tweets_tablename)
#    mysql.drop_table(credentials.history_tablename)
    # create a table for tweets
    mysql.create_tweets_table()
    # create a table for history
#    mysql.create_historic_table()
    mysql.close()
    LDA = LDA()
    ctr = 0
    tweets = []
    userids = []
    timestamps = []
    raw_tweets = []
    for line in consumer:
        # process tweets in batches of 100 to limit the calls for spark
        if ctr < 100:
            tweet = json.loads(json.dumps(line.value))
            # will use this in later versions
            raw_tweets.append(tweet['text'])
            tweets.append(utils.preprocess(tweet['text']))
            userids.append(tweet['id_str'])
            # Twitter timestamp and MySQL timestamps are different, hence the reformatting
            timestamps.append(utils.reformat_timestamp(tweet['created_at']))
            ctr += 1
        else:
            df = pd.DataFrame(tweets, columns=['tweet'])
            spark_df = spark.createDataFrame(df).toDF("text")
            # get sentiments using DistilBERT
            result = pipeline.fit(spark_df).transform(spark_df)
            sentiments = result.select("label.result").collect()
            df['sent'] = sentiments
            # change the formatting of sentiments from ['pos'] type to 'positive' type
            df['sentiment'] = df['sent'].apply(utils.reformat_sentiments)
            df['userid'] = userids
            df['timestamp'] = timestamps
            df['raw'] = raw_tweets
            df.drop('sent', axis=1, inplace=True)
            for i in range(0, 2):
                topic_number = "topic_{}".format(i)
                df[topic_number] = df['tweet'].apply(lambda x: LDA.get_topic(x, i))
                i += 1
            ctr = 0
            tweets.clear()
            userids.clear()
            timestamps.clear()
            raw_tweets.clear()
            mysql = Connector()
            mysql.write_table(df)
            mysql.clear_table()
            mysql.close()
