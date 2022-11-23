#!/usr/bin/python

import re
import datetime
from datetime import datetime
import json
import credentials
from bs4 import BeautifulSoup
from nltk.tokenize import WordPunctTokenizer

tok = WordPunctTokenizer()


# reformats twitter timestamps into SQL format
def reformat_timestamp(twitter_timestamp=str):
    date_obj = datetime.strptime(twitter_timestamp, "%a %b %d %H:%M:%S %z %Y")

    return "{} {}".format(date_obj.date(),date_obj.time())


# preprocesses text - lowercase, remove URLs, tickers, punctuation, single chars
def preprocess(message=str):
    rx1 = r'@[A-Za-z0-9_]+'
    rx2 = r'https?://[^ ]+'
    http = r'|'.join((rx1, rx2))
    www = r'www.[^ ]+'

    # preprocess text, clean punctuation, URLs etc.
    soup = BeautifulSoup(message, 'html.parser')
    text = soup.get_text()
    try:
        text = text.decode("utf-8-sig").replace(u"\ufffd", "?")
    except:
        pass
    text = re.sub(http, '', text)
    text = re.sub(www, '', text)
    text = re.compile('RT @').sub('', text)
    text = text.lower()
    text = re.sub("[^a-zA-Z]", " ", text)
    words = [x for x in tok.tokenize(text) if len(x) > 1]
    return (" ".join(words)).strip()


# queries the database and returns a latest timestamp
def get_db_timestamp():
    from sql_connector import Connector
    connector = Connector()
    try:
        latest = ""
        cursor = connector.get_cursor()
        cursor.execute("select MAX(timestamp) from tweets")
        for date in cursor:
            latest = date[0]
        connector.close()

        return latest

    except Exception as e:
        print(e)


# separates the latest tweets from the dataframe
def separate_latest(tweets):
    latest_timestamp = get_db_timestamp()
    if latest_timestamp is not None :
        return tweets.loc[tweets['timestamp'] > str(latest_timestamp)]
    else:
        raise Exception("empty DB")


def reformat_sentiments(text: list):
    if "pos" in text[0]:
        return "positive"
    elif "neg" in text[0]:
        return "negative"
    else:
        return "neutral"