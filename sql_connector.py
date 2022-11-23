#!/usr/bin/python3.8

import mysql.connector
import pandas
from sqlalchemy import create_engine
import credentials


class Connector:

    def __init__(self):
        # database connection credentials
        # TODO rewrite to use env variables or file
        self.db_host = credentials.db_host
        self.dbname = credentials.dbname
        self.uname = credentials.dbuser
        self.pwd = credentials.dbpass
        self.tweets_table = credentials.tweets_tablename
        self.history_table = credentials.history_tablename
        self.engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}"
                                    .format(host=self.db_host,
                                            db=self.dbname,
                                            user=self.uname,
                                            pw=self.pwd))

        self.mydb = mysql.connector.connect(user=self.uname,
                                            password=self.pwd,
                                            host=self.db_host,
                                            database=self.dbname,
                                            auth_plugin='mysql_native_password')

    def get_engine(self):
        # returns sqlalchemy engine
        return self.engine

    def get_cursor(self):
        # connect to mysql db and get a cursor obj
        return self.mydb.cursor()

    def create_tweets_table(self):
        # create a table for tweets if it doesn't exist
        try:
            cursor = self.get_cursor()
            cursor.execute("CREATE TABLE {} (\
            id INT AUTO_INCREMENT PRIMARY KEY,\
            userid VARCHAR(280), \
            raw VARCHAR(2000), \
            tweet VARCHAR(280), \
            sentiment VARCHAR(12), \
            topic_0 FLOAT, \
            topic_1 FLOAT, \
            topic_2 FLOAT, \
            topic_3 FLOAT, \
            topic_4 FLOAT, \
            timestamp DATETIME)".format(self.tweets_table))
            cursor.execute("commit")
            cursor.close()
            return True

        except Exception as e:
            print("FAILED AT TABLE CREATION {}".format(e))


    def create_historic_table(self):
        # create a table with daily tweet counts
        try:
            cursor = self.get_cursor()
            cursor.execute("CREATE TABLE {} (\
            id INT AUTO_INCREMENT PRIMARY KEY,\
            day DATE, \
            numtweets INT)".format(self.history_table))
            cursor.execute("commit")
            cursor.close()
            return True

        except Exception as e:
            print("FAILED AT TABLE CREATION {}".format(e))



    def write_table(self, tweets=pandas.DataFrame):
        # write Pandas dataframe to sql table
        tweets.to_sql(self.tweets_table, self.engine, index=False, if_exists='append')

        return True

    def clear_table(self):
        # delete rows that are older than 1 day
        try:
            cursor = self.get_cursor()
            cursor.execute("DELETE FROM {} WHERE timestamp < (NOW() - INTERVAL 1 DAY)".format(self.tweets_table))
            cursor.execute("commit")
            cursor.close()
            return True

        except Exception as e:
            print("FAILED CLEARING THE TABLE".format(e))

    def drop_table(self, tname):
        # delete the entire table
        try:
            cursor = self.get_cursor()
            cursor.execute("drop table {}".format(tname))
            cursor.execute("commit")
            cursor.close()
            return True

        except Exception as e:
            print("FAILED DROPPING THE TABLE {}".format(e))

    def close(self):
        self.mydb.close()
        return True

