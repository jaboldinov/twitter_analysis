#!/usr/bin/python3

import credentials
from pyspark.sql import SparkSession
from sparknlp.annotator import *
from pyspark.ml import Pipeline


class SparkConnector:
    def __init__(self):
        self.spark = SparkSession \
            .builder \
            .appName("Spark NLP") \
            .master("local[*]") \
            .config("spark.driver.memory", "10G") \
            .config("spark.executor.memory", "10G") \
            .config("spark.max.cores", "3") \
            .config("spark.driver.maxResultSize", "0") \
            .config("spark.kryoserializer.buffer.max", "2000M") \
            .config("spark.jars.packages", "com.johnsnowlabs.nlp:spark-nlp_2.12:4.2.3") \
            .getOrCreate()
        self.documentAssembler = DocumentAssembler() \
            .setInputCol("text") \
            .setOutputCol("document")
        self.tokenizer = Tokenizer() \
            .setInputCols(["document"]) \
            .setOutputCol("token")
        self.sequenceClassifier = DistilBertForSequenceClassification.pretrained() \
            .setInputCols(["token", "document"]) \
            .setOutputCol("label") \
            .setCaseSensitive(True)
        self.pipeline = Pipeline().setStages([
            self.documentAssembler,
            self.tokenizer,
            self.sequenceClassifier
        ])

    def get_pipeline(self):
        # returns sparkNLP pipeline
        return self.pipeline

    def get_spark(self):
        return self.spark

    def is_alive(self):
        return True
