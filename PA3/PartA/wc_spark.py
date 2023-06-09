from pyspark import SparkContext, SparkConf, StorageLevel
from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F

import re

# set up spark configuration
spark_conf = SparkConf()
spark_conf.set("spark.executor.storageFraction", "0.8")
spark_conf.set("spark.memory.fraction", "0.8")
spark_conf.set("spark.executor.memory", "8g")
spark_conf.set("spark.eventLog.enabled", "true")
spark_conf.set("spark.eventLog.dir", "hdfs://localhost:9000/spark_log")
spark_conf.set("spark.history.fs.logDirectory", "hdfs://localhost:9000/spark_log")

def main():
    # create spark session
    spark = SparkSession.builder.appName("WordCountSpark").config(conf=spark_conf).getOrCreate()

    # read the stopword file and put it into a dataframe
    df_stopwords = (
        spark
        .read
        .text('stopword.txt')
        .withColumnRenamed('value', 'stopword')
    )

    # read the log file from hdfs
    df_text = spark.read.text('hdfs://localhost:9000/wordcount/input/data_2.5GB')

    # split the text into words
    df_words = (
        df_text
        .withColumn("words", F.split(F.col("value"), " "))
        .withColumn("word", F.explode(F.col("words")))
        .withColumn("word", F.lower(F.col("word")))
        .filter(F.col("word") != "")
        .drop("words", "value")
    )

    # remove stopwords
    df_words = (
        df_words
        .join(df_stopwords, df_words.word == df_stopwords.stopword, how='left_anti')
    )

    # find top 100 words
    df_word_counts = (
        df_words
        .groupBy("word")
        .agg(F.count("word").alias("count"))
    )

    # find top 100 words > 6 characters
    df_word_counts_6 = (
        df_words
        .filter(F.length(F.col("word")) > 6)
        .groupBy("word")
        .agg(F.count("word").alias("count"))
    )

    df_word_counts.show(100)

if __name__ == "__main__":
    main()