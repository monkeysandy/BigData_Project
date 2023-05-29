from pyspark import SparkContext, SparkConf, StorageLevel
from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F

import re

spark_conf = SparkConf()
spark_conf.set("spark.executor.storageFraction", "0.8")
spark_conf.set("spark.memory.fraction", "0.8")
spark_conf.set("spark.executor.memory", "8g")
spark_conf.set("spark.eventLog.enabled", "true")
spark_conf.set("spark.eventLog.dir", "hdfs://localhost:9000/spark_log")
spark_conf.set("spark.history.fs.logDirectory", "hdfs://localhost:9000/spark_log")

def main():

    spark = SparkSession.builder.appName("WordCountSpark").config(conf=spark_conf).getOrCreate()

    df_stopwords = (
        spark
        .read
        .text('stopword.txt')
        .withColumnRenamed('value', 'stopword')
    )

    df_text = spark.read.text('hdfs://localhost:9000/wordcount/input/data_2.5GB')
    df_words = (
        df_text
        .withColumn("words", F.split(F.col("value"), " "))
        .withColumn("word", F.explode(F.col("words")))
        .withColumn("word", F.lower(F.col("word")))
        .filter(F.col("word") != "")
        .drop("words", "value")
    )

    df_words = (
        df_words
        .join(df_stopwords, df_words.word == df_stopwords.stopword, how='left_anti')
    )

    # df_words.cache()
    # df_words.rdd.persist(StorageLevel.MEMORY_AND_DISK)

    df_word_counts = (
        df_words
        .groupBy("word")
        .agg(F.count("word").alias("count"))
        # .orderBy(F.desc("count"))
        # .limit(100)
    )

    df_word_counts.show(100)
    # df_words.unpersist()

if __name__ == "__main__":
    main()