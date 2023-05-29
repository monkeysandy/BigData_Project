from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark import SparkConf

import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--driver-memory 4g --packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 pyspark-shell'

spark_conf = SparkConf()
spark_conf.set("spark.executor.storageFraction", "0.8")
spark_conf.set("spark.memory.fraction", "0.8")
spark_conf.set("spark.executor.memory", "8g")
spark_conf.set("spark.eventLog.enabled", "true")
spark_conf.set("spark.eventLog.dir", "hdfs://localhost:9000/spark_log")
spark_conf.set("spark.history.fs.logDirectory", "hdfs://localhost:9000/spark_log")


KAFKA_ADDR = "159.89.47.231:9092"
KAFKA_TOPIC = "log-events"
CHECKPOINT_PATH = f"dbfs:/stream-checkpoints/kafka/{KAFKA_TOPIC}"
OUTPUT_PATH = f"dbfs:/tmp/test999"
CHECKPOINT_PATH_HADOOP = f"hdfs:/kafka/parquet/checkpoint"
HDFS_HOST = "hdfs://localhost:9000"

def main():
  df = (
    create_read_stream()
    .writeStream
    .trigger(availableNow=True)
    .option("checkpointLocation", CHECKPOINT_PATH_HADOOP)
    .foreachBatch(process_micro_Batch)
    .start()
  )

  df = df.selectExpr("CAST(value as STRING)")
  
def create_read_stream():
  return (
    spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_ADDR)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest")
    .load()
  )
  
def process_micro_batch(df, batch_id):
  print("")
  
def to_parquet(df):
  output = "/Users/sandy/Desktop/COEN242/BigData_Project/PA3"
  p_df = df.writeStream.format("parquet").outputMode("append").option("checkpointLocation", CHECKPOINT_PATH)
  
  p_df.start(output_path)
  
  
  
def with_parsed_fields(df_logs):
    host_pattern = r'(^\S+\.[\S+\.]+\S+)\s'
    ts_pattern = r'\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} \+\d{4})]'
    method_uri_protocol_pattern = r'\"(\S+)\s(\S+)\s*(\S*)\"'
    content_size_pattern = r'\s(\d+)$'
    status_pattern = r'\s(\d{3})\s'
    
    return df_logs.select(
        F.regexp_extract('value', host_pattern, 1).alias('host'),
        F.to_timestamp(
            F.regexp_extract('value', ts_pattern, 1), 
            'dd/MMM/yyyy:HH:mm:ss Z'
        ).alias('timestamp'),
        F.regexp_extract('value', method_uri_protocol_pattern, 1).alias('method'),
        F.regexp_extract('value', method_uri_protocol_pattern, 2).alias('endpoint'),
        F.regexp_extract('value', method_uri_protocol_pattern, 3).alias('protocol'),
        F.regexp_extract('value', status_pattern, 1).cast('integer').alias('status'),
        F.regexp_extract('value', content_size_pattern, 1).cast('integer').alias('content_size')
    )

if __name__ == '__main__':
#   main()
  spark = (
    SparkSession
    .builder

    .config("spark.executor.memory", "1g")
    .config("spark.driver.memory", "4g")
    .appName("KafkaConsumer")
    .getOrCreate()
  )
  spark.sparkContext.setLogLevel("WARN")
  df = create_read_stream()

#   def process_micro_batch(df, batch_id):
#     (
#       df
#       .writeStream
#       .mode("append")
#       .parquet("hdfs://localhost:9000/kafka/parquet1")
#     )
#   (
#     df
#     .writeStream
#     .foreachBatch(process_micro_batch)
#     .start()
#     # .awaitTermination()
#   )
  
  # start the stream
  (
    df
    .withColumn("value", F.col("value").cast("string"))
    # .writeStream
    # .format("console")
    # .start()
    # .awaitTermination()
  )
  
  logs_df = df.select(df['value'])
  parse_df = with_parsed_fields(logs_df)

#   process_micro_batch(parse_df, 1)

  (
    parse_df
    .writeStream
    .format("console")
    .start()
    .awaitTermination()
  )

#   read parquet files
#   parquet_df = spark.read.parquet("hdfs://localhost:9000/kafka/parquet_files/part-00000-f41a12de-4bb2-4591-afd5-e15a239f565b-c000.snappy.parquet")
#   parquet_df.show()