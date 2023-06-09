# The objective is to process and analyze log files 
# by establishing a flow that begins with a Kafka producer, 
# followed by a Kafka consumer that performs transformations using Spark. 
# The transformed data will then be converted into Parquet file format 
# and stored in the HDFS


# Log File 
# -> Kafka(Producer) 
# -> Kafka(Consumer which performs transformation using Spark) 
# -> Create Parquet files 
# ->Store the Parquet file in HDFS

from kafka import KafkaProducer
from kafka.errors import KafkaError
import logging as log

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Create a Kafka Producer
# set request.required.acks=1 for Kafka Producer
# set compression.type=gzip for Kafka Producer

producer = KafkaProducer(bootstrap_servers='159.89.47.231:9092', acks=1)
file_location = "/Users/sandy/Desktop/COEN242/BigData_Project/PA3/PartB/17GBBigServerLog.log"
# file_location = "/Users/sandy/Desktop/COEN242/BigData_Project/PA3/PartB/42MBSmallServerLog.log"
# file_location = "/Users/sandy/Desktop/COEN242/BigData_Project/PA3/PartB/test.log"
# file_location = 'hdfs://localhost:9000/log-file/17gb'

# set request.required.acks=1 for Kafka Producer


# Read the log file line by line
with open(file_location) as f:
    for line in f:
        length = len(line)
        producer.send('17gb', line.encode('utf-8'))

        print(f"Send {length} chararacters to Kafka")

# spark = (
#     SparkSession
#     .builder
#     .config("spark.executor.memory", "1g")
#     .config("spark.driver.memory", "4g")
#     .config("spark.driver.maxResultSize", "4g")
#     .appName("LogReader")
#     .getOrCreate()
# )

# file = spark.read.text(file_location)
# for line in file.collect():
#     length = len(line)
#     producer.send('17gb', line.value.encode('utf-8'))
#     print(f"Send {length} chararacters to Kafka")




