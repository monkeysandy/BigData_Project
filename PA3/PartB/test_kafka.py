from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("Read Parquet Files").getOrCreate()

# Read the first parquet file
df1 = (
    spark.read.parquet("hdfs://localhost:9000/kafka/parquet/endpoint")
    .groupBy("Day in a week", "endpoint")
    .agg(F.sum("count").alias("Count"))
    .sort(F.desc("Count"))
)

df1.show(10, truncate=False)

spark.stop()
