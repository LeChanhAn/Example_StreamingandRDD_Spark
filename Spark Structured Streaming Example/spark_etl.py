from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, split
from pyspark.sql.types import StructType, StructField, StringType
import os

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 pyspark-shell'

spark = SparkSession.builder \
    .appName("KafkaToParquet") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Định nghĩa schema
schema = StructType([
    StructField("student_name", StringType()),
    StructField("graduation_year", StringType()),
    StructField("major", StringType()),
])

# Hàm xử lý
def process_data(df):
    parsed = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")
    split_col = split(parsed["student_name"], "XX")
    return parsed.withColumn("first_name", split_col.getItem(0)) \
                 .withColumn("last_name", split_col.getItem(1)) \
                 .drop("student_name")

# Đọc Kafka
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "student_topic") \
    .option("startingOffsets", "earliest") \
    .load()

# Ghi Parquet
query = process_data(raw_df).writeStream \
    .trigger(availableNow=True) \
    .format("parquet") \
    .option("checkpointLocation", "./checkpoint") \
    .option("path", "./output_data") \
    .start()

query.awaitTermination()
print(">>> XONG! Da ghi file Parquet.")