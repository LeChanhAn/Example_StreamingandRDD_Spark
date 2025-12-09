from pyspark.sql import SparkSession
import time

master_ip = "192.168.196.131"

spark = SparkSession.builder \
    .appName("WordCount_Tren_Cluster_File_Text") \
    .master(f"spark://{master_ip}:7077") \
    .config("spark.driver.host", master_ip) \
    .config("spark.driver.bindAddress", "0.0.0.0") \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("WARN")

print(">>> DA KET NOI TOI CLUSTER! Dang doc file some_words.txt...")

with open("some_words.txt", "r") as f:
    file_content = f.read().splitlines()

rdd = sc.parallelize(file_content, numSlices=4)

counts = (
    rdd.flatMap(lambda line: line.split(" ")) \
    .map(lambda word: (word, 1)) \
    .reduceByKey(lambda a, b: a + b)
)

output = counts.collect()

print("---------------------------------")
print("KET QUA WORD COUNT TU FILE TEXT:")
for word, count in output:
    print(f"('{word}', {count})")
print("---------------------------------")

print(">>> Code dang tam dung 60s. Hay mo Web UI (Port 8080) de xem ngay!")
time.sleep(60)

spark.stop()