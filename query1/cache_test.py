import time
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Cache All Crash Test") \
    .getOrCreate()

df = spark.read.parquet("/home/spirost/advdb/task0/parquet_output/review/")

print(f"Starting cache test...")
df.cache()

start_time = time.time()

try:
    total_rows = df.count()
    end_time = time.time()
    print(f"Measured {total_rows} rows.")
    print(f"Completion time: {end_time - start_time} seconds")
except Exception as e:
    print(f"System crashed. Error: {e}")

time.sleep(600)