import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType
from pyspark.sql.functions import year, col, avg


if len(sys.argv) != 2 or sys.argv[1] not in ["inference", "schema", "parquet"]:
    print("spark-submit ... task1_benchmark.py [inference|schema|parquet]")
    sys.exit(1)

mode = sys.argv[1]

spark = SparkSession.builder.appName(f"Task1_Benchmark_{mode}").getOrCreate()

review_schema = StructType([
    StructField("review_id",   StringType(),  nullable=True),
    StructField("user_id",     StringType(),  nullable=True),
    StructField("business_id", StringType(),  nullable=True),
    StructField("stars",       FloatType(),   nullable=True),
    StructField("useful",      IntegerType(), nullable=True),
    StructField("funny",       IntegerType(), nullable=True),
    StructField("cool",        IntegerType(), nullable=True),
    StructField("text",        StringType(),  nullable=True),
    StructField("date",        StringType(),  nullable=True),
])


def run_query(df):
    query_df = df.withColumn("year", year(col("date"))) \
                 .groupBy("year") \
                 .agg(avg("stars").alias("avg_stars")) \
                 .orderBy("year")
    
    query_df.explain() #print the physical plan for the query
    query_df.collect()

# start measuring wall-clock time
start_time = time.time()

if mode == "inference":
    # Spark infers the schema automatically 
    df = spark.read.json("../raw/yelp_academic_dataset_review.json")
    run_query(df)

elif mode == "schema":
    # Explicit Schema: spark uses the provided schema to read the JSON file
    df = spark.read.schema(review_schema).json("../raw/yelp_academic_dataset_review.json")
    run_query(df)

elif mode == "parquet":
    # Parquet: spark reads the pre-converted Parquet file
    df = spark.read.parquet("parquet_output/review")
    run_query(df)

wall_clock_time = time.time() - start_time
print(f"\n[{mode.upper()}] Wall-clock time: {wall_clock_time:.2f} seconds\n")

spark.stop()