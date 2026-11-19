import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, to_timestamp, regexp_replace, split, array_contains, when, count, avg, sum

# read the strategy from command line arguments, default to "DEFAULT" if not provided
strategy = sys.argv[1].upper() if len(sys.argv) > 1 else "DEFAULT"

aqe_enabled = sys.argv[2].lower() if len(sys.argv) > 2 else "false"

spark = SparkSession.builder \
    .appName(f"query 3 - {strategy} - AQE_{aqe_enabled}") \
    .config("spark.sql.adaptive.enabled", aqe_enabled) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

user_df = spark.read.parquet("/home/spirost/advdb/task0/parquet_output/user")
review_df = spark.read.parquet("/home/spirost/advdb/task0/parquet_output/review")
business_df = spark.read.parquet("/home/spirost/advdb/task0/parquet_output/business")

# Rename columns to avoid ambiguity during joins
review_df = review_df.withColumnRenamed("stars", "review_stars")
business_df = business_df.withColumnRenamed("stars", "business_stars")

user_df = user_df.withColumn("reg_year", year(to_timestamp(col("yelping_since")))) \
                 .withColumn("cleaned_elite", regexp_replace(col("elite"), "20,20", "2020"))

review_df = review_df.withColumn("review_year", year(to_timestamp(col("date"))).cast("string"))

if strategy == "DEFAULT":
    df = user_df.join(review_df, on="user_id", how="left")
else:
    df = user_df.join(review_df.hint(strategy), on="user_id", how="left")

df = df.join(business_df, on="business_id", how="left")

df = df.withColumn("strictness", col("review_stars") - col("business_stars"))

df = df.withColumn(
    "is_elite_review",
    when(
        col("review_id").isNotNull() & 
        col("cleaned_elite").isNotNull() & 
        array_contains(split(col("cleaned_elite"), ","), col("review_year")), 
        1
    ).otherwise(0)
)

df = df.withColumn(
    "no_review_user",
    when(col("review_id").isNull(), 1).otherwise(0)
)

# Final calculations and aggregations
result_df = df.groupBy("reg_year").agg(
    count("review_id").alias("total_reviews"),
    avg("review_stars").alias("average_rating"),
    avg("strictness").alias("average_strictness"),
    ((sum("is_elite_review") / count("review_id")) * 100).alias("elite_percent"),
    sum("no_review_user").alias("users_without_reviews")
).orderBy("reg_year")

print(f"\n--- PHYSICAL PLAN ({strategy} | AQE={aqe_enabled}) ---")
result_df.explain(mode="formatted")

print(f"Execution time for: {strategy} (AQE={aqe_enabled})...")
start_time = time.time()
try:
    result_df.write.format("noop").mode("overwrite").save()
    end_time = time.time()
    print(f"Completed in: {end_time - start_time:.2f} seconds\n")

    #result_df.show(1000, truncate=False)
except Exception as e:
    print(f"Failed ({strategy}): {str(e)[:150]}...\n")

spark.stop()