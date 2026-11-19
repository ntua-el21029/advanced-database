import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, to_timestamp, regexp_replace, split, array_contains, when, count, avg, sum

spark = SparkSession.builder \
    .appName("query 3 - SHUFFLE_REPLICATE_NL") \
    .config("spark.sql.adaptive.enabled", "false") \
    .config("spark.sql.ansi.enabled", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

user_df = spark.read.parquet("/home/spirost/advdb/task0/parquet_output/user")
review_df = spark.read.parquet("/home/spirost/advdb/task0/parquet_output/review")
business_df = spark.read.parquet("/home/spirost/advdb/task0/parquet_output/business")

u_count = user_df.count()
r_count = review_df.count()
print(f"\nROW COUNTS")
print(f"Users (|U|):   {u_count:,}")
print(f"Reviews (|R|): {r_count:,}")
print(f"Estimated NL Comparisons (|U| x |R|): {u_count * r_count:,}\n")

review_df = review_df.withColumnRenamed("stars", "review_stars") \
                     .withColumn("review_year", year(to_timestamp(col("date"))).cast("string"))
business_df = business_df.withColumnRenamed("stars", "business_stars")
user_df = user_df.withColumn("reg_year", year(to_timestamp(col("yelping_since")))) \
                 .withColumn("cleaned_elite", regexp_replace(col("elite"), "20,20", "2020"))

def build_query(u_df, r_df, b_df):
    df = u_df.join(r_df.hint("SHUFFLE_REPLICATE_NL"), on="user_id", how="inner") \
             .join(b_df, on="business_id", how="left") \
             .withColumn("strictness", col("review_stars") - col("business_stars")) \
             .withColumn(
                 "is_elite_review",
                 when(
                     col("review_id").isNotNull() &
                     col("cleaned_elite").isNotNull() &
                     array_contains(split(col("cleaned_elite"), ","), col("review_year")),
                     1
                 ).otherwise(0)
             ) \
             .withColumn("no_review_user", when(col("review_id").isNull(), 1).otherwise(0))

    return df.groupBy("reg_year").agg(
        count("review_id").alias("total_reviews"),
        avg("review_stars").alias("average_rating"),
        avg("strictness").alias("average_strictness"),
        ((sum("is_elite_review") / count("review_id")) * 100).alias("elite_percent"),
        sum("no_review_user").alias("users_without_reviews")
    ).orderBy("reg_year")


full_result = build_query(user_df, review_df, business_df)
print("PHYSICAL PLAN (SHUFFLE_REPLICATE_NL | FULL DATASET)")
full_result.explain(mode="formatted")


print("RUNNING ON 0.1% SAMPLE")
user_sample = user_df.sample(fraction=0.001, seed=42)
review_sample = review_df.sample(fraction=0.001, seed=42)

sample_result = build_query(user_sample, review_sample, business_df)

start_time = time.time()
sample_result.write.format("noop").mode("overwrite").save()
end_time = time.time()
print(f"0.1% Sample completed in: {end_time - start_time:.2f} seconds\n")

spark.stop()