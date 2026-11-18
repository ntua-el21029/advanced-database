from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, to_timestamp, regexp_replace, split, array_contains, when, count, avg, sum
import time

spark = SparkSession.builder.appName("query 3 dataframe").getOrCreate()

spark.sparkContext.setLogLevel("WARN") #both warn and error 

user_df = spark.read.parquet("/home/spirost/advdb/task0/parquet_output/user")
review_df = spark.read.parquet("/home/spirost/advdb/task0/parquet_output/review")
business_df = spark.read.parquet("/home/spirost/advdb/task0/parquet_output/business")

# rename columns to avoid ambiguity during joins
review_df = review_df.withColumnRenamed("stars", "review_stars")
business_df = business_df.withColumnRenamed("stars", "business_stars")

user_df = user_df.withColumn("reg_year", year(to_timestamp(col("yelping_since")))) \
                 .withColumn("cleaned_elite", regexp_replace(col("elite"), "20,20", "2020"))

review_df = review_df.withColumn("review_year", year(to_timestamp(col("date"))).cast("string"))

def benchmark_join(df_to_test, strategy_name):
    print(f"Execution time for: {strategy_name}...")
    start_time = time.time()
    
    try:
        # format("noop") is used to avoid writing to disk just for measure
        df_to_test.write.format("noop").mode("overwrite").save()
        end_time = time.time()
        print(f": {end_time - start_time:.2f} seconds\n")
    except Exception as e:
        print(f"Error: {str(e)[:100]}...\n")

# SortMergeJoin (default)
df_merge = user_df.hint("MERGE").join(review_df, on="user_id", how="left")
benchmark_join(df_merge, "MERGE (SortMergeJoin)")

# BroadcastHashJoin
df_broadcast = user_df.join(review_df.hint("BROADCAST"), on="user_id", how="left")
benchmark_join(df_broadcast, "BROADCAST (BroadcastHashJoin)")

#  ShuffleHashJoin
df_shuffle = user_df.hint("SHUFFLE_HASH").join(review_df, on="user_id", how="left")
benchmark_join(df_shuffle, "SHUFFLE_HASH (ShuffleHashJoin)")

# joins
# left join to ensure we keep all users, even those without reviews
# their review_id will be null
df = user_df.join(review_df, on="user_id", how="left")
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

# final calculations and aggregations for the tasks
result_df = df.groupBy("reg_year").agg(
    count("review_id").alias("total_reviews"),                                    
    avg("review_stars").alias("average_rating"),                                      
    avg("strictness").alias("average_strictness"),                                    
    ((sum("is_elite_review") / count("review_id")) * 100).alias("elite_percent"),     
    sum("no_review_user").alias("users_without_reviews")                          
).orderBy("reg_year")

result_df.explain(mode="formatted")
result_df.show(1000, truncate=False)

spark.stop()

