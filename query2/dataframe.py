from pyspark.sql.functions import col, split, explode, trim, count, avg, round, when, sum as spark_sum, row_number
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
import time

start_time = time.time()

spark = SparkSession.builder \
    .appName("Dataframe Query2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN") #both warn and error 

business_df = spark.read.parquet("/home/spirost/advdb/task0/parquet_output/business/")
reviews_df = spark.read.parquet("/home/spirost/advdb/task0/parquet_output/review/")

#  returns 2 columns: state, number of businesses and filters out states with less than 1000 businesses
valid_states = business_df.groupBy("state").count().filter(col("count") >= 1000)

# exact businesses in these states
target_business = (
    business_df.join(valid_states, on="state", how="inner")    
    .filter(col("categories").isNotNull()) 
    .withColumn("category", explode(split(col("categories"), ",")))   
    .withColumn("category", trim(col("category")))    # remove leading and trailing spaces
)
# join with reviews because we want exact number of rating 5
joined_df = target_business.join(reviews_df, on="business_id", how="inner")

# calculate the 3 tasks
category_stats = joined_df.groupBy("state", "category").agg(
    count("*").alias("total_reviews"),
    round(avg(reviews_df["stars"]), 2).alias("avg_stars"),
    round((spark_sum(when(reviews_df["stars"] == 5, 1).otherwise(0)) / count("*")) * 100, 1).alias("percent_5_stars")
).filter(col("total_reviews") >= 500)

windowSpec = Window.partitionBy("state").orderBy(col("avg_stars").desc(), col("total_reviews").desc())

final_df = (
    category_stats.withColumn("rn", row_number().over(windowSpec)) 
    .filter(col("rn") <= 3) 
    .drop("rn") 
    .orderBy("state", col("avg_stars").desc())
)

final_df.show(100)
final_df.explain(mode = "formatted")

end_time = time.time()
print(f"Execution time dataframe: {end_time - start_time} seconds")

spark.stop()