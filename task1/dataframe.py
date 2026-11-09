from pyspark.sql.functions import col, year, length, when, count, avg
from pyspark.sql.window import Window
from pyspark.sql.functions import sum, round
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Dataframe Query1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN") #both warn and error 

# parquet to dataframe
df = spark.read.parquet("/home/spirost/advdb/task0/parquet_output/review") 

filtered_df = df.filter(
    (col("year") >= 2015) & 
    (col("year") <= 2021) & 
    (col("useful") >= 1)
)

# new column for text length 
categorized_df = filtered_df.withColumn(
    "length_category",
    when(length(col("text")) < 250, "Short")
    .when((length(col("text")) >= 250) & (length(col("text")) <= 749), "Medium")
    .when((length(col("text")) >= 750) & (length(col("text")) <= 1999), "Long")
    .otherwise("Very Long")
)

# final result
result = categorized_df.groupBy("length_category").agg(
    count("*").alias("total_reviews"),
    avg("stars").alias("avg_stars"),
    avg("useful").alias("avg_useful")
).orderBy(col("avg_useful").desc()) 


result.explain()
# print results
result.show(truncate=False)

# b 
df_with_year = categorized_df.withColumn("review_year", year(col("date")))

counts_df = df_with_year.groupBy("review_year", "length_category").agg(count("*").alias("cat_by_year_count"))

# window function to calculate total reviews per year
window_year = Window.partitionBy("review_year")

result_b = counts_df.withColumn("total_year", sum("cat_by_year_count").over(window_year)) \
    .withColumn("percentage", round((col("cat_by_year_count") / col("total_year")) * 100, 2)) \
    .orderBy("review_year", "length_category")


result_b.select("review_year", "length_category", "percentage").show(100)

spark.stop()