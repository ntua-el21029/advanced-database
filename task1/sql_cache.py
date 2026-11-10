from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import time

spark = SparkSession.builder \
    .appName("SQL API Query1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

df = spark.read.parquet("/home/spirost/advdb/task0/parquet_output/review")

# Partition at 
filtered_df = df.filter(
    (col("year") >= 2015) & 
    (col("year") <= 2021) & 
    (col("useful") >= 1)
)

filtered_df.createOrReplaceTempView("filtered_reviews") # Necessary for SQL to see the DataFrame

start_time = time.time()

filtered_df.cache()  # enable cache
filtered_df.count()  # force spark to materialize the cache

query_a = """
    SELECT 
        CASE 
            WHEN length(text) < 250 THEN 'Short'
            WHEN length(text) BETWEEN 250 AND 749 THEN 'Medium'
            WHEN length(text) BETWEEN 750 AND 1999 THEN 'Long'
            ELSE 'Very Long'
        END AS length_category,
        COUNT(*) AS total_reviews,
        AVG(stars) AS avg_stars,
        AVG(useful) AS avg_useful
    FROM filtered_reviews
    GROUP BY 
        CASE 
            WHEN length(text) < 250 THEN 'Short'
            WHEN length(text) BETWEEN 250 AND 749 THEN 'Medium'
            WHEN length(text) BETWEEN 750 AND 1999 THEN 'Long'
            ELSE 'Very Long'
        END
    ORDER BY avg_useful DESC
"""

result_a = spark.sql(query_a)
result_a.show(truncate=False)

query_b = """
    WITH CategoryCounts AS (
        SELECT 
            year AS review_year,
            CASE 
                WHEN length(text) < 250 THEN 'Short'
                WHEN length(text) BETWEEN 250 AND 749 THEN 'Medium'
                WHEN length(text) BETWEEN 750 AND 1999 THEN 'Long'
                ELSE 'Very Long'
            END AS length_category,
            COUNT(*) AS cat_by_year_count
        FROM filtered_reviews
        GROUP BY 
            year,
            CASE 
                WHEN length(text) < 250 THEN 'Short'
                WHEN length(text) BETWEEN 250 AND 749 THEN 'Medium'
                WHEN length(text) BETWEEN 750 AND 1999 THEN 'Long'
                ELSE 'Very Long'
            END
    )
    SELECT 
        review_year,
        length_category,
        ROUND((cat_by_year_count / SUM(cat_by_year_count) OVER (PARTITION BY review_year)) * 100, 2) AS percentage
    FROM CategoryCounts
    ORDER BY review_year, length_category
"""

result_b = spark.sql(query_b)
result_b.show(100, truncate=False)

end_time = time.time()
print(f"Total time with cache: : {end_time - start_time} seconds")
time.sleep(300)
spark.stop()