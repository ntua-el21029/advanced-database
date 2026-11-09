from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("SQL API Query1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

df = spark.read.parquet("/home/spirost/advdb/task0/parquet_output/review")
df.createOrReplaceTempView("reviews")


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
    FROM reviews
    WHERE year BETWEEN 2015 AND 2021
      AND useful >= 1
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
        FROM reviews
        WHERE year BETWEEN 2015 AND 2021
          AND useful >= 1
        GROUP BY 
            year(date),
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

spark.stop()