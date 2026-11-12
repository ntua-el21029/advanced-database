from pyspark.sql import SparkSession
import time

start_time = time.time()

spark = SparkSession.builder \
    .appName("SQL Query2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

business_df = spark.read.parquet("/home/spirost/advdb/task0/parquet_output/business/")
reviews_df = spark.read.parquet("/home/spirost/advdb/task0/parquet_output/review/")

business_df.createOrReplaceTempView("business")
reviews_df.createOrReplaceTempView("review")

sql_query = """
SELECT state, category, total_reviews, avg_stars, percent_5_stars
FROM (
    SELECT 
        state, 
        category, 
        total_reviews, 
        avg_stars, 
        percent_5_stars,
        ROW_NUMBER() OVER(PARTITION BY state ORDER BY avg_stars DESC, total_reviews DESC) as rn
    FROM (
        SELECT 
            b.state,
            TRIM(c.category) AS category,
            COUNT(r.review_id) AS total_reviews,
            ROUND(AVG(r.stars), 2) AS avg_stars,
            ROUND((SUM(CASE WHEN r.stars = 5 THEN 1 ELSE 0 END) / COUNT(r.review_id)) * 100, 1) AS percent_5_stars
        FROM business b
        
        INNER JOIN (
            SELECT state 
            FROM business 
            GROUP BY state 
            HAVING COUNT(business_id) >= 1000
        ) valid_states ON b.state = valid_states.state
        
        INNER JOIN review r ON b.business_id = r.business_id
        
        LATERAL VIEW explode(split(b.categories, ',')) c AS category
        
        WHERE b.categories IS NOT NULL
        GROUP BY b.state, TRIM(c.category)
        
        HAVING COUNT(r.review_id) >= 500
    ) grouped_stats
) ranked_stats
WHERE rn <= 3
ORDER BY state, avg_stars DESC
"""

final_sql_df = spark.sql(sql_query)
final_sql_df.show(100)
final_sql_df.explain(mode = "formatted")

end_time = time.time()
print(f"Execution time sql: {end_time - start_time} seconds")

spark.stop()