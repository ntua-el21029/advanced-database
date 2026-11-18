from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("query 3 sql").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

user_df = spark.read.parquet("/home/spirost/advdb/task0/parquet_output/user")
review_df = spark.read.parquet("/home/spirost/advdb/task0/parquet_output/review")
business_df = spark.read.parquet("/home/spirost/advdb/task0/parquet_output/business")

user_df.createOrReplaceTempView("user")
review_df.createOrReplaceTempView("review")
business_df.createOrReplaceTempView("business")


query = """
WITH CleanedUsers AS (
    SELECT 
        user_id,
        YEAR(CAST(yelping_since AS TIMESTAMP)) AS reg_year,
        REGEXP_REPLACE(elite, '20,20', '2020') AS cleaned_elite
    FROM user
),
CleanedReviews AS (
    SELECT 
        review_id,
        user_id,
        business_id,
        stars AS review_stars,
        CAST(YEAR(CAST(date AS TIMESTAMP)) AS STRING) AS review_year
    FROM review
)

SELECT 
    u.reg_year,
    
    COUNT(r.review_id) AS total_reviews,
    
    AVG(r.review_stars) AS average_rating,
    
    AVG(r.review_stars - b.stars) AS average_strictness,
    
    (SUM(
        CASE 
            WHEN r.review_id IS NOT NULL 
             AND u.cleaned_elite IS NOT NULL 
             AND ARRAY_CONTAINS(SPLIT(u.cleaned_elite, ','), r.review_year) 
            THEN 1 ELSE 0 
        END
    ) / COUNT(r.review_id)) * 100 AS elite_percent,
    
    SUM(CASE WHEN r.review_id IS NULL THEN 1 ELSE 0 END) AS users_without_reviews

FROM CleanedUsers u
LEFT JOIN CleanedReviews r ON u.user_id = r.user_id
LEFT JOIN business b ON r.business_id = b.business_id
GROUP BY u.reg_year
ORDER BY u.reg_year
"""

result_sql = spark.sql(query)
result_sql.show(1000, truncate=False)

spark.stop()