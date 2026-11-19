import sys
import time
from pyspark.sql import SparkSession

# read the strategy from command line arguments, default to "DEFAULT" if not provided
strategy = sys.argv[1].upper() if len(sys.argv) > 1 else "DEFAULT"

aqe_enabled = sys.argv[2].lower() if len(sys.argv) > 2 else "false"

spark = SparkSession.builder \
    .appName(f"query 3 SQL - {strategy} - AQE_{aqe_enabled}") \
    .config("spark.sql.adaptive.enabled", aqe_enabled) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Read parquet files and register them as temporary SQL views
spark.read.parquet("/home/spirost/advdb/task0/parquet_output/user").createOrReplaceTempView("user")
spark.read.parquet("/home/spirost/advdb/task0/parquet_output/review").createOrReplaceTempView("review")
spark.read.parquet("/home/spirost/advdb/task0/parquet_output/business").createOrReplaceTempView("business")

# Build the SQL join hint dynamically
hint_clause = "" if strategy == "DEFAULT" else f"/*+ {strategy}(r) */"

query = f"""
    SELECT {hint_clause}
        YEAR(TO_TIMESTAMP(u.yelping_since)) AS reg_year,
        COUNT(r.review_id) AS total_reviews,
        AVG(r.stars) AS average_rating,
        AVG(r.stars - b.stars) AS average_strictness,
        (SUM(
            CASE
                WHEN r.review_id IS NOT NULL
                 AND u.elite IS NOT NULL
                 AND ARRAY_CONTAINS(
                     SPLIT(REGEXP_REPLACE(u.elite, '20,20', '2020'), ','),
                     CAST(YEAR(TO_TIMESTAMP(r.date)) AS STRING)
                 )
                THEN 1 ELSE 0
            END
        ) / COUNT(r.review_id)) * 100 AS elite_percent,
        SUM(
            CASE WHEN r.review_id IS NULL THEN 1 ELSE 0 END
        ) AS users_without_reviews
    FROM user u
    LEFT JOIN review r ON u.user_id = r.user_id
    LEFT JOIN business b ON r.business_id = b.business_id
    GROUP BY YEAR(TO_TIMESTAMP(u.yelping_since))
    ORDER BY reg_year
"""

result_df = spark.sql(query)

print(f"\n--- PHYSICAL PLAN SQL ({strategy} | AQE={aqe_enabled}) ---")
result_df.explain(mode="formatted")

print(f"Execution time for SQL: {strategy} (AQE={aqe_enabled})...")
start_time = time.time()
try:
    result_df.write.format("noop").mode("overwrite").save()
    end_time = time.time()
    print(f"Completed in: {end_time - start_time:.2f} seconds\n")

    result_df.show(1000, truncate=False)
except Exception as e:
    print(f"Failed ({strategy}): {str(e)[:150]}...\n")

spark.stop()