from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    StringType, FloatType, IntegerType, LongType, MapType
)
from pyspark.sql.functions import year, to_timestamp

# ── Spark Session ───────────────────────────────────────────────────────────────
spark = (SparkSession.builder
    .appName("YelpToParquet")
    .master("spark://localhost:7077")
    .config("spark.driver.memory", "2g")
    .config("spark.executor.memory", "8g")
    .config("spark.executor.cores", "6")
    .getOrCreate()
)

RAW_DIR    = "/home/spirost/advdb/raw"
OUTPUT_DIR = "/home/spirost/advdb/task0/parquet_output"

# ── Schemas ─────────────────────────────────────────────────────────────────────

review_schema = StructType([
    StructField("review_id",   StringType(),  nullable=True),
    StructField("user_id",     StringType(),  nullable=True),
    StructField("business_id", StringType(),  nullable=True),
    StructField("stars",       FloatType(),   nullable=True),
    StructField("useful",      IntegerType(), nullable=True),
    StructField("funny",       IntegerType(), nullable=True),
    StructField("cool",        IntegerType(), nullable=True),
    StructField("text",        StringType(),  nullable=True),
    StructField("date",        StringType(),  nullable=True),
])

business_schema = StructType([
    StructField("business_id",  StringType(),                        nullable=True),
    StructField("name",         StringType(),                        nullable=True),
    StructField("address",      StringType(),                        nullable=True),
    StructField("city",         StringType(),                        nullable=True),
    StructField("state",        StringType(),                        nullable=True),
    StructField("postal_code",  StringType(),                        nullable=True),
    StructField("latitude",     FloatType(),                         nullable=True),
    StructField("longitude",    FloatType(),                         nullable=True),
    StructField("stars",        FloatType(),                         nullable=True),
    StructField("review_count", IntegerType(),                       nullable=True),
    StructField("is_open",      IntegerType(),                       nullable=True),
    StructField("attributes",   MapType(StringType(), StringType()), nullable=True),
    StructField("categories",   StringType(),                        nullable=True),
    StructField("hours",        MapType(StringType(), StringType()), nullable=True),
])

user_schema = StructType([
    StructField("user_id",            StringType(),  nullable=True),
    StructField("name",               StringType(),  nullable=True),
    StructField("review_count",       IntegerType(), nullable=True),
    StructField("yelping_since",      StringType(),  nullable=True),
    StructField("useful",             LongType(),    nullable=True),
    StructField("funny",              LongType(),    nullable=True),
    StructField("cool",               LongType(),    nullable=True),
    StructField("elite",              StringType(),  nullable=True),
    StructField("friends",            StringType(),  nullable=True),
    StructField("fans",               IntegerType(), nullable=True),
    StructField("average_stars",      FloatType(),   nullable=True),
    StructField("compliment_hot",     IntegerType(), nullable=True),
    StructField("compliment_more",    IntegerType(), nullable=True),
    StructField("compliment_profile", IntegerType(), nullable=True),
    StructField("compliment_cute",    IntegerType(), nullable=True),
    StructField("compliment_list",    IntegerType(), nullable=True),
    StructField("compliment_note",    IntegerType(), nullable=True),
    StructField("compliment_plain",   IntegerType(), nullable=True),
    StructField("compliment_cool",    IntegerType(), nullable=True),
    StructField("compliment_funny",   IntegerType(), nullable=True),
    StructField("compliment_writer",  IntegerType(), nullable=True),
    StructField("compliment_photos",  IntegerType(), nullable=True),
])

tip_schema = StructType([
    StructField("user_id",          StringType(),  nullable=True),
    StructField("business_id",      StringType(),  nullable=True),
    StructField("text",             StringType(),  nullable=True),
    StructField("date",             StringType(),  nullable=True),
    StructField("compliment_count", IntegerType(), nullable=True),
])

checkin_schema = StructType([
    StructField("business_id", StringType(), nullable=True),
    StructField("date",        StringType(), nullable=True),
])

# Convert JSON to Parquet

# 1. Business
print("Converting business...")
df_business = spark.read.schema(business_schema).json(f"{RAW_DIR}/yelp_academic_dataset_business.json")
df_business.write.mode("overwrite").parquet(f"{OUTPUT_DIR}/business")

# 2. User
print("Converting user...")
df_user = spark.read.schema(user_schema).json(f"{RAW_DIR}/yelp_academic_dataset_user.json")
df_user.coalesce(4).write.mode("overwrite").parquet(f"{OUTPUT_DIR}/user")

# 3. Tip
print("Converting tip...")
df_tip = spark.read.schema(tip_schema).json(f"{RAW_DIR}/yelp_academic_dataset_tip.json")
df_tip.write.mode("overwrite").parquet(f"{OUTPUT_DIR}/tip")

# 4. Checkin
print("Converting checkin...")
df_checkin = spark.read.schema(checkin_schema).json(f"{RAW_DIR}/yelp_academic_dataset_checkin.json")
df_checkin.write.mode("overwrite").parquet(f"{OUTPUT_DIR}/checkin")

# 5. Review — partitionBy year
print("Converting review (partitioned by year)...")
df_review = spark.read.schema(review_schema).json(f"{RAW_DIR}/yelp_academic_dataset_review.json")

# Add a new column "year" extracted from the "date" column
df_review = df_review.withColumn("year", year(to_timestamp("date", "yyyy-MM-dd HH:mm:ss")))

df_review.write.mode("overwrite").partitionBy("year").parquet(f"{OUTPUT_DIR}/review")

print("Done! All datasets converted to Parquet.")
spark.stop()

