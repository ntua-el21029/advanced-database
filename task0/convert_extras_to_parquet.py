from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, ArrayType, FloatType
)
from pyspark.sql.functions import col, split, slice, transform

# ── Spark Session ───────────────────────────────────────────────────────────────
spark = (SparkSession.builder
    .appName("ExtrasToParquet")
    .master("spark://localhost:7077")
    .config("spark.driver.memory", "2g")
    .config("spark.executor.memory", "8g")
    .config("spark.executor.cores", "6")
    .getOrCreate()
)

RAW_DIR    = "/home/spirost/advdb/raw"
OUTPUT_DIR = "/home/spirost/advdb/task0/parquet_output"

# ── 1. GloVe ────────────────────────────────────────────────────────────────────
# Μορφή: "the -0.038194 -0.24487 0.72812 ..."
# → word: STRING, vec: ARRAY<FLOAT>

print("Converting GloVe...")

# Διαβάζουμε ως raw text (κάθε γραμμή = μία εγγραφή)
glove_raw = spark.read.text(f"{RAW_DIR}/glove.6B.100d.txt")

glove_df = (
    glove_raw
    # Σπάμε κάθε γραμμή στα κενά
    .withColumn("parts", split(col("value"), " "))
    # Το πρώτο token είναι η λέξη
    .withColumn("word", col("parts")[0])
    # Τα υπόλοιπα 100 tokens είναι τα floats → ARRAY<FLOAT>
    .withColumn("vec", transform(
        slice(col("parts"), 2, 100),   # tokens[1..100]
        lambda x: x.cast(FloatType())
    ))
    .select("word", "vec")
)

glove_schema = StructType([
    StructField("word", StringType(),              nullable=False),
    StructField("vec",  ArrayType(FloatType()),    nullable=False),
])

# Επαλήθευση schema
print("GloVe schema:")
glove_df.printSchema()
glove_df.show(3, truncate=60)

glove_df.write.mode("overwrite").parquet(f"{OUTPUT_DIR}/glove")
print("GloVe done.")

# ── 2. ACS ZCTA ─────────────────────────────────────────────────────────────────
# Μορφή CSV με ; ως separator:
#   zcta;median_household_income;population
# → zcta ως STRING (όχι integer, γιατί έχει leading zeros π.χ. "00601")

print("Converting acs_zcta...")

acs_schema = StructType([
    StructField("zcta",                   StringType(),  nullable=True),
    StructField("median_household_income", IntegerType(), nullable=True),
    StructField("population",              IntegerType(), nullable=True),
])

acs_df = (spark.read
    .schema(acs_schema)
    .option("header", "true")
    .option("sep", ";")
    .csv(f"{RAW_DIR}/acs_zcta.csv")
)

print("ACS ZCTA schema:")
acs_df.printSchema()
acs_df.show(3)

acs_df.write.mode("overwrite").parquet(f"{OUTPUT_DIR}/acs_zcta")
print("ACS ZCTA done.")

print("\nAll extra datasets converted to Parquet.")
spark.stop()

