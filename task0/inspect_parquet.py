from pyspark.sql import SparkSession

spark = (SparkSession.builder
    .appName("InspectParquet")
    .master("local[*]")
    .config("spark.driver.memory", "4g")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")  # σιγή στα logs

BASE = "/home/spirost/advdb/task0/parquet_output"

datasets = ["business", "user", "tip", "checkin", "review", "glove", "acs_zcta"]

import os

for name in datasets:
    path = f"{BASE}/{name}"
    # Έλεγχος αν υπάρχουν αρχεία .parquet στον φάκελο
    has_files = any(
        f.endswith(".parquet")
        for _, _, files in os.walk(path)
        for f in files
    )
    if not has_files:
        print(f"\n⚠️  {name.upper()} — φάκελος άδειος, παράλειψη")
        continue

    print(f"\n{'='*50}")
    print(f"  {name.upper()}")
    print(f"{'='*50}")
    df = spark.read.parquet(path)
    df.printSchema()
    df.show(3, truncate=60)
    print(f"  → Σύνολο εγγραφών: {df.count():,}")

spark.stop()

