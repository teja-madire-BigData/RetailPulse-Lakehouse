import argparse
import uuid

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

import silver_utils as u

SILVER_RUN_ID = str(uuid.uuid4())
# Args
parser = argparse.ArgumentParser()
parser.add_argument("--partition_date",   required=True, help="YYYY-MM-DD")
parser.add_argument("--run_timestamp",    required=True, help="ISO timestamp from Airflow")
parser.add_argument("--catalog",          required=True, help="Catalog name")
parser.add_argument("--bronze_database",  required=True, help="Bronze database")
parser.add_argument("--silver_database",  required=True, help="Silver database")
parser.add_argument("--bronze_table",     required=True, help="Bronze table")
parser.add_argument("--silver_table",     required=True, help="Silver table")

args = parser.parse_args()
PARTITION_DATE = args.partition_date
SILVER_RUN_TIMESTAMP = args.run_timestamp

# Table refs
CATALOG = args.catalog  
BRONZE_DB = args.bronze_database
SILVER_DB = args.silver_database
BRONZE_TABLE = f"{CATALOG}.{BRONZE_DB}.{args.bronze_table}"
SILVER_TABLE = f"{CATALOG}.{SILVER_DB}.{args.silver_table}"

EXPECTED_CITIES = {
    "Mumbai", "Delhi", "New York", "London", "Dubai",
    "Singapore", "Sydney", "Tokyo", "Paris", "Frankfurt"
}
MIN_TEMP_C = -90.0
MAX_TEMP_C =  60.0

# Spark
print("===== creating spark session =====")
spark = SparkSession.builder \
    .appName(f"silver_weather_{PARTITION_DATE}") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")
print(f"===== partition_date={PARTITION_DATE} =====")

# Read Bronze
print("===== reading bronze partition =====")
bronze_df = spark.read.format("iceberg") \
    .load(BRONZE_TABLE) \
    .filter(F.col("ingestion_date") == PARTITION_DATE)

print('===== bronze df schema before transformations =====')
bronze_df.printSchema()    

row_count = u.get_row_count(bronze_df)
print(f"===== Bronze rows read: {row_count} =====")

if row_count == 0:
    print(f"===== No data found for {PARTITION_DATE} — exiting. =====")
    spark.stop()
    exit(0)

# DQ checks
print("===== running DQ checks =====")

null_city = bronze_df.filter(F.col("city").isNull()).count()
assert null_city == 0, f"[DQ FAIL] {null_city} rows with null city"

present_cities = {r.city for r in bronze_df.select("city").distinct().collect()}
missing_cities = EXPECTED_CITIES - present_cities
assert not missing_cities, f"[DQ FAIL] Missing cities: {missing_cities}"

bad_temp = bronze_df.filter(
    F.col("temperature_c").isNull() |
    (F.col("temperature_c") < MIN_TEMP_C) |
    (F.col("temperature_c") > MAX_TEMP_C)
).count()
assert bad_temp == 0, f"[DQ FAIL] {bad_temp} rows with implausible temperature_c"

bad_humidity = bronze_df.filter(
    F.col("humidity_pct").isNull() |
    (F.col("humidity_pct") < 0) |
    (F.col("humidity_pct") > 100)
).count()
assert bad_humidity == 0, f"[DQ FAIL] {bad_humidity} rows with invalid humidity_pct"

print("===== DQ checks passed =====")

# Guard against reprocessing
print("===== checking for existing Silver records =====")
try:
    existing = spark.read.format("iceberg") \
        .load(SILVER_TABLE) \
        .filter(F.col("silver_processed_date") == PARTITION_DATE) \
        .select("city", "observation_time", "run_id")
    bronze_df = bronze_df.join(
        existing,
        on=["city", "observation_time", "run_id"],
        how="left_anti"
    )
    print(f"===== After Silver dedup: {u.get_row_count(bronze_df)} new observations =====")
except Exception:
    print("===== Silver table empty — first load =====")

if u.get_row_count(bronze_df) == 0:
    print("===== Already processed — idempotent rerun. Exiting. =====")
    spark.stop()
    exit(0)

# Transformations
print("===== applying transformations =====")

silver_df = bronze_df \
    .withColumn("last_run_id",           F.col("run_id")) \
    .withColumn("run_id",                F.lit(SILVER_RUN_ID)) \
    .withColumn("run_timestamp",         F.lit(SILVER_RUN_TIMESTAMP).cast("timestamp")) \
    .withColumn("silver_processed_date", F.lit(PARTITION_DATE).cast("date")) \
    .drop(
        "dataset",
        "source",
        "ingestion_date",
        "ingested_at",
    )

print('===== silver df schema after transformation =====')
silver_df.printSchema()

# Append to Silver
print(f"===== appending to {SILVER_TABLE} =====")
silver_df.writeTo(SILVER_TABLE).append()

print(f"===== append complete in {SILVER_TABLE} =====")
spark.stop()
print("===== processing completed =====")