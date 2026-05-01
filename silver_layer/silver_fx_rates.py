import argparse
import uuid

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

import silver_utils as u

# arguments from airflow
parser = argparse.ArgumentParser()
parser.add_argument("--partition_date",   required=True, help="YYYY-MM-DD")
parser.add_argument("--run_timestamp",    required=True, help="ISO timestamp from Airflow")
parser.add_argument("--catalog",          required=True, help="Catalog name")
parser.add_argument("--bronze_database",  required=True, help="Bronze database")
parser.add_argument("--silver_database",  required=True, help="Silver database")
parser.add_argument("--bronze_table",     required=True, help="Bronze table")
parser.add_argument("--silver_table",     required=True, help="Silver table")

args = parser.parse_args()
SILVER_RUN_ID = str(uuid.uuid4())
PARTITION_DATE = args.partition_date
SILVER_RUN_TIMESTAMP = args.run_timestamp

# table references
CATALOG = args.catalog  
BRONZE_DB = args.bronze_database
SILVER_DB = args.silver_database
BRONZE_TABLE = f"{CATALOG}.{BRONZE_DB}.{args.bronze_table}"
SILVER_TABLE = f"{CATALOG}.{SILVER_DB}.{args.silver_table}"

EXPECTED_PAIRS = 20
REQUIRED_CURRENCIES = {"EUR", "GBP", "INR", "JPY", "AUD", "AED", "SGD"}

print("===== creating spark session =====")
spark = SparkSession.builder \
    .appName(f"silver_fx_rates_{PARTITION_DATE}") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")
print(f"===== partition_date={PARTITION_DATE} =====")

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

print("===== running DQ checks =====")
assert row_count >= EXPECTED_PAIRS, \
    f"[DQ FAIL] Expected >= {EXPECTED_PAIRS} pairs, got {row_count}"

bad_rate = bronze_df.filter(
    F.col("exchange_rate").isNull() | (F.col("exchange_rate") <= 0)
).count()
assert bad_rate == 0, f"[DQ FAIL] {bad_rate} rows with null or non-positive exchange_rate"

wrong_base = bronze_df.filter(F.col("base_currency") != "USD").count()
assert wrong_base == 0, f"[DQ FAIL] {wrong_base} rows with unexpected base_currency"

present = {r.target_currency for r in bronze_df.select("target_currency").collect()}
missing = REQUIRED_CURRENCIES - present
assert not missing, f"[DQ FAIL] Missing required currencies: {missing}"

print("===== DQ checks passed =====")

print("===== deduplicating =====")
bronze_df = u.deduplication(bronze_df, "pair", "ingested_at")
print(f"===== After dedup: {u.get_row_count(bronze_df)} currency pairs =====")
print("===== checking for existing Silver records =====")
try:
    existing = spark.read.format("iceberg") \
        .load(SILVER_TABLE) \
        .filter(F.col("silver_processed_date") == PARTITION_DATE) \
        .select("pair")
    bronze_df = bronze_df.join(existing, on="pair", how="left_anti")
    print(f"===== After Silver dedup: {u.get_row_count(bronze_df)} new pairs =====")
except Exception:
    print("===== Silver table empty — first load =====")

if u.get_row_count(bronze_df) == 0:
    print("===== Already processed — idempotent rerun. Exiting. =====")
    spark.stop()
    exit(0)

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

print(f"===== appending to {SILVER_TABLE} =====")

print('===== silver df schema after transformation =====')
silver_df.printSchema()

silver_df.writeTo(SILVER_TABLE).append()

print(f"===== append complete in {SILVER_TABLE} =====")
spark.stop()
print("===== processing completed =====")