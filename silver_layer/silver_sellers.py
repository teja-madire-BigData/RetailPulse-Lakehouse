import argparse
import uuid
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

import silver_utils as u

SILVER_RUN_ID = str(uuid.uuid4())

# Args
parser = argparse.ArgumentParser()
parser.add_argument("--partition_date", required=True, help="YYYY-MM-DD")
parser.add_argument("--run_timestamp",  required=True, help="ISO timestamp from Airflow")
args = parser.parse_args()
PARTITION_DATE = args.partition_date
SILVER_RUN_TIMESTAMP = args.run_timestamp

# Table refs
CATALOG = "glue_catalog"
BRONZE_TABLE = f"{CATALOG}.ecom_bronze.bronze_sellers"
SILVER_TABLE = f"{CATALOG}.ecom_silver.silver_sellers"

print("===== creating spark session =====")
spark = SparkSession.builder \
    .appName(f"silver_sellers_{PARTITION_DATE}") \
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

assert row_count >= 90, f"[DQ FAIL] Expected >= 90 sellers, got {row_count}"

null_seller_id = bronze_df.filter(F.col("seller_id").isNull()).count()
assert null_seller_id == 0, f"[DQ FAIL] {null_seller_id} null seller_ids"

bad_rating = bronze_df.filter(
    F.col("seller_rating").isNull() |
    (F.col("seller_rating") < 1.0) |
    (F.col("seller_rating") > 5.0)
).count()
assert bad_rating == 0, f"[DQ FAIL] {bad_rating} rows with invalid seller_rating"

bad_fulfillment = bronze_df.filter(
    F.col("fulfillment_score").isNull() |
    (F.col("fulfillment_score") < 0) |
    (F.col("fulfillment_score") > 100)
).count()
assert bad_fulfillment == 0, f"[DQ FAIL] {bad_fulfillment} rows with invalid fulfillment_score"

print("===== DQ checks passed =====")

# Deduplicate
print("===== deduplicating =====")
bronze_df = u.deduplication(bronze_df, "seller_id", "ingested_at")
print(f"===== After dedup: {u.get_row_count(bronze_df)} sellers =====")

# Transformations
print("===== applying transformations =====")

silver_df = bronze_df \
    .withColumn("seller_tier",
        F.when(F.col("seller_rating") >= 4.5, F.lit("platinum"))
         .when(F.col("seller_rating") >= 4.0, F.lit("gold"))
         .when(F.col("seller_rating") >= 3.5, F.lit("silver"))
         .otherwise(F.lit("bronze"))
    ) \
    .withColumn("account_age_days",
        F.datediff(
            F.current_date(),
            F.to_date(F.col("joined_at"))
        )
    ) \
    .withColumn("last_updated_at",       F.col("ingested_at").cast("timestamp")) \
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

print("===== silver df schema after transformation =====")
silver_df.printSchema()

# MERGE INTO Silver
print("===== creating temp view =====")
silver_df.createOrReplaceTempView("updates")

print(f"===== upserting into {SILVER_TABLE} =====")
spark.sql(f"""
    MERGE INTO {SILVER_TABLE} AS target
    USING updates AS source
    ON target.seller_id = source.seller_id
    WHEN MATCHED THEN
        UPDATE SET *
    WHEN NOT MATCHED THEN
        INSERT *
""")

print(f"===== upsert complete in {SILVER_TABLE} =====")
spark.stop()
print("===== processing completed =====")