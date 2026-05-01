import argparse
import uuid

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

import silver_utils as u

SILVER_RUN_ID        = str(uuid.uuid4())

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
CATALOG = args.catalog  
BRONZE_DB = args.bronze_database
SILVER_DB = args.silver_database
BRONZE_TABLE = f"{CATALOG}.{BRONZE_DB}.{args.bronze_table}"
SILVER_TABLE = f"{CATALOG}.{SILVER_DB}.{args.silver_table}"

print("===== creating spark session =====")
spark = SparkSession.builder \
    .appName(f"silver_orders_{PARTITION_DATE}") \
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

VALID_STATUSES = [
    "pending", "processing", "shipped", "out_for_delivery",
    "delivered", "cancelled", "returned", "refunded"
]

null_order_id = bronze_df.filter(F.col("order_id").isNull()).count()
assert null_order_id == 0, f"[DQ FAIL] {null_order_id} rows with null order_id"

dupe_orders = bronze_df.groupBy("order_id").count().filter(F.col("count") > 1).count()
assert dupe_orders == 0, f"[DQ FAIL] {dupe_orders} duplicate order_ids in partition"

bad_total = bronze_df.filter(F.col("order_total") <= 0).count()
assert bad_total == 0, f"[DQ FAIL] {bad_total} rows with order_total <= 0"

bad_status = bronze_df.filter(~F.col("order_status").isin(VALID_STATUSES)).count()
assert bad_status == 0, f"[DQ FAIL] {bad_status} rows with invalid order_status"

bad_items = bronze_df.filter(F.col("item_count") <= 0).count()
assert bad_items == 0, f"[DQ FAIL] {bad_items} rows with item_count <= 0"

print("===== DQ checks passed =====")

# Deduplicate
print("===== deduplicating =====")
bronze_df = u.deduplication(bronze_df, "order_id", "ingested_at")
print(f"===== After dedup: {u.get_row_count(bronze_df)} orders =====")

# Guard against reprocessing — skip order_ids already in Silver
print("===== checking for existing Silver records =====")
try:
    existing_ids = spark.read.format("iceberg") \
        .load(SILVER_TABLE) \
        .filter(F.col("silver_processed_date") == PARTITION_DATE) \
        .select("order_id")
    bronze_df = bronze_df.join(existing_ids, on="order_id", how="left_anti")
    print(f"===== After Silver dedup: {u.get_row_count(bronze_df)} new orders =====")
except Exception:
    print("===== Silver table empty — first load =====")

if u.get_row_count(bronze_df) == 0:
    print("===== All orders already processed — idempotent rerun. Exiting. =====")
    spark.stop()
    exit(0)

# Transformations
print("===== applying transformations =====")

silver_df = bronze_df \
    .withColumn("estimated_delivery_at",
        F.when(
            F.col("estimated_delivery_at").isNull() |
            (F.trim(F.col("estimated_delivery_at").cast("string")) == ""),
            F.lit(None)
        ).otherwise(F.col("estimated_delivery_at"))
    ) \
    .withColumn("has_discount",
        F.col("discount_total") > 0
    ) \
    .withColumn("order_value_band",
        F.when(F.col("order_total") < 100,   F.lit("low"))
         .when(F.col("order_total") < 500,   F.lit("medium"))
         .when(F.col("order_total") < 2000,  F.lit("high"))
         .otherwise(                          F.lit("premium"))
    ) \
    .withColumn("days_to_delivery",
        F.when(
            F.col("estimated_delivery_at").isNotNull(),
            F.datediff(
                F.col("estimated_delivery_at").cast("date"),
                F.col("ordered_at").cast("date")
            )
        ).otherwise(None)
    ) \
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

# Append to Silver
print(f"===== appending to {SILVER_TABLE} =====")
silver_df.writeTo(SILVER_TABLE).append()

print(f"===== append complete in {SILVER_TABLE} =====")
spark.stop()
print("===== processing completed =====")