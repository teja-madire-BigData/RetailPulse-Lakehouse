import argparse
import uuid
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


import silver_utils as u

SILVER_RUN_ID = str(uuid.uuid4())

print("===== parsing arguments =====")
parser = argparse.ArgumentParser()
parser.add_argument("--partition_date", required=True, help="YYYY-MM-DD")
parser.add_argument("--run_timestamp",   required=True, help="ISO timestamp from Airflow")
args = parser.parse_args()
PARTITION_DATE = args.partition_date
SILVER_RUN_TIMESTAMP = args.run_timestamp
print("===== arguments parsed =====")
    
# Table refs
print("===== setup tables =====")
CATALOG = "glue_catalog"
BRONZE_TABLE = f"{CATALOG}.ecom_bronze.bronze_products"
SILVER_TABLE = f"{CATALOG}.ecom_silver.silver_products"

print(f"===== BRONZE_TABLE={BRONZE_TABLE} =====")
print(f"===== SILVER_TABLE={SILVER_TABLE} =====")

print("===== tables set up =====")

# Spark
print("===== creating spark session =====")
spark = SparkSession.builder \
    .appName(f"silver_products_{PARTITION_DATE}") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")


print(f"===== partition_date={PARTITION_DATE} =====")

bronze_df = spark.read.format("iceberg") \
    .load(BRONZE_TABLE) \
    .filter(F.col("ingestion_date") == PARTITION_DATE)

print('===== bronze df schema before transformations =====')
bronze_df.printSchema()    

row_count = u.get_row_count(bronze_df)

print(f"===== Bronze rows read: {row_count} =====")

if row_count == 0:
    print(f"===== No data found in {BRONZE_TABLE} for {PARTITION_DATE} — exiting. =====")
    spark.stop()
    exit(0)

print("===== deduplicating rows =====")    

bronze_df = u.deduplication(bronze_df, "product_id", "ingested_at")    

print(f"===== After dedup: {u.get_row_count(bronze_df)} products =====")    

print("===== processing data =====")

silver_df = bronze_df \
    .withColumn(
        "price_vs_base",
        F.round(F.col("current_price") / F.col("base_price"), 4)
    ) \
    .withColumn("last_updated_at",       F.col("ingested_at")) \
    .withColumn("last_run_id",           F.col("run_id")) \
    .withColumn("run_timestamp", F.lit(SILVER_RUN_TIMESTAMP).cast("timestamp")) \
    .withColumn("run_id",                F.lit(SILVER_RUN_ID)) \
    .withColumn("silver_processed_date", F.lit(PARTITION_DATE).cast("date")) \
    .drop(
        "dataset",
        "source",
        "ingestion_date",
        "orders_last_15min",
        "ingested_at",       
    )
print("===== silver df schema after transformation =====")
silver_df.printSchema()

print("===== creating temp view =====")
silver_df.createOrReplaceTempView("updates")

print(f"===== upserting into {SILVER_TABLE} =====")
spark.sql(f"""
    MERGE INTO {SILVER_TABLE} AS target
    USING updates AS source
    ON target.product_id = source.product_id
    WHEN MATCHED THEN
        UPDATE SET *
    WHEN NOT MATCHED THEN
        INSERT *
""")

print(f"===== upsert into {SILVER_TABLE} completed =====")

spark.stop()

print("===== processing completed =====")