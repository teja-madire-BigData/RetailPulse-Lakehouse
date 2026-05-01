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

# Spark
print("===== creating spark session =====")
spark = SparkSession.builder \
    .appName(f"silver_users_{PARTITION_DATE}") \
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

# Deduplicate
print("===== deduplicating =====")
bronze_df = u.deduplication(bronze_df, "user_id", "ingested_at")
print(f"===== After dedup: {u.get_row_count(bronze_df)} users =====")

# Transformations
print("===== applying transformations =====")

silver_df = bronze_df \
    .withColumn("name_clean",
        F.regexp_replace(
            F.regexp_replace(
                F.trim(F.col("name")),
                r'(?i)^(dr\.|mr\.|mrs\.|ms\.|prof\.|rev\.|sir\.?)\s+', ''
            ),
            r'(?i)\s+(jr\.?|sr\.?|md|phd|ii|iii|iv|esq\.?)$', ''
        )
    ) \
    .withColumn("first_name",
        F.split(F.col("name_clean"), ' ')[0]
    ) \
    .withColumn("last_name",
        F.when(
            F.size(F.split(F.col("name_clean"), ' ')) > 1,
            F.array_join(
                F.slice(
                    F.split(F.col("name_clean"), ' '),
                    2,
                    F.size(F.split(F.col("name_clean"), ' '))
                ),
                ' '
            )
        ).otherwise(None)
    ) \
    .drop("name_clean") \
    .withColumn("is_valid_email",
        F.col("email").rlike(
            r'^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$'
        )
    ) \
    .withColumn("email_domain",
        F.lower(F.element_at(F.split(F.col("email"), '@'), 2))
    ) \
    .withColumn("phone_country_code",
        F.when(
            F.col("phone").rlike(r'^\+\d{1,3}[\s\-]'),
            F.concat(
                F.lit("+"),
                F.regexp_extract(F.col("phone"), r'^\+(\d{1,3})[\s\-]', 1)
            )
        ).when(
            F.col("phone").rlike(r'^00\d{1,3}[\s\-]'),
            F.concat(
                F.lit("+"),
                F.regexp_extract(F.col("phone"), r'^00(\d{1,3})[\s\-]', 1)
            )
        ).otherwise(None)
    ) \
    .withColumn("has_country_code",
        F.col("phone").rlike(r'^\+\d{1,3}[\s\-]|^00\d{1,3}[\s\-]')
    ) \
    .withColumn("phone_number_clean",
        F.regexp_replace(
            F.regexp_replace(
                F.regexp_replace(
                    F.regexp_replace(
                        F.col("phone"),
                        r'^\+\d{1,3}[\s\-]', ''   # strip +1-
                    ),
                    r'^00\d{1,3}[\s\-]', ''         # strip 001-
                ),
                r'(?i)x\d+$', ''                    # strip extension x634
            ),
            r'\D', ''                               # digits only
        )
    ) \
    .withColumn("account_age_days",
        F.datediff(F.current_date(), F.col("created_at").cast("date"))
    ) \
    .withColumn("last_updated_at",       F.col("ingested_at")) \
    .withColumn("last_run_id",           F.col("run_id")) \
    .withColumn("run_id",                F.lit(SILVER_RUN_ID)) \
    .withColumn("run_timestamp",         F.lit(SILVER_RUN_TIMESTAMP).cast("timestamp")) \
    .withColumn("silver_processed_date", F.lit(PARTITION_DATE).cast("date")) \
    .drop(
        "name",
        "email",
        "phone",
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
    ON target.user_id = source.user_id
    WHEN MATCHED THEN
        UPDATE SET *
    WHEN NOT MATCHED THEN
        INSERT *
""")

print(f"===== upsert complete in {SILVER_TABLE} =====")
spark.stop()
print("===== processing completed =====")