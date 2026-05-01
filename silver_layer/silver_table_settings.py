"""
One-time script to disable object-storage writes on silver tables.
Run via spark-submit on EMR with Iceberg + Glue Catalog configs.

Usage:
    spark-submit silver_table_settings.py \
        --catalog glue_catalog \
        --database ecom_silver \
        --tables silver_fx_rates,silver_orders,silver_products
"""
import argparse
from pyspark.sql import SparkSession

parser = argparse.ArgumentParser()
parser.add_argument("--catalog", required=True)
parser.add_argument("--database", required=True)
parser.add_argument("--tables", required=True, help="Comma-separated list of table names")
args = parser.parse_args()

spark = SparkSession.builder \
    .appName("silver_table_settings") \
    .getOrCreate()

tables = [t.strip() for t in args.tables.split(",")]

for table in tables:
    spark.sql(f"""
        ALTER TABLE {args.catalog}.{args.database}.{table}
        SET TBLPROPERTIES ('write.object-storage.enabled' = 'false')
    """)
    print(f"✅ {table}: write.object-storage.enabled = false")

print(f"✅ All {len(tables)} tables updated successfully")

spark.stop()
