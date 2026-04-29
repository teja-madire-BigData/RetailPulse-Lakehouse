from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window

def deduplication(df: DataFrame, partition_key: str, order_by: str) -> DataFrame:
    window = Window.partitionBy(partition_key).orderBy(F.col(order_by).desc())
    return df.withColumn("row_num", F.row_number().over(window)) \
             .filter(F.col("row_num") == 1) \
             .drop("row_num")

def get_row_count(df: DataFrame) -> int:
    return df.count()