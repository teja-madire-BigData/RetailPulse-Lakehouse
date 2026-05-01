from __future__ import annotations
import pendulum
from datetime import timedelta
from airflow.sdk import DAG
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor

CLUSTER_ID = "{{ var.value.emr_cluster_id }}"
S3_BUCKET = "s3://{{ var.value.emr_s3_bucket_name }}"
SCRIPTS_PATH = f"{S3_BUCKET}/silver_layer"

SPARK_CONF = [
    "--conf", "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "--conf", "spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog",
    "--conf", f"spark.sql.catalog.glue_catalog.warehouse={S3_BUCKET}/",
    "--conf", "spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog",
    "--conf", "spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO",
]

SILVER_TABLES = [
    "silver_fx_rates",
    "silver_orders",
    "silver_products",
    "silver_sellers",
    "silver_users",
    "silver_weather",
]

default_args = {
    "owner": "retailpulse",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id = "silver_table_settings",
    description = "One-time: disable object-storage writes on all silver tables",
    schedule = "@once",
    start_date = pendulum.datetime(2026, 4, 30, tz="UTC"),
    catchup = False,
    default_args = default_args,
    tags = ["silver", "one-time", "settings"],
) as dag:

    add_step = EmrAddStepsOperator(
        task_id = "add_table_settings_step",
        job_flow_id = CLUSTER_ID,
        aws_conn_id = "aws_default",
        steps = [{
            "Name": "silver_table_settings",
            "ActionOnFailure": "CONTINUE",
            "HadoopJarStep": {
                "Jar":  "command-runner.jar",
                "Args": [
                    "spark-submit",
                    "--deploy-mode", "cluster",
                ] + SPARK_CONF + [
                    f"{SCRIPTS_PATH}/silver_table_settings.py",
                    "--catalog", "glue_catalog",
                    "--database", "ecom_silver",
                    "--tables", ",".join(SILVER_TABLES),
                ],
            },
        }],
    )

    wait_step = EmrStepSensor(
        task_id = "wait_table_settings_step",
        job_flow_id = CLUSTER_ID,
        step_id = "{{ task_instance.xcom_pull('add_table_settings_step')[0] }}",
        aws_conn_id = "aws_default",
        poke_interval = 30,
        timeout = 1800,
        target_states = ["COMPLETED"],
        failed_states = ["FAILED", "CANCELLED", "INTERRUPTED"],
    )

    add_step >> wait_step

