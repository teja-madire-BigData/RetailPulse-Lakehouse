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

default_args = {
    "owner": "retailpulse",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id = "silver_orders",
    description = "Bronze to Silver: processing for orders",
    schedule = "0 2 * * *",
    start_date = pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup = False,
    default_args = default_args,
    tags = ["silver", "orders"],
) as dag:

    PARTITION_DATE = "{{ (logical_date - macros.timedelta(days=1)).strftime('%Y-%m-%d') }}"

    add_step = EmrAddStepsOperator(
        task_id = "add_silver_orders_step",
        job_flow_id = CLUSTER_ID,
        aws_conn_id = "aws_default",
        steps = [{
            "Name": "silver_orders",
            "ActionOnFailure": "CONTINUE",
            "HadoopJarStep": {
                "Jar":  "command-runner.jar",
                "Args": [
                    "spark-submit",
                    "--deploy-mode", "cluster",
                    "--py-files", f"{SCRIPTS_PATH}/silver_utils.py",
                ] + SPARK_CONF + [
                    f"{SCRIPTS_PATH}/silver_orders.py",
                    "--partition_date", PARTITION_DATE,
                    "--run_timestamp", "{{ logical_date.isoformat() }}",
                ],
            },
        }],
    )

    wait_step = EmrStepSensor(
        task_id = "wait_silver_orders",
        job_flow_id = CLUSTER_ID,
        step_id = "{{ task_instance.xcom_pull('add_silver_orders_step')[0] }}",
        aws_conn_id = "aws_default",
        poke_interval = 30,
        timeout = 3600,
        target_states = ["COMPLETED"],       
        failed_states = ["FAILED", "CANCELLED", "INTERRUPTED"],
    )

    add_step >> wait_step
