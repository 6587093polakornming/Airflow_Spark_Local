# dags/read_bq_dag.py
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {'owner': 'you', 'retries': 0}

with DAG(
    dag_id="spark_read_from_bigquery",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=['spark', 'bigquery'],
) as dag:

    read_bq = SparkSubmitOperator(
        task_id="read_bq",
        application="/opt/bitnami/spark/app/read_data_bq.py",
        name="spark_read_bq_job",
        conn_id="spark_default",
        packages="com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.42.2",
    )

    read_bq