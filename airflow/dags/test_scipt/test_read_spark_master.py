# dags/read_bq_dag.py
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {'owner': 'you', 'retries': 0}

with DAG(
    dag_id="spark_read_from_spark",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=['spark'],
) as dag:

    read_bq = SparkSubmitOperator(
        task_id="read_spark",
        application="/opt/bitnami/spark/app/read_data_spark.py",
        name="spark_read_spark_job",
        conn_id="spark_default",
    )

    read_bq