from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    'owner': 'you',
    'retries': 0
}

with DAG(
    dag_id='spark_write_to_bigquery',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=['spark', 'bigquery'],
) as dag:

    spark_to_bq = SparkSubmitOperator(
        task_id='write_to_bq',
        application='/opt/bitnami/spark/app/send_data_bq.py',
        name='spark_to_bq_job',
        conn_id='spark_default',  # This should point to your Spark cluster/container
        packages='com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.42.2',
        verbose=True
    )
