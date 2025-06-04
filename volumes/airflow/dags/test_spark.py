from airflow import DAG
from datetime import datetime
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
}

with DAG(
    dag_id='spark_wordcount',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    spark_wordcount = SparkSubmitOperator(
        task_id='run_wordcount',
        application='/opt/bitnami/spark/app/wordcount.py',
        conn_id='spark_default',
        verbose=1,
        dag=dag,
    )
