import os
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException
from utlis.utlis_function import check_file_exists


default_args = {
    'owner': 'Polakorn Anantapakorn',
    'start_date': days_ago(1),
    'email': ['supakorn.ming@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

spark_master = "spark://spark-master:7077"
READ_DIR_PATH = "/opt/bitnami/spark/resources/dataset/"
FLIE_NAME = "TMDB_movie_dataset_v11.csv"
INPUT_FILE = READ_DIR_PATH + FLIE_NAME

with DAG(
    dag_id="ETL_tmdb_dataset",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=['project'],
) as dag:
    
    check_dataset_is_exist = PythonOperator(
    task_id="check_dataset_exists",
    python_callable=check_file_exists
    ) 

    cleasing_data = SparkSubmitOperator(
        task_id="cleasing_data",
        application="/opt/bitnami/spark/app/clean_data.py",
        name="spark_cleasing_data",
        conn_id="spark_default",
        conf={"spark.master":spark_master}
    )

    check_dataset_is_exist >> cleasing_data