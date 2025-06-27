import os
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException
from utlis.utlis_function import check_file_exists

### TODO use ENV variable (docker, docker compose, env, conf) 
#   Don't use direct path of credential file

default_args = {
    'owner': 'Polakorn Anantapakorn',
    'start_date': days_ago(1),
    'email': ['supakorn.ming@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

spark_master = "spark://spark-master:7077"


with DAG(
    dag_id="ETL_tmdb_dataset",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=['project'],
) as dag:
    
    check_dataset_is_exist_task = PythonOperator(
    task_id="check_dataset_exists",
    python_callable=check_file_exists
    ) 

    cleasing_data_task = SparkSubmitOperator(
        task_id="cleasing_data",
        application="/opt/bitnami/spark/app/clean_data.py",
        name="spark_cleasing_data",
        conn_id="spark_default",
        conf={"spark.master":spark_master}
    )

    ### TODO create task transform data

    ### TODO add transform data
    check_dataset_is_exist_task >> cleasing_data_task