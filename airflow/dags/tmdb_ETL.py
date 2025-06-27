# 📝 DAG Documentation
"""
## 📝 DAG Documentation - ETL_tmdb_dataset  
**ETL pipeline for cleansing and transforming TMDB dataset**

⚙️  
Default Arguments  
&ensp;&ensp;  
🧑‍💻 **Owner**: &ensp; `Polakorn Anantapakorn` &emsp; | &emsp; 🕒 **Schedule**: &ensp; `None` &emsp; | &emsp; 🗓️ **Start Date**: &ensp; `days_ago(1)` &emsp; | &emsp;  
####  
📋 Pipeline Info  
-  
📌  
**Source**: &ensp; `TMDB Dataset (v11)`  
-  
🗂️  
**Source Data**: &ensp; `/opt/bitnami/spark/resources/dataset/TMDB_movie_dataset_v11.csv`  
-  
📦  
**Destination**: &ensp; `/opt/shared/output/`  
-  
🔗  
**Github Link**: &ensp; [ETL_tmdb_dataset](https://github.com/your-org/tmdb-etl-project)  
####  
📞 Contact  
&ensp;&ensp;  
📧 **Requestor Team**: &ensp; `Data Engineering` &emsp; | &emsp; 👥 **Source Team**: &ensp; `TMDB API` &emsp; | &emsp; 🧑‍💻 **Users Team**: &ensp; `ML/Analytics`
"""

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


with DAG(
    dag_id="ETL_tmdb_dataset",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=['project'],
) as dag:
    
    # 👇 Assign docstring to the DAG
    dag.doc_md = __doc__
    
    check_dataset_is_exist_task = PythonOperator(
        task_id="check_dataset_exists",
        python_callable=check_file_exists
    ) 

    cleasing_data_task = SparkSubmitOperator(
        task_id="cleasing_data",
        application="/opt/bitnami/spark/app/clean_data.py",
        name="spark_cleasing_data",
        conn_id="spark_default",
        conf={"spark.master": spark_master}
    )

    transform_data_task = SparkSubmitOperator(
        task_id="transform_data",
        application="/opt/bitnami/spark/app/transform_data.py",
        name="spark_transform_data",
        conn_id="spark_default",
        conf={"spark.master": spark_master}
    )

    # DAG Dependencies
    check_dataset_is_exist_task >> cleasing_data_task >> transform_data_task
