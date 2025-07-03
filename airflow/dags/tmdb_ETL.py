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
from datetime import timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from utlis.utlis_function import check_file_exists, upload_parquet_folder_to_bq
from airflow.providers.google.cloud.sensors.bigquery import BigQueryTableExistenceSensor
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCheckOperator,
    BigQueryGetDataOperator,
)


default_args = {
    "owner": "Polakorn Anantapakorn",
    "start_date": days_ago(1),
    "email": ["supakorn.ming@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

spark_master = "spark://spark-master:7077"
PROJECT_ID = "datapipeline467803"
DATASET_ID = "tmdb_dw"
load_dataset_lst = [
    {"table_name": "dim_movie", "parquet_filename": "dim_movie"},
    {"table_name": "dim_keyword", "parquet_filename": "dim_keywords"},
    {
        "table_name": "dim_production_company",
        "parquet_filename": "dim_production_companies",
    },
    {"table_name": "dim_spoken_language", "parquet_filename": "dim_spoken_languages"},
    {
        "table_name": "dim_production_country",
        "parquet_filename": "dim_production_countries",
    },
    {"table_name": "dim_genre", "parquet_filename": "dim_genres"},
    {"table_name": "bridge_movie_keyword", "parquet_filename": "bridge_keywords"},
    {
        "table_name": "bridge_movie_company",
        "parquet_filename": "bridge_production_companies",
    },
    {
        "table_name": "bridge_movie_language",
        "parquet_filename": "bridge_spoken_languages",
    },
    {
        "table_name": "bridge_movie_country",
        "parquet_filename": "bridge_production_countries",
    },
    {"table_name": "bridge_movie_genre", "parquet_filename": "bridge_genres"},
    {"table_name": "fact_movie", "parquet_filename": "fact_movie"},
]


with DAG(
    dag_id="ETL_tmdb_dataset",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=["project"],
) as dag:

    dag.doc_md = __doc__

    check_dataset_is_exist_task = PythonOperator(
        task_id="check_dataset_exists", python_callable=check_file_exists
    )

    ### TODO change CSV file to Parquet
    cleasing_data_task = SparkSubmitOperator(
        task_id="cleansing_data",
        application="/opt/bitnami/spark/app/clean_data.py",
        name="spark_cleansing_data",
        conn_id="spark_default",
        conf={"spark.master": spark_master},
    )

    ### TODO change CSV file to Parquet
    transform_data_task = SparkSubmitOperator(
        task_id="transform_data",
        application="/opt/bitnami/spark/app/transform_data.py",
        name="spark_transform_data",
        conn_id="spark_default",
        conf={"spark.master": spark_master},
    )

    # TaskGroup for BigQuery uploads
    with TaskGroup(
        group_id="load_to_bigquery_group", tooltip="Upload to BigQuery"
    ) as load_group:

        # Sort load tasks: dimensions -> bridge -> fact
        dimension_tables = [
            d for d in load_dataset_lst if d["table_name"].startswith("dim_")
        ]
        bridge_tables = [
            d for d in load_dataset_lst if d["table_name"].startswith("bridge_")
        ]
        fact_tables = [
            d for d in load_dataset_lst if d["table_name"].startswith("fact_")
        ]

        ordered_tasks = dimension_tables + bridge_tables + fact_tables

        for dataset in ordered_tasks:
            table_name = dataset["table_name"]
            parquet_folder = f"/opt/shared/output/{dataset['parquet_filename']}"
            table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"

            PythonOperator(
                task_id=f"upload_{table_name}",
                python_callable=upload_parquet_folder_to_bq,
                op_kwargs={
                    "parquet_folder": parquet_folder,
                    "table_id": table_id,
                    "gcp_conn_id": "google_cloud_default",
                },
            )

    # After TaskGroup load_to_bigquery_group
    with TaskGroup(
        group_id="validate_bigquery_group", tooltip="Validate BigQuery loads"
    ) as validate_group:
        for dataset in ordered_tasks:
            table = dataset["table_name"]
            table_ref = f"{PROJECT_ID}.{DATASET_ID}.{table}"

            # 1️ Ensure the table exists
            t_exist = BigQueryTableExistenceSensor(
                task_id=f"check_{table}_exists",
                project_id=PROJECT_ID,
                dataset_id=DATASET_ID,
                table_id=table,
                gcp_conn_id="google_cloud_default",
                # deferrable=True  # optional
            )

            # 2️ Check record count > 0
            t_count = BigQueryCheckOperator(
                task_id=f"check_{table}_has_rows",
                sql=f"SELECT COUNT(*) FROM `{table_ref}`",
                use_legacy_sql=False,
                gcp_conn_id="google_cloud_default",
                # deferrable=True
            )

            # 3️ (Optional) Inspect schema: fetch first row and inspect types
            t_schema = BigQueryGetDataOperator(
                task_id=f"get_{table}_schema_sample",
                dataset_id=DATASET_ID,
                table_id=table,
                max_results=1,
                selected_fields=None,
                gcp_conn_id="google_cloud_default",
            )

            # Chain validations: exists ➝ record count ➝ sample schema
            t_exist >> t_count >> t_schema

    # DAG Dependencies
    (
        check_dataset_is_exist_task
        >> cleasing_data_task
        >> transform_data_task
        >> load_group
        >> validate_group
    )
