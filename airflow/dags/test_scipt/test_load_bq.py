from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook

from google.cloud import bigquery
import os
from glob import glob
import logging

def upload_parquet_folder_to_bq(parquet_folder, table_id, gcp_conn_id="google_cloud_default"):
    logger = logging.getLogger("airflow.task")
    try:
        logger.info(f"Starting upload for folder: {parquet_folder} to table: {table_id}")

        hook = GoogleBaseHook(gcp_conn_id=gcp_conn_id)
        credentials = hook.get_credentials()
        client = bigquery.Client(credentials=credentials, project=credentials.project_id)

        part_files = sorted(glob(os.path.join(parquet_folder, "*.parquet")))
        logger.info(f"Found {len(part_files)} Parquet files in folder: {parquet_folder}")

        if not part_files:
            raise Exception(f"No Parquet files found in {parquet_folder}")

        for i, file_path in enumerate(part_files):
            write_mode = (
                bigquery.WriteDisposition.WRITE_TRUNCATE if i == 0
                else bigquery.WriteDisposition.WRITE_APPEND
            )

            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.PARQUET,
                write_disposition=write_mode,
            )

            logger.info(f"Uploading file {file_path} to BigQuery table {table_id} with write mode: {write_mode}")
            with open(file_path, "rb") as f:
                load_job = client.load_table_from_file(f, table_id, job_config=job_config)
                load_job.result()

            logger.info(f"Successfully uploaded: {file_path}")

        logger.info(f"All Parquet part files uploaded to {table_id} successfully.")

    except Exception as e:
        logger.error(f"Failed to upload Parquet files to BigQuery: {str(e)}")
        raise


# DAG config
default_args = {"owner": "airflow"}
PROJECT_ID = "datapipeline467803"
DATASET_ID = "tmdb_dw"
TABLE_NAME = "fact_movie"

with DAG(
    dag_id="upload_parquet_folder_to_bigquery",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=['bigquery']
) as dag:

    upload_fact = PythonOperator(
        task_id="upload_fact_movie",
        python_callable=upload_parquet_folder_to_bq,
        op_kwargs={
            "parquet_folder": "/opt/shared/output/fact_movie",
            "table_id": f"{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}"
        }
    )
