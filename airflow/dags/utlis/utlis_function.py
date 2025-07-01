import os
from glob import glob
import logging
import pandas as pd
from google.cloud import bigquery
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_local import GCSToLocalFilesystemOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook


# Define the Python function to check file existence
def check_file_exists(filename="TMDB_movie_dataset_v11.csv"):
    INPUT_PATH = f"/opt/bitnami/spark/resources/dataset/{filename}"
    
    if not os.path.exists(INPUT_PATH):
        raise AirflowFailException(f"Dataset not found at: {INPUT_PATH}")
    
    logging.info(f"Dataset found at: {INPUT_PATH}")


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


def merge_gcs_csv_shards(bucket: str, prefix: str, destination: str):
    hook = GCSHook(gcp_conn_id="google_cloud_default")
    client = hook.get_conn()
    blobs = list(client.list_blobs(bucket, prefix=prefix))
    blobs_sorted = sorted(blobs, key=lambda b: b.name)
    
    first = True
    merged_data = []
    for blob in blobs_sorted:
        data = blob.download_as_bytes().splitlines()
        if first:
            merged_data.extend(data)
            first = False
        else:
            merged_data.extend(data[1:])
    
    # Upload merged file
    merged_blob = client.bucket(bucket).blob(destination)
    merged_blob.upload_from_string(b"\n".join(merged_data).decode('utf-8'))


def validate_csv(filepath: str):
    logger = logging.getLogger("airflow.task")
    logger.info(f"Starting validation for file: {filepath}")

    try:
        df = pd.read_csv(filepath)
        logger.info(f"Loaded CSV with shape: {df.shape}")

        expected_columns = ["movie_id", "title", "genres", "keywords", "overview"]
        actual_columns = list(df.columns)

        # 1. Column name validation
        if actual_columns != expected_columns:
            raise ValueError(f"Schema mismatch! Expected: {expected_columns}, Got: {actual_columns}")
        logger.info("Column names validation passed")

        # 2. Data type validation
        if not pd.api.types.is_integer_dtype(df["movie_id"]):
            raise ValueError("Column 'movie_id' must be of type INTEGER")
        for col in ["title", "genres", "keywords", "overview"]:
            if not pd.api.types.is_string_dtype(df[col]):
                raise ValueError(f"Column '{col}' must be of type STRING")
        logger.info("Data type validation passed")

        # 3. Null check
        if df.isnull().any().any():
            null_report = df.isnull().sum()
            raise ValueError(f"Null values found:\n{null_report}")
        logger.info("Null value check passed")

        # 4. Duplicate check on movie_id
        if df.duplicated(subset=["movie_id"]).any():
            duplicates = df[df.duplicated(subset=["movie_id"], keep=False)]
            raise ValueError(f"Duplicate movie_id found:\n{duplicates}")
        logger.info("Duplicate ID check passed")

        # 5. Text length check
        text_columns = ["title", "overview"]
        for col in text_columns:
            blank_rows = df[col].astype(str).str.strip() == ""
            if blank_rows.any():
                raise ValueError(f"Blank or whitespace-only values found in '{col}':\n{df[blank_rows]}")
        logger.info("Text length check passed")

        # 6. Corrupt character check
        corrupted_char = "�"
        corrupted_found = df.apply(lambda col: col.astype(str).str.contains(corrupted_char)).any()
        if corrupted_found.any():
            corrupt_columns = corrupted_found[corrupted_found].index.tolist()
            raise ValueError(f"Corrupted character '{corrupted_char}' found in columns: {corrupt_columns}")
        logger.info("Encoding check passed")

        logger.info("Data validation completed successfully.")

    except Exception as e:
        logger.error(f"Validation failed: {str(e)}")
        raise
