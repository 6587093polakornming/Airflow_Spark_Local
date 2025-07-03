### TODO DAGs Doc
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

from airflow import DAG
from utlis.utlis_function import merge_gcs_parquet_shards, validate_parquet
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_local import (
    GCSToLocalFilesystemOperator,
)


default_args = {
    "owner": "Polakorn Anantapakorn",
    "start_date": days_ago(1),
    "email": ["supakorn.ming@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}


BUCKET_URI = "gs://tmdb-reco-flow-bucket"
OUTPUT_FILENAME = "cbf_movie"
PROJECT_ID = "datapipeline467803"
DATASET_ID = "tmdb_dw"
TABLE_NAME = "cbf_movie_recommendations_view"


with DAG(
    dag_id="Export_Recom_Dataset_from_GCP",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["project"],
) as dag:

    dag.doc_md = __doc__

    # 1️ Export Parquet shards from BigQuery
    export_bq = BigQueryInsertJobOperator(
        task_id="export_view_to_gcs",
        configuration={
            "query": {
                "query": f"""
                    EXPORT DATA OPTIONS (
                      uri='{BUCKET_URI}/output/{OUTPUT_FILENAME}_*.parquet',
                      format='PARQUET',
                      overwrite=true
                    )
                    AS
                    SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}`;
                """,
                "useLegacySql": False,
            }
        },
        location="US",
        gcp_conn_id="google_cloud_default",
    )

    # 2️ Merge Parquet shards into one file
    merge_parquet = PythonOperator(
        task_id="merge_parquet_shards",
        python_callable=merge_gcs_parquet_shards,
        op_kwargs={
            "bucket": "tmdb-reco-flow-bucket",
            "prefix": f"output/{OUTPUT_FILENAME}_",
            # "prefix": f"output/cbf_movie_demo",
            "destination": f"final/{OUTPUT_FILENAME}.parquet",
        },
    )

    # 3️ Download final Parquet file
    download = GCSToLocalFilesystemOperator(
        task_id="download_from_gcs",
        bucket="tmdb-reco-flow-bucket",
        object_name=f"final/{OUTPUT_FILENAME}.parquet",
        filename=f"/opt/airflow/data/{OUTPUT_FILENAME}.parquet",
        gcp_conn_id="google_cloud_default",
    )

    # 4️ Validate Parquet data
    validate = PythonOperator(
        task_id="validate_data",
        python_callable=validate_parquet,
        op_kwargs={"filepath": f"/opt/airflow/data/{OUTPUT_FILENAME}.parquet"},
    )

    # ====== Task Dependencies ======
    export_bq >> merge_parquet >> download >> validate
