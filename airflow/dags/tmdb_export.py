# DAG Documentation
"""
## 📝 DAG Documentation - Export_Recom_Dataset_from_GCP

### Export pipeline for generating and validating content-based recommendation dataset

This DAG exports a materialized view (`cbf_movie_recommendations_view`) from BigQuery into Google Cloud Storage (GCS) in Parquet format. It then merges the exported shards into a single Parquet file, downloads the final file to the Airflow local volume, and validates the file structure for downstream use in machine learning and analytics.

⚙️  
Default Arguments  
&ensp;&ensp;  
🧑‍💻 **Owner**: &ensp; `Polakorn Anantapakorn` &emsp; | &emsp; 🕒 **Schedule**: &ensp; `None` &emsp; | &emsp; 🗓️ **Start Date**: &ensp; `days_ago(1)`  

####  
📋 Pipeline Info  
-  
📌 **Source View**: &ensp; `cbf_movie_recommendations_view`  
🗃️ **BigQuery Table**: &ensp; `datapipeline467803.tmdb_dw.cbf_movie_recommendations_view`  
📤 **Export Target (GCS)**: &ensp; `gs://tmdb-reco-flow-bucket/output/cbf_movie_*.parquet`  
📦 **Final Output Path**: &ensp; `gs://tmdb-reco-flow-bucket/final/cbf_movie.parquet`  
📥 **Local Output File**: &ensp; `/opt/airflow/data/cbf_movie.parquet`  
🔗 **GitHub Link**: &ensp; [Export DAG on GitHub](https://github.com/6587093polakornming/TMDB_RecoFlow.git)

---

🛠️ **Main Components**
- ✅ Export BigQuery view as Parquet shards
- ✅ Merge shards into a single GCS Parquet file
- ✅ Download file to Airflow container
- ✅ Validate structure and schema for downstream use

---

📞 Contact  
&ensp;&ensp;  
📧 **Requestor Team**: &ensp; `My Supervisor - Data Engineer` &emsp; | &emsp; 👥 **Source Team**: &ensp; `N/A` &emsp; | &emsp; 🧑‍💻 **Users Team**: &ensp; `Data Scientist`  
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
