from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_local import GCSToLocalFilesystemOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.utils.dates import days_ago

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

with DAG(
    dag_id="bq_to_gcs_to_local",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["bigquery", "bq_to_gcs"],
) as dag:

    # 1️ Export CSV shards from BigQuery view
    export_bq = BigQueryInsertJobOperator(
        task_id="export_view_to_gcs",
        configuration={
            "query": {
                "query": """
                    EXPORT DATA OPTIONS (
                      uri='gs://tmdb-reco-flow-bucket/output/demo_movie_*.csv',
                      format='CSV',
                      overwrite=true,
                      header=true
                    )
                    AS
                    SELECT * FROM `datapipeline467803.tmdb_dw.movie_enriched_view`;
                """,
                "useLegacySql": False,
            }
        },
        location="US",
        gcp_conn_id="google_cloud_default",
    )

    # 2️ Merge CSV shards in GCS into a single clean file
    merge_csv = PythonOperator(
        task_id="merge_csv_shards",
        python_callable=merge_gcs_csv_shards,
        op_kwargs={
            "bucket": "tmdb-reco-flow-bucket",
            "prefix": "output/demo_movie_",
            "destination": "final/demo_movie.csv",
        },
    )

    # 3️ Download the final CSV locally
    download = GCSToLocalFilesystemOperator(
        task_id="download_from_gcs",
        bucket="tmdb-reco-flow-bucket",
        object_name="final/demo_movie.csv",
        filename="/opt/airflow/data/output/demo_movie.csv",
        gcp_conn_id="google_cloud_default",
    )

    # export_bq >> merge_csv >> download
    export_bq >> merge_csv >> download
