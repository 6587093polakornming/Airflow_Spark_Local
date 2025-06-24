import os
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException

# Define the Python function to check file existence
def check_file_exists():
    input_path = "/opt/bitnami/spark/resources/dataset/TMDB_movie_dataset_v11.csv"
    if not os.path.exists(input_path):
        raise AirflowFailException(f"Dataset not found at: {input_path}")
    print(f"Dataset found at: {input_path}")

