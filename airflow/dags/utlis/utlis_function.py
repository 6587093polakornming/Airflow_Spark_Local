import os
import logging
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException

# Define the Python function to check file existence
def check_file_exists():
    INPUT_PATH = "/opt/bitnami/spark/resources/dataset/TMDB_movie_dataset_v11.csv"
    
    if not os.path.exists(INPUT_PATH):
        raise AirflowFailException(f"Dataset not found at: {INPUT_PATH}")
    
    logging.info(f"Dataset found at: {INPUT_PATH}")