import unittest
from unittest.mock import patch, MagicMock, mock_open
import os
import logging
from glob import glob


# 👇 Fake exception to avoid installing Airflow
class AirflowFailException(Exception):
    pass


# 👇 Copy function: check_file_exists
def check_file_exists(filename="TMDB_movie_dataset_v11.csv"):
    INPUT_PATH = f"/opt/bitnami/spark/resources/dataset/{filename}"
    if not os.path.exists(INPUT_PATH):
        raise AirflowFailException(f"Dataset not found at: {INPUT_PATH}")
    logging.info(f"Dataset found at: {INPUT_PATH}")


# 👇 Copy function: upload_parquet_folder_to_bq (lazy safe)
def upload_parquet_folder_to_bq(
    parquet_folder, table_id, gcp_conn_id="google_cloud_default"
):
    logger = logging.getLogger("test")
    try:
        logger.info(
            f"Starting upload for folder: {parquet_folder} to table: {table_id}"
        )

        # Simulate GCP hook and client
        class FakeCred:
            project_id = "fake-project"

        class FakeHook:
            def get_credentials(self):
                return FakeCred()

        class FakeClient:
            def load_table_from_file(self, file_obj, table_id, job_config=None):
                class Result:
                    def result(self):
                        return None

                return Result()

        hook = FakeHook()
        credentials = hook.get_credentials()
        client = FakeClient()

        part_files = sorted(glob(os.path.join(parquet_folder, "*.parquet")))
        logger.info(
            f"Found {len(part_files)} Parquet files in folder: {parquet_folder}"
        )

        if not part_files:
            raise Exception(f"No Parquet files found in {parquet_folder}")

        for i, file_path in enumerate(part_files):
            write_mode = "WRITE_TRUNCATE" if i == 0 else "WRITE_APPEND"

            logger.info(
                f"Uploading file {file_path} to BigQuery table {table_id} with write mode: {write_mode}"
            )
            with open(file_path, "rb") as f:
                load_job = client.load_table_from_file(f, table_id)
                load_job.result()

            logger.info(f"Successfully uploaded: {file_path}")

        logger.info(f"All Parquet part files uploaded to {table_id} successfully.")

    except Exception as e:
        logger.error(f"Failed to upload Parquet files to BigQuery: {str(e)}")
        raise


# === Test Cases ===
class TestCheckFileExists(unittest.TestCase):

    @patch("os.path.exists")
    def test_file_exists(self, mock_exists):
        mock_exists.return_value = True
        try:
            check_file_exists("dummy.csv")
        except Exception:
            self.fail("check_file_exists() raised Exception unexpectedly!")

    @patch("os.path.exists")
    def test_file_not_exists(self, mock_exists):
        mock_exists.return_value = False
        with self.assertRaises(AirflowFailException):
            check_file_exists("dummy.csv")


class TestUploadParquetToBQ(unittest.TestCase):

    @patch("builtins.open", new_callable=mock_open, read_data=b"dummy_data")
    @patch("__main__.glob")  # 👈 PATCH the correct name
    def test_upload_success(self, mock_glob, mock_file):
        mock_glob.return_value = ["file1.parquet", "file2.parquet"]
        try:
            upload_parquet_folder_to_bq("/some/path", "project.dataset.table")
        except Exception:
            self.fail("upload_parquet_folder_to_bq raised Exception unexpectedly!")

    @patch("glob.glob")
    def test_no_files_found(self, mock_glob):
        mock_glob.return_value = []
        with self.assertRaises(Exception) as context:
            upload_parquet_folder_to_bq("/some/path", "project.dataset.table")
        self.assertIn("No Parquet files found", str(context.exception))


if __name__ == "__main__":
    unittest.main()
