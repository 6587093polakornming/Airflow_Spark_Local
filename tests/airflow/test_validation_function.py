import unittest
from unittest.mock import patch
import pandas as pd
import logging

# 👇 Local copy of validate_csv for isolated testing
def validate_csv(filepath: str):
    logger = logging.getLogger("test")
    logger.info(f"Starting validation for file: {filepath}")

    try:
        df = pd.read_csv(filepath)
        logger.info(f"Loaded CSV with shape: {df.shape}")

        expected_columns = ["movie_id", "title", "genres", "keywords", "overview"]
        actual_columns = list(df.columns)

        if actual_columns != expected_columns:
            raise ValueError(f"Schema mismatch! Expected: {expected_columns}, Got: {actual_columns}")
        logger.info("Column names validation passed")

        if not pd.api.types.is_integer_dtype(df["movie_id"]):
            raise ValueError("Column 'movie_id' must be of type INTEGER")
        for col in ["title", "genres", "keywords", "overview"]:
            if not pd.api.types.is_string_dtype(df[col]):
                raise ValueError(f"Column '{col}' must be of type STRING")
        logger.info("Data type validation passed")

        if df.isnull().any().any():
            null_report = df.isnull().sum()
            raise ValueError(f"Null values found:\n{null_report}")
        logger.info("Null value check passed")

        if df.duplicated(subset=["movie_id"]).any():
            duplicates = df[df.duplicated(subset=["movie_id"], keep=False)]
            raise ValueError(f"Duplicate movie_id found:\n{duplicates}")
        logger.info("Duplicate ID check passed")

        text_columns = ["title", "overview"]
        for col in text_columns:
            blank_rows = df[col].astype(str).str.strip() == ""
            if blank_rows.any():
                raise ValueError(f"Blank or whitespace-only values found in '{col}':\n{df[blank_rows]}")
        logger.info("Text length check passed")

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


# ✅ Unit test
class TestValidateCSV(unittest.TestCase):

    @patch("pandas.read_csv")
    def test_valid_csv(self, mock_read_csv):
        df = pd.DataFrame({
            "movie_id": [1, 2],
            "title": ["Inception", "Matrix"],
            "genres": ["Action", "Sci-Fi"],
            "keywords": ["dream", "virtual"],
            "overview": ["A mind-bending story", "Simulation theory"]
        })
        mock_read_csv.return_value = df

        try:
            validate_csv("fake/path.csv")
        except Exception:
            self.fail("validate_csv raised an exception on valid input.")


if __name__ == "__main__":
    unittest.main()
