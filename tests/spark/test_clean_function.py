import unittest
from pyspark.sql import SparkSession
from pyspark.sql import Row
import sys
import os
from app.utlis.clean_function import clean_double_quotes

class TestCleanDoubleQuotes(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local[1]").appName("Test").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_clean_single_column(self):
        df = self.spark.createDataFrame([
            Row(title='"Inception"', overview='A "mind" bending movie')
        ])
        cleaned_df = clean_double_quotes(df, columns=["title"])
        result = cleaned_df.collect()[0]
        self.assertEqual(result["title"], 'Inception')
        self.assertEqual(result["overview"], 'A "mind" bending movie')  # Not cleaned

    def test_clean_multiple_columns(self):
        df = self.spark.createDataFrame([
            Row(title='"Inception"', overview='A "mind" bending movie')
        ])
        cleaned_df = clean_double_quotes(df, columns=["title", "overview"])
        result = cleaned_df.collect()[0]
        self.assertEqual(result["title"], 'Inception')
        self.assertEqual(result["overview"], 'A mind bending movie')

    def test_column_not_exist(self):
        df = self.spark.createDataFrame([
            Row(title='"Inception"')
        ])
        cleaned_df = clean_double_quotes(df, columns=["not_a_column"])
        result = cleaned_df.collect()[0]
        self.assertEqual(result["title"], '"Inception"')  # Unchanged

if __name__ == '__main__':
    unittest.main()
