import unittest
from pyspark.sql import SparkSession, Row
import sys
import os

# Set path to find `app.utlis.transform_function`
sys.path.insert(0, "/opt/bitnami/spark")

from app.utlis.transform_function import (
    create_dim_table_from_column,
    create_bridge_table
)

class TestTransformFunctions(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .master("local[1]") \
            .appName("TransformFunctionTests") \
            .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    # === Tests for create_dim_table_from_column ===
    def test_dim_table_basic_split(self):
        df = self.spark.createDataFrame([
            Row(genres="Action, Comedy")
        ])
        result_df = create_dim_table_from_column(df, column_name="genres", id_column="genre_id", value_column="genre_name")
        result = [row["genre_name"] for row in result_df.collect()]
        self.assertCountEqual(result, ["Action", "Comedy"])

    def test_dim_table_removes_duplicates_and_trims(self):
        df = self.spark.createDataFrame([
            Row(genres="Action , Comedy , Action")
        ])
        result_df = create_dim_table_from_column(df, column_name="genres", id_column="genre_id", value_column="genre_name")
        result = [row["genre_name"] for row in result_df.collect()]
        self.assertCountEqual(result, ["Action", "Comedy"])

    # === Tests for create_bridge_table ===
    def test_bridge_table_creation(self):
        fact_df = self.spark.createDataFrame([
            Row(id=1, genres="Action, Comedy")
        ])
        dim_df = self.spark.createDataFrame([
            Row(genre_id=10, genre_name="Action"),
            Row(genre_id=20, genre_name="Comedy")
        ])
        bridge_df = create_bridge_table(
            fact_df=fact_df,
            dim_df=dim_df,
            fact_id_col="id",
            fact_list_col="genres",
            dim_value_col="genre_name",
            dim_id_col="genre_id",
            bridge_id_col="bridge_id"
        )
        results = bridge_df.select("id", "genre_id").collect()
        pairs = [(row["id"], row["genre_id"]) for row in results]
        self.assertCountEqual(pairs, [(1, 10), (1, 20)])


if __name__ == "__main__":
    unittest.main()
