# read_from_bq.py
from pyspark.sql import SparkSession
import os

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("ReadFromBigQuery") \
        .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.42.2") \
        .config("parentProject", "datapipeline467803") \
        .getOrCreate()

    # Set credentials path via env var or option
    # os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/opt/bitnami/spark/resources/credential/key.json"
    CREDENTIAL_PATH = "/opt/bitnami/spark/resources/credential/key.json"

    df = spark.read.format("bigquery") \
        .option("table", "datapipeline467803.tmdb_dw.fact_movie") \
        .option("credentialsFile", CREDENTIAL_PATH) \
        .load()

    df.show(5)  # Print up to 10 rows
    # df.coalesce(1).write.csv('/opt/bitnami/spark/resources/output/exported_demo_data', header = True)
    # df.write.mode("overwrite").csv('/opt/bitnami/spark/resources/output/exported_demo_data.csv', header = True)
    # df.coalesce(1).write.mode("overwrite").csv('/opt/bitnami/spark/resources/output/exported_demo_data', header = True)
    
    # output_base_path = '/opt/spark/output/fact_movie_csv_export'
    # df.coalesce(1).write.mode("overwrite").csv(output_base_path, header = True)

    spark.stop()