from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder \
    .appName("WriteToBigQuery") \
    .config("parentProject", "datapipeline467803") \
    .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.42.2") \
    .getOrCreate()

    df = spark.createDataFrame([
        (99, 8.5)
    ], ["movie_id", "vote_average"])

    FACTTABLE_PATH_GCP = "datapipeline467803.tmdb_dw.fact_movie"
    CREDENTIAL_PATH = "/opt/bitnami/spark/resources/credential/key.json"

    df.write.format("bigquery") \
    .option("table", FACTTABLE_PATH_GCP) \
    .option("parentProject", "datapipeline467803") \
    .option("writeMethod", "direct") \
    .option("credentialsFile", CREDENTIAL_PATH) \
    .mode("append") \
    .save()

    spark.stop()