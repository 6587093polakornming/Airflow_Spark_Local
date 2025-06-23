from pyspark.sql import SparkSession

READ_PATH = '/opt/shared/output/fact_movie_csv_export'

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("ReadFromSpark") \
        .master("spark://spark-master:7077") \
        .getOrCreate()

    df = spark.read \
        .option("header", True) \
        .option("inferSchema", True) \
        .csv(READ_PATH)

    df.show(5)

    spark.stop()
