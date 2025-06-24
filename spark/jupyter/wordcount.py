from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.appName("WordCount").getOrCreate()
    sc = spark.sparkContext
    lines = sc.parallelize(["hello world", "hello airflow", "hello spark"])
    counts = lines.flatMap(lambda x: x.split(" ")) \
                  .map(lambda x: (x, 1)) \
                  .reduceByKey(lambda a, b: a + b)
    print(counts.collect())
    spark.stop()
