from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import BooleanType, DoubleType, IntegerType
from pyspark.sql.functions import split, expr, trim, lower, explode, col, concat_ws, collect_list, sum
from utlis.clean_function import clean_genres_column, clean_production_countries_column

READ_DIR_PATH = "/opt/bitnami/spark/resources/dataset/"
FLIE_NAME = "TMDB_movie_dataset_v11.csv"
OUTPUT_PATH = "/opt/shared/output/cleaned_data"
ISOFILE_NAME = "iso_countries_cleaned.csv"
VALIDATE_FILE = READ_DIR_PATH+ISOFILE_NAME
read_file = READ_DIR_PATH + FLIE_NAME

# List of valid genres
genres_lst = ["Crime", "Romance", "TV Movie", "Thriller", "Adventure", "Drama", "War", "Documentary", "Family",
              "Fantasy", "History", "Mystery", "Animation", "Music", "Science Fiction", "Horror", "Western", "Comedy", "Action"]

if __name__ == "__main__":
    spark = SparkSession.builder \
    .appName("Spark_Cleansing") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

    df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(read_file)

    # Step 1 Drop Columns
    drop_col_list = ["backdrop_path", "homepage", "imdb_id", "original_title", "poster_path", "tagline"]
    df_step1 = df.drop(*drop_col_list)

    # Step 2 Drop Null Value
    dropna_col_lst = ["title", "release_date", "adult", "budget", "original_language", "overview", "popularity", "genres", "production_companies", "production_countries", "spoken_languages", "keywords"]
    df_step2 = df_step1.na.drop(subset=dropna_col_lst)

    # Step 3 Drop Duplicate Rows
    df_step3 = df_step2.dropDuplicates(["id"])

    # Step 4 Change Data Type
    df_step4 = df_step3.withColumn("vote_average", df_step3["vote_average"].cast(DoubleType())) \
                    .withColumn("popularity", df_step3["popularity"].cast(DoubleType())) \
                    .withColumn("vote_count", df_step3["vote_count"].cast(IntegerType())) \
                    .withColumn("revenue", df_step3["revenue"].cast(IntegerType())) \
                    .withColumn("runtime", df_step3["runtime"].cast(IntegerType())) \
                    .withColumn("budget", df_step3["budget"].cast(IntegerType())) \
                    .withColumn("adult", df_step3["adult"].cast(BooleanType()))

    # Step 5 Change Date Datatype
    df_step5 = df_step4.withColumn("release_date",f.to_timestamp(df_step4.release_date, 'yyyy-MM-dd'))

    # Step 6 Filter Cutoff Date
    cutoff_date = "2025-06-02" # Define cutoff date (end of year 2025)

    # Keep only rows where release_date is less than or equal to the cutoff
    df_step6 = df_step5.filter(f.col("release_date") <= f.lit(cutoff_date))

    # Step 7 Filter Range Numeric Values
    df_step7 = df_step6.filter( 
                                (col("vote_average") > 0.0) & \
                                (col("vote_count") > 0) & \
                                (col("revenue") > 0) & \
                                (col("runtime") > 0) & \
                                (col("budget") > 0) & \
                                (col("popularity") > 0.0)
                            )

    # Step 8 Filter Status Column is Release
    df_step8 = df_step7.filter(f.col("status") == "Released")

    # Step 9 Cleaning Genres Column
    df_step9 = clean_genres_column(genres_lst, df_step8)

    # Step 10 Cleansing Production Contries
    df_iso = spark.read.option("header", True).csv(VALIDATE_FILE)
    df_cleaned = clean_production_countries_column(df_iso, df_step9)

    df_cleaned.show(5)  # Print up to 10 rows

    df_cleaned.coalesce(1).write.mode("overwrite").csv(OUTPUT_PATH, header = True)

    spark.stop()