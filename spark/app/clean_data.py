import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import BooleanType, DoubleType, IntegerType
from pyspark.sql.functions import split, expr, trim, lower, explode, col, concat_ws, collect_list, sum
from utlis.clean_function import clean_double_quotes, clean_genres_column, clean_production_countries_column

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

READ_DIR_PATH = "/opt/bitnami/spark/resources/dataset/"
FLIE_NAME = "TMDB_movie_dataset_v11.csv"
ISOFILE_NAME = "iso_countries_cleaned.csv"
VALIDATE_FILE = READ_DIR_PATH + ISOFILE_NAME
READ_FILE = READ_DIR_PATH + FLIE_NAME
OUTPUT_PATH = "/opt/shared/output/cleaned_data"

# List of valid genres
genres_lst = [
    "Crime", "Romance", "TV Movie", "Thriller", "Adventure", "Drama", "War", "Documentary", "Family",
    "Fantasy", "History", "Mystery", "Animation", "Music", "Science Fiction", "Horror", "Western", "Comedy", "Action"
]


if __name__ == "__main__":
    try:
        logger.info("Starting Spark Cleansing Job")

        spark = SparkSession.builder \
            .appName("Spark_Cleansing") \
            .master("spark://spark-master:7077") \
            .getOrCreate()

        logger.info("Spark Session created")

        df = spark.read \
            .option("header", True) \
            .option("inferSchema", True) \
            .csv(READ_FILE)
        logger.info(f"CSV file loaded: {READ_FILE}")

        drop_col_list = ["backdrop_path", "homepage", "imdb_id", "original_title", "poster_path", "tagline"]
        df_step1 = df.drop(*drop_col_list)
        logger.info("Step 1: Dropped unnecessary columns")

        dropna_col_lst = ["title", "release_date", "adult", "budget", "original_language", "overview", "popularity", "genres", "production_companies", "production_countries", "spoken_languages", "keywords"]
        df_step2 = df_step1.na.drop(subset=dropna_col_lst)
        logger.info("Step 2: Dropped null values")

        df_step3 = df_step2.dropDuplicates(["id"])
        logger.info("Step 3: Dropped duplicate rows")

        df_step4 = df_step3.withColumn("vote_average", df_step3["vote_average"].cast(DoubleType())) \
            .withColumn("popularity", df_step3["popularity"].cast(DoubleType())) \
            .withColumn("vote_count", df_step3["vote_count"].cast(IntegerType())) \
            .withColumn("revenue", df_step3["revenue"].cast(IntegerType())) \
            .withColumn("runtime", df_step3["runtime"].cast(IntegerType())) \
            .withColumn("budget", df_step3["budget"].cast(IntegerType())) \
            .withColumn("adult", df_step3["adult"].cast(BooleanType()))
        logger.info("Step 4: Converted data types")

        df_step5 = df_step4.withColumn("release_date", f.to_timestamp(df_step4.release_date, 'yyyy-MM-dd'))
        logger.info("Step 5: Converted release_date to timestamp")

        # NOTE check cut_off_date
        cutoff_date = "2025-06-02"
        df_step6 = df_step5.filter(f.col("release_date") <= f.lit(cutoff_date))
        logger.info(f"Step 6: Filtered by cutoff:{cutoff_date}:(YYYY-MM-DD) release_date")

        df_step7 = df_step6.filter( 
            (col("vote_average") > 0.0) & 
            (col("vote_count") > 0) & 
            (col("revenue") > 0) & 
            (col("runtime") > 0) & 
            (col("budget") > 0) & 
            (col("popularity") > 0.0)
        )
        logger.info("Step 7: Filtered invalid numeric values")

        df_step8 = df_step7.filter(f.col("status") == "Released")
        logger.info("Step 8: Filtered movies with status 'Released'")

        df_step9 = clean_genres_column(genres_lst, df_step8)
        logger.info("Step 9: Cleaned genres column")

        # Apply clean_double_quotes only to specific columns
        clean_columns = ["title", "overview", "keywords"]
        df_step10 = clean_double_quotes(df_step9, clean_columns)
        logger.info(f"Step 10: Removed double quotes from columns: {clean_columns}")

        df_iso = spark.read.option("header", True).csv(VALIDATE_FILE)
        logger.info(f"Loaded ISO country validation file: {VALIDATE_FILE}")

        df_cleaned = clean_production_countries_column(df_iso, df_step10)
        logger.info("Step 11: Cleaned production countries column")

        df_cleaned.show(5)
        logger.info("Displaying 5 rows of final cleaned data")

        # Show final schema and count
        logger.info("Final schema of cleaned dataset:")
        df_cleaned.printSchema()

        cleaned_count = df_cleaned.count()
        logger.info(f"Total cleaned records: {cleaned_count}")

        # Save to Local Storage
        df_cleaned.coalesce(1).write \
            .option("header", True) \
            .option("quoteAll", True) \
            .mode("overwrite") \
            .csv(OUTPUT_PATH)
        
        logger.info(f"Final cleaned data saved to: {OUTPUT_PATH}")

    except Exception as e:
        logger.error(f"Error occurred during Spark cleansing job: {e}", exc_info=True)
    finally:
        spark.stop()
        logger.info("Spark Session stopped")
