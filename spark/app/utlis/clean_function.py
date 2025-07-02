import logging
from pyspark.sql import functions as f
from pyspark.sql.types import BooleanType, DoubleType, IntegerType
from pyspark.sql.functions import (
    split,
    expr,
    trim,
    lower,
    explode,
    col,
    concat_ws,
    collect_list,
    sum,
)

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

READ_DIR_PATH = "/opt/bitnami/spark/resources/dataset/"
FLIE_NAME = "TMDB_movie_dataset_v11.csv"
ISOFILE_NAME = "iso_countries_cleaned.csv"
VALIDATE_FILE = READ_DIR_PATH + ISOFILE_NAME
READ_FILE = READ_DIR_PATH + FLIE_NAME


def clean_genres_column(genres_lst, df):
    try:
        logger.info("Starting clean_genres_column")

        # Step 1: Convert genres string to array (split by comma and optional space)
        df_split = df.withColumn("genre_array", split("genres", ",\\s*"))
        logger.debug("Split genres column into array")

        # Step 2: Create a SQL-style array string for use in expr
        genres_array_str = "array(" + ", ".join([f"'{g}'" for g in genres_lst]) + ")"
        logger.debug(f"Valid genre array: {genres_array_str}")

        # Step 3: Filter rows where all genres are in the valid list
        df_valid = df_split.filter(
            expr(f"size(array_except(genre_array, {genres_array_str})) = 0")
        )
        logger.info("Filtered valid genres")

        # Step 4: Drop helper column if needed
        df_cleaned = df_valid.drop("genre_array")
        logger.info("Completed clean_genres_column")
        return df_cleaned

    except Exception as e:
        logger.error(f"Error in clean_genres_column: {e}", exc_info=True)
        raise


def clean_production_countries_column(valid_countries_df, df):
    try:
        logger.info("Starting clean_production_countries_column")

        # Step 1: Split into array
        df_split = df.withColumn(
            "country_array", split(col("production_countries"), ",\\s*")
        )
        logger.debug("Split countries column into array")

        # Step 2: Explode into individual rows
        df_exploded = df_split.withColumn("country_raw", explode(col("country_array")))
        logger.debug("Exploded country array")

        # Step 3: Normalize country values
        df_normalized = df_exploded.withColumn(
            "country_norm", lower(trim(col("country_raw")))
        )
        logger.debug("Normalized country values")

        # Step 4: Join with ISO list
        df_valid = df_normalized.join(
            valid_countries_df,
            df_normalized["country_norm"] == valid_countries_df["country_name"],
            how="inner",
        )
        logger.debug("Joined with valid ISO country list")

        # Step 5: Rebuild valid country list per movie
        df_grouped = df_valid.groupBy("id").agg(
            concat_ws(", ", collect_list("country_name")).alias(
                "cleaned_production_countries"
            )
        )
        logger.debug("Grouped valid countries by movie")

        # Step 6: Join with original dataset (to keep other columns)
        df_final = (
            df.join(df_grouped, on="id", how="inner")
            .drop("production_countries")
            .withColumnRenamed("cleaned_production_countries", "production_countries")
        )
        logger.info("Completed clean_production_countries_column")
        return df_final

    except Exception as e:
        logger.error(f"Error in clean_production_countries_column: {e}", exc_info=True)
        raise


def clean_double_quotes(df, columns=None):
    """
    Replaces all double quotes in selected string columns with an empty string.

    Parameters:
    - df: Spark DataFrame
    - columns: List of column names to clean. If None, it will not clean any columns.
    """
    try:
        logger.info("Starting clean_double_quotes")

        if columns is None:
            logger.warning(
                "No columns specified for double quote cleaning. Returning DataFrame unchanged."
            )
            return df

        for col_name in columns:
            if col_name in df.columns:
                df = df.withColumn(col_name, f.regexp_replace(f.col(col_name), '"', ""))
            else:
                logger.warning(f"Column '{col_name}' not found in DataFrame. Skipping.")

        logger.info("Completed clean_double_quotes")
        return df

    except Exception as e:
        logger.error(f"Error in clean_double_quotes: {e}", exc_info=True)
        raise
