from pyspark.sql import functions as f
from pyspark.sql.types import BooleanType, DoubleType, IntegerType
from pyspark.sql.functions import split, expr, trim, lower, explode, col, concat_ws, collect_list, sum 

READ_DIR_PATH = "/opt/bitnami/spark/resources/dataset/"
FLIE_NAME = "TMDB_movie_dataset_v11.csv"
ISOFILE_NAME = "iso_countries_cleaned.csv"
VALIDATE_FILE = READ_DIR_PATH+ISOFILE_NAME
read_file = READ_DIR_PATH + FLIE_NAME

def clean_genres_column(genres_lst, df):
    
    # Step 1: Convert genres string to array (split by comma and optional space)
    df_split = df.withColumn("genre_array", split("genres", ",\\s*"))
    
    # Step 2: Create a SQL-style array string for use in expr
    genres_array_str = "array(" + ", ".join([f"'{g}'" for g in genres_lst]) + ")"
    
    # Step 3: Filter rows where all genres are in the valid list
    df_valid = df_split.filter(expr(f"size(array_except(genre_array, {genres_array_str})) = 0"))
    
    # Step 4: Drop helper column if needed
    df_cleaned = df_valid.drop("genre_array")

    return df_cleaned
    

def clean_production_countries_column(valid_countries_df, df):
    # Step 1: Split into array
    df_split = df.withColumn("country_array", split(col("production_countries"), ",\\s*"))
    
    # Step 2: Explode into individual rows
    df_exploded = df_split.withColumn("country_raw", explode(col("country_array")))
    
    # Step 3: Normalize country values
    df_normalized = df_exploded.withColumn("country_norm", lower(trim(col("country_raw"))))
    
    # Step 4: Join with ISO list
    df_valid = df_normalized.join(
        valid_countries_df,
        df_normalized["country_norm"] == valid_countries_df["country_name"],
        how="inner"
    )
    
    # Step 5: Rebuild valid country list per movie
    df_grouped = df_valid.groupBy("id").agg(
        concat_ws(", ", collect_list("country_name")).alias("cleaned_production_countries")
    )
    
    # Step 6: Join with original dataset (to keep other columns)
    df_final = df.join(df_grouped, on="id", how="inner") \
                        .drop("production_countries") \
                        .withColumnRenamed("cleaned_production_countries", "production_countries")
    return df_final