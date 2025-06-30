
import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import split, explode, trim, col, monotonically_increasing_id

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def create_dim_table_from_column(
    df: DataFrame,
    column_name: str,
    delimiter: str = ",",
    id_column: str = "id",
    value_column: str = "value"
) -> DataFrame:
    """
    Create a dimension table with distinct values from a delimited string column,
    and assign a unique ID using monotonically_increasing_id.
    """
    try:
        logger.info(f"Creating dimension table from column: {column_name}")

        result_df = (
            df.select(explode(split(col(column_name), delimiter + r"\s*")).alias(value_column))
              .select(trim(col(value_column)).alias(value_column))
              .distinct()
              .withColumn(id_column, monotonically_increasing_id())
              .select(id_column, value_column)
        )

        logger.info(f"Successfully created dimension table with {result_df.count()} records.")
        return result_df

    except Exception as e:
        logger.error(f"Error in create_dim_table_from_column: {e}", exc_info=True)
        raise


def create_bridge_table(
    fact_df: DataFrame,
    dim_df: DataFrame,
    fact_id_col: str,
    fact_list_col: str,
    dim_value_col: str,
    dim_id_col: str,
    delimiter: str = ", ",
    bridge_id_col: str = "bridge_id"
) -> DataFrame:
    """
    Create a bridge (many-to-many) table between a fact table and a dimension table.
    """
    try:
        logger.info(f"Creating bridge table between fact: {fact_list_col} and dimension: {dim_value_col}")

        exploded_df = (
            fact_df.select(col(fact_id_col), col(fact_list_col))
                   .withColumn(fact_list_col, split(col(fact_list_col), delimiter))
                   .withColumn(fact_list_col, explode(col(fact_list_col)))
                   .select(col(fact_id_col), trim(col(fact_list_col)).alias(fact_list_col))
        )

        joined_df = (
            exploded_df.join(
                dim_df,
                exploded_df[fact_list_col] == dim_df[dim_value_col],
                "inner"
            )
        )

        bridge_df = (
            joined_df.select(col(fact_id_col), col(dim_id_col))
                     .withColumn(bridge_id_col, monotonically_increasing_id())
                     .select(bridge_id_col, fact_id_col, dim_id_col)
        )

        logger.info(f"Successfully created bridge table with {bridge_df.count()} records.")
        return bridge_df

    except Exception as e:
        logger.error(f"Error in create_bridge_table: {e}", exc_info=True)
        raise


def create_dimension_tables_generator(df, dim_specs):
    """
    Generator that yields dimension tables one at a time to control memory usage.
    
    Args:
        df: Source DataFrame
        dim_specs: List of tuples (column_name, id_col, value_col)
    
    Yields:
        tuple: (table_name, dimension_dataframe)
    """
    for col_name, id_col, value_col in dim_specs:
        try:
            logger.info(f"Creating dimension table for {col_name}")
            dim_table = create_dim_table_from_column(
                df, col_name, id_column=id_col, value_column=value_col
            )
            logger.info(f"Successfully created dimension table for {col_name}")
            yield col_name, dim_table
        except Exception as e:
            logger.error(f"Error creating dimension table for {col_name}: {str(e)}")
            raise


def create_bridge_tables_generator(df, dim_tables, dim_specs):
    """
    Generator that yields bridge tables one at a time.
    
    Args:
        df: Source DataFrame
        dim_tables: Dictionary of dimension tables
        dim_specs: List of tuples (column_name, id_col, value_col)
    
    Yields:
        tuple: (table_name, bridge_dataframe)
    """
    for col_name, id_col, value_col in dim_specs:
        try:
            logger.info(f"Creating bridge table for {col_name}")
            bridge_df = create_bridge_table(
                fact_df=df,
                dim_df=dim_tables[col_name],
                fact_id_col="id",
                fact_list_col=col_name,
                dim_value_col=value_col,
                dim_id_col=id_col,
                bridge_id_col="bridge_id"
            ).withColumnRenamed("id", "movie_id")
            
            logger.info(f"Successfully created bridge table for {col_name}")
            yield col_name, bridge_df
        except Exception as e:
            logger.error(f"Error creating bridge table for {col_name}: {str(e)}")
            raise


def process_tables_incrementally(df):
    """
    Main generator function that processes all tables incrementally.
    
    Args:
        df: Source DataFrame
    
    Yields:
        tuple: (table_type, table_name, dataframe)
    """
    try:
        # === Dimension Tables ===
        logger.info("Creating main movie dimension table")
        dim_movie_df = df.select("id","title" ,"status", "release_date", "adult", "original_language", "overview") \
                         .withColumnRenamed("id", "movie_id")
        yield "dim", "movie", dim_movie_df
        
        # === Fact Tables ===
        logger.info("Creating fact table")
        fact_movie_df = df.select("id", "vote_average", "popularity", "vote_count", "budget", "revenue", "runtime") \
                          .withColumnRenamed("id", "movie_id")
        yield "fact", "movie", fact_movie_df
        
        # === Define dimension specifications ===
        dim_specs = [
            ("genres", "genre_id", "genre_name"),
            ("keywords", "keyword_id", "keyword_name"),
            ("production_companies", "company_id", "company_name"),
            ("production_countries", "country_id", "country_name"),
            ("spoken_languages", "language_id", "language_name")
        ]
        
        # === Create dimension tables incrementally ===
        dim_tables = {}
        for col_name, dim_table in create_dimension_tables_generator(df, dim_specs):
            dim_tables[col_name] = dim_table
            yield "dim", col_name, dim_table
        
        # === Create bridge tables incrementally ===
        for col_name, bridge_table in create_bridge_tables_generator(df, dim_tables, dim_specs):
            yield "bridge", col_name, bridge_table
            
    except Exception as e:
        logger.error(f"Error in process_tables_incrementally: {str(e)}")
        raise