import logging
from pyspark.sql import SparkSession
from utlis.transform_function import process_tables_incrementally

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

CLEAN_DATA_PATH = "/opt/shared/output/cleaned_data"
OUTPUT_DIR = "/opt/shared/output"


if __name__ == "__main__":
    
    spark = SparkSession.builder \
        .appName("Spark_Transform") \
        .master("spark://spark-master:7077") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()

    try:
        logger.info("Starting data transformation process")
        
        # Load data with caching for reuse
        df = spark.read.option("header", True).option("inferSchema", True).csv(CLEAN_DATA_PATH).cache()
        record_count = df.count()
        logger.info(f"Loaded data with {record_count:,} records")
        
        # Process tables using generator pattern and save immediately
        table_counts = {}
        
        for table_type, table_name, table_df in process_tables_incrementally(df):
            
            # Save immediately to disk to free memory
            output_path = f"{OUTPUT_DIR}/{table_type}_{table_name}"
            table_df.write.mode("overwrite").parquet(output_path)
            
            # Get count before the DataFrame is released from memory
            count = table_df.count()
            table_counts[f"{table_type}_{table_name}"] = count
            
            logger.info(f"Processed and saved {table_type}_{table_name} ({count:,} records) to {output_path}")
        
        logger.info("All tables processed and saved successfully")
        
        # Print table counts for verification
        logger.info("Table counts summary:")
        for table_name, count in table_counts.items():
            logger.info(f"  - {table_name}: {count:,} records")
        
    except Exception as e:
        logger.error(f"Fatal error in main process: {str(e)}")
        raise
        
    finally:
        # Always stop Spark session
        logger.info("Stopping Spark session")
        spark.stop()