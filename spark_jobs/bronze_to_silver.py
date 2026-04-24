import argparse
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, trim, regexp_replace, to_date, when

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main(date_partition, bronze_bucket, silver_bucket):
    spark = SparkSession.builder \
        .appName("THEMIS_Bronze_to_Silver") \
        .getOrCreate()

    # E.g. s3://themis-dev-datalake-bronze/tse_campaigns/raw/ingestion_date=2026-04-23/
    input_path = f"s3://{bronze_bucket}/tse_campaigns/raw/ingestion_date={date_partition}/"
    output_path = f"s3://{silver_bucket}/tse_campaigns/clean/ingestion_date={date_partition}/"

    logger.info(f"Reading from {input_path}")
    
    # In a real TSE format, encoding is typically 'latin1' or 'ISO-8859-1'.
    df = spark.read.option("header", "true") \
        .option("encoding", "latin1") \
        .option("sep", ";") \
        .csv(input_path)

    # 1. Clean Column Names
    # Remove spaces and normalize to lower case snake_case
    clean_cols = [c.strip().lower().replace(" ", "_") for c in df.columns]
    df = df.toDF(*clean_cols)

    # 2. Schema normalization and basic cleaning
    # Dynamically cast any value column (starting with 'vr_') from string "123,45" to double
    for col_name in df.columns:
        # Step A: Convert #NULO and #NE strings to real Nulls across all columns
        df = df.withColumn(
            col_name,
            when(col(col_name).isin(["#NULO", "#NULO#", "#NE"]), None).otherwise(col(col_name))
        )
        
        # Step B: specifically for numeric vr_ metrics
        if col_name.startswith("vr_"):
            df = df.withColumn(col_name, regexp_replace(col(col_name), ",", ".").cast("double"))
            # Step C: -1 and -3 in numeric columns represent Null based on TSE README constraints
            df = df.withColumn(
                col_name,
                when(col(col_name).isin([-1.0, -3.0]), None).otherwise(col(col_name))
            )
    
    # Generic string cleansing for generic identifiers
    if 'nm_candidato' in df.columns:
        df = df.withColumn("nm_candidato", trim(col("nm_candidato")))

    if 'dt_receita' in df.columns: # Specific to receipts
        df = df.withColumn("dt_receita", to_date(col("dt_receita"), "dd/MM/yyyy"))

    # 3. Write to Silver layer as Parquet with Snappy compression
    logger.info(f"Writing clean data to {output_path}")
    df.write.mode("overwrite") \
        .option("compression", "snappy") \
        .parquet(output_path)

    spark.stop()
    logger.info("Bronze to Silver completed.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', required=True, help="Ingestion date partition (YYYY-MM-DD)")
    parser.add_argument('--bronze_bucket', required=True, help="Bronze Bucket Name")
    parser.add_argument('--silver_bucket', required=True, help="Silver Bucket Name")
    args = parser.parse_args()
    
    main(args.date, args.bronze_bucket, args.silver_bucket)
