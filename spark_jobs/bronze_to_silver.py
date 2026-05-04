import argparse
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, trim, regexp_replace, to_date, when

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main(domain, year, bronze_bucket, silver_bucket):
    spark = SparkSession.builder \
        .appName(f"THEMIS_Bronze_to_Silver_{domain}_{year}") \
        .getOrCreate()

    # Read from the domain and year partition logic used by ingest_data.py
    input_path = f"s3://{bronze_bucket}/tse/{domain}/ano={year}/"
    output_path = f"s3://{silver_bucket}/tse/{domain}/silver/ano={year}/"

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

    # --- DATA QUALITY ASSERTS (FAIL-FAST) ---
    logger.info("Executing Data Quality Checks (Fail-Fast)...")
    
    # 1. Volume Check
    row_count = df.count()
    if row_count == 0:
        raise ValueError("Data Quality Failed: DataFrame is completely empty (0 rows).")
    logger.info(f"DQ Check 1 Passed: Volume is {row_count} rows.")
    
    # 2 & 3. Schema and Null Threshold Check
    join_keys = ['sq_candidato', 'nm_candidato']
    found_keys = [k for k in join_keys if k in df.columns]
    
    if not found_keys:
        logger.warning("No candidate identifier (sq_candidato or nm_candidato) found. This dataset won't be joined in the Wide Table.")
    else:
        # Check Null threshold (Max 5%) on the primary identifier
        primary_key = found_keys[0]
        null_count = df.filter(col(primary_key).isNull()).count()
        null_percentage = null_count / row_count
        
        if null_percentage > 0.05:
            raise ValueError(f"Data Quality Failed: Column '{primary_key}' has {null_percentage*100:.2f}% nulls (Threshold is 5%).")
        logger.info(f"DQ Check 2 Passed: Primary key '{primary_key}' null rate is {null_percentage*100:.2f}% (acceptable).")

    # 3. Write to Silver layer as Parquet with Snappy compression
    logger.info(f"Writing clean data to {output_path}")
    df.write.mode("overwrite") \
        .option("compression", "snappy") \
        .parquet(output_path)

    spark.stop()
    logger.info("Bronze to Silver completed.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--domain', required=True, help="Domain, e.g., receitas")
    parser.add_argument('--year', required=True, help="Election year, e.g., 2022")
    parser.add_argument('--bronze_bucket', required=True, help="Bronze Bucket Name")
    parser.add_argument('--silver_bucket', required=True, help="Silver Bucket Name")
    args = parser.parse_args()
    
    main(args.domain, args.year, args.bronze_bucket, args.silver_bucket)
