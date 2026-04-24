import argparse
import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main(date_partition, silver_bucket):
    spark = SparkSession.builder \
        .appName("THEMIS_Data_Quality_Check") \
        .getOrCreate()

    silver_path = f"s3://{silver_bucket}/tse_campaigns/clean/ingestion_date={date_partition}/"
    logger.info(f"Running Data Quality checks on {silver_path}")

    try:
        df = spark.read.parquet(silver_path)
    except Exception as e:
        logger.error(f"Failed to read path: {silver_path}. Error: {e}")
        sys.exit(1)

    errors = 0

    # Rule 1: Any value column 'vr_*' must not be negative
    value_columns = [c for c in df.columns if c.startswith('vr_')]
    for v_col in value_columns:
        negative_values = df.filter(col(v_col) < 0).count()
        if negative_values > 0:
            logger.error(f"Data Quality Error: Found {negative_values} records with negative {v_col}")
            errors += 1

    # Rule 2: Core identifiers must not be null (if they exist in the domain)
    # Using 'nm_candidato' (Notice: 'nr_cpf_candidato' is excluded as it is masked post-2019)
    core_identifiers = ['nm_candidato']
    for identifier in core_identifiers:
        if identifier in df.columns:
            null_count = df.filter(col(identifier).isNull()).count()
            if null_count > 0:
                logger.error(f"Data Quality Error: Found {null_count} nulls in {identifier}")
                errors += 1

    if errors > 0:
        logger.error("Data Quality validations failed. Aborting pipeline.")
        sys.exit(1)
    else:
        logger.info("All Data Quality checks passed successfully.")

    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', required=True)
    parser.add_argument('--silver_bucket', required=True)
    args = parser.parse_args()
    
    main(args.date, args.silver_bucket)
