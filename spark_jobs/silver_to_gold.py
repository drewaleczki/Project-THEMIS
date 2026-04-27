import argparse
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main(domain, year, silver_bucket, gold_bucket):
    spark = SparkSession.builder \
        .appName(f"THEMIS_Silver_to_Gold_{domain}_{year}") \
        .getOrCreate()

    silver_path = f"s3://{silver_bucket}/tse/{domain}/silver/ano={year}/"
    output_path = f"s3://{gold_bucket}/tse/{domain}/gold/ano={year}/"

    logger.info(f"Reading Silver Data from {silver_path}")
    df = spark.read.parquet(silver_path)

    # Aggregation: Find the correct value column based on the dataset (e.g. vr_bem_candidato)
    value_cols = [c for c in df.columns if c.startswith('vr_')]
    target_value_col = value_cols[0] if value_cols else None
    
    # Find candidate identifier
    cand_cols = [c for c in df.columns if c in ['nm_candidato', 'sq_candidato']]
    target_cand_col = cand_cols[0] if cand_cols else None

    if target_cand_col and target_value_col:
        agg_df = df.groupBy(target_cand_col) \
                   .agg(
                       sum(target_value_col).alias(f"total_{target_value_col}"),
                       count("*").alias("qtd_registros")
                   )
                   
        logger.info(f"Writing Gold Data to {output_path}")
        agg_df.write.mode("overwrite") \
            .option("compression", "snappy") \
            .parquet(output_path)
    else:
        logger.warning(f"Required columns for aggregation missing. Found value col: {target_value_col}. Skipping Gold processing.")

    spark.stop()
    logger.info("Silver to Gold completed.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--domain', required=True)
    parser.add_argument('--year', required=True)
    parser.add_argument('--silver_bucket', required=True)
    parser.add_argument('--gold_bucket', required=True)
    args = parser.parse_args()
    
    main(args.domain, args.year, args.silver_bucket, args.gold_bucket)
