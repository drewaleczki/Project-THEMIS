import argparse
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, col, coalesce, lit, round

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main(year, silver_bucket, gold_bucket):
    spark = SparkSession.builder \
        .appName(f"THEMIS_Build_Gold_Analytics_{year}") \
        .getOrCreate()

    base_silver_path = f"s3://{silver_bucket}/tse/{{}}/silver/ano={year}/"
    output_path = f"s3://{gold_bucket}/tse/campaign_analytics/gold/ano={year}/"

    logger.info("Reading Silver Datasets...")

    # 1. Candidate Dimension
    try:
        df_cand = spark.read.parquet(base_silver_path.format("candidatos"))
        df_cand = df_cand.select(
            "sq_candidato", "nm_candidato", "sg_uf", "sg_partido", 
            "ds_cargo", "ds_cor_raca", "ds_genero"
        ).dropDuplicates(["sq_candidato"])
    except Exception as e:
        logger.error(f"Failed to load candidatos: {e}")
        spark.stop()
        return

    def aggregate_metrics(domain, prefix):
        """ Dynamically finds metric columns and aggregates them by candidate ID """
        try:
            df = spark.read.parquet(base_silver_path.format(domain))
            cols = [c for c in df.columns if c.startswith(prefix)]
            if not cols:
                return None
            agg_exprs = [sum(c).alias(f"total_{c}") for c in cols]
            
            # fallback for sq_candidato if only nm_candidato exists
            join_col = "sq_candidato" if "sq_candidato" in df.columns else "nm_candidato"
            return df.groupBy(join_col).agg(*agg_exprs).withColumnRenamed(join_col, "sq_candidato")
        except Exception as e:
            logger.warning(f"Could not aggregate {domain}: {e}")
            return None

    # Dynamic Aggregations of Fact Tables
    df_bens_agg = aggregate_metrics("bens_candidato", "vr_")
    df_contas_agg = aggregate_metrics("prestacao_contas", "vr_")
    df_votos_agg = aggregate_metrics("votacao_nominal", "qt_")

    logger.info("Executing Joins to build Wide Table...")

    # Left Join using Candidate Dimension as Spine
    wide_df = df_cand
    if df_bens_agg:
        wide_df = wide_df.join(df_bens_agg, on="sq_candidato", how="left")
    if df_contas_agg:
        wide_df = wide_df.join(df_contas_agg, on="sq_candidato", how="left")
    if df_votos_agg:
        wide_df = wide_df.join(df_votos_agg, on="sq_candidato", how="left")

    # Null Cleaning to Avoid Mathematical Operations with NULL
    for c in wide_df.columns:
        if c.startswith("total_"):
            wide_df = wide_df.withColumn(c, coalesce(col(c), lit(0.0)))

    # ROI Calculation (Cost per Vote)
    despesa_col = next((c for c in wide_df.columns if "despesa" in c and c.startswith("total_")), None)
    voto_col = next((c for c in wide_df.columns if "votos" in c and c.startswith("total_")), None)
    
    if despesa_col and voto_col:
        # If 0 votes, consider 1 to avoid division by zero
        wide_df = wide_df.withColumn(
            "custo_por_voto", 
            round(col(despesa_col) / coalesce(col(voto_col), lit(1.0)), 2)
        )

    logger.info(f"Writing Gold Analytics Wide Table to {output_path}")
    
    # Strategic Partitioning
    wide_df.write.mode("overwrite") \
        .partitionBy("sg_uf", "ds_cargo") \
        .option("compression", "snappy") \
        .parquet(output_path)

    spark.stop()
    logger.info("Gold Layer processing completed successfully.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--year', required=True)
    parser.add_argument('--silver_bucket', required=True)
    parser.add_argument('--gold_bucket', required=True)
    args = parser.parse_args()
    
    main(args.year, args.silver_bucket, args.gold_bucket)
