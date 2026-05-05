import os
import requests
import boto3
from botocore.exceptions import ClientError
import logging
import zipfile
import shutil
import argparse

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Config
BRONZE_BUCKET = os.environ.get("BRONZE_BUCKET", "themis-dev-datalake-bronze")
CHUNK_SIZE = 8192 * 4 # 32KB chunks for faster download

def upload_to_s3(file_path: str, bucket: str, object_name: str) -> bool:
    """Upload a file to an S3 bucket"""
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_path, bucket, object_name)
    except ClientError as e:
        logger.error(f"Error uploading to S3: {e}")
        return False
    return True

def s3_prefix_has_files(bucket: str, prefix: str) -> bool:
    """Check if any files exist under the given S3 prefix"""
    s3_client = boto3.client('s3')
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    return 'Contents' in response and len(response['Contents']) > 0

def download_file(url: str, local_path: str):
    """Download large files (like ZIPs) to local path chunk by chunk"""
    logger.info(f"Downloading data from {url} to {local_path} ...")
    # Added timeout (30s connect, 300s read per chunk) to avoid infinite hang
    with requests.get(url, stream=True, timeout=(30, 300)) as r:
        r.raise_for_status()
        total_downloaded = 0
        log_threshold = 50 * 1024 * 1024 # Log every 50MB
        next_log = log_threshold
        
        with open(local_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=CHUNK_SIZE): 
                if chunk:
                    f.write(chunk)
                    total_downloaded += len(chunk)
                    if total_downloaded >= next_log:
                        logger.info(f"Downloaded {total_downloaded / (1024*1024):.2f} MB...")
                        next_log += log_threshold
    logger.info(f"Download complete: {local_path} ({total_downloaded / (1024*1024):.2f} MB)")

def run_tse_ingestion(url: str, domain: str, year: str):
    """
    Main ingestion logic:
    1. Downloads the ZIP
    2. Searches for '*BRASIL*.csv' inside the ZIP
    3. Extracts only that file
    4. Uploads it to S3 following the structure: bronze/tse/{domain}/ano={year}/
    5. Cleans up /tmp storage
    """
    s3_prefix = f"tse/{domain}/ano={year}/"
    if s3_prefix_has_files(BRONZE_BUCKET, s3_prefix):
        logger.info(f"Data already exists in s3://{BRONZE_BUCKET}/{s3_prefix}. Skipping ingestion.")
        return

    temp_dir = f"/tmp/themis_ingestion_{domain}_{year}"
    
    # Prune dir if exists from previous crashed runs
    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)
    os.makedirs(temp_dir, exist_ok=True)
    
    local_zip_path = os.path.join(temp_dir, f"tse_data_{domain}_{year}.zip")
    
    try:
        # 1. Download File
        download_file(url, local_zip_path)
        
        # 2. Open Zip and extract ONLY the BRASIL file
        logger.info(f"Scanning ZIP file for BRASIL unified dataset...")
        extracted_file_path = None
        
        with zipfile.ZipFile(local_zip_path, 'r') as z:
            # Look for any .csv file containing 'BRASIL' in its name (consolidated data)
            for file_info in z.infolist():
                filename_upper = file_info.filename.upper()
                if filename_upper.endswith('.CSV') and 'BRASIL' in filename_upper:
                    
                    # For prestacao_contas, we specifically want the Expenses file to calculate ROI
                    if domain == 'prestacao_contas' and 'DESPESAS_CONTRATADAS' not in filename_upper:
                        continue
                        
                    logger.info(f"Found target unified file: {file_info.filename}. Extracting...")
                    z.extract(file_info, temp_dir)
                    extracted_file_path = os.path.join(temp_dir, file_info.filename)
                    break 
        
        if not extracted_file_path:
            logger.error(f"Could not find a unified BRASIL CSV inside the ZIP from {url}")
            raise FileNotFoundError(f"Missing BRASIL CSV in downloaded ZIP for {domain}")
            
        # 3. Upload to Bronze Layer
        file_basename = os.path.basename(extracted_file_path)
        s3_key = f"tse/{domain}/ano={year}/{file_basename}"
        
        logger.info(f"Uploading Extracted CSV to s3://{BRONZE_BUCKET}/{s3_key} ...")
        success = upload_to_s3(extracted_file_path, BRONZE_BUCKET, s3_key)
        
        if success:
            logger.info("Upload to S3 successful!")
        else:
            logger.error("Upload failed.")

    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise e
        
    finally:
        # 4. Clean up the disk space on the Airflow worker
        logger.info(f"Cleaning up local temp directory: {temp_dir}")
        shutil.rmtree(temp_dir, ignore_errors=True)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest TSE Data")
    parser.add_argument('--url', required=True, help="Direct URL to the TSE .zip file")
    parser.add_argument('--domain', required=True, help="Data domain, e.g., 'receitas', 'candidatos', 'bens'")
    parser.add_argument('--year', required=True, help="Election year, e.g., '2022'")
    
    args = parser.parse_args()
    
    run_tse_ingestion(args.url, args.domain, args.year)
