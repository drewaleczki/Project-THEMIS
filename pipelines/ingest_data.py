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

def download_file(url: str, local_path: str):
    """Download large files (like ZIPs) to local path chunk by chunk"""
    logger.info(f"Downloading data from {url} to {local_path} ...")
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(local_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=CHUNK_SIZE): 
                if chunk:
                    f.write(chunk)
    logger.info(f"Download complete: {local_path}")

def run_tse_ingestion(url: str, domain: str, year: str):
    """
    Main ingestion logic:
    1. Downloads the ZIP
    2. Searches for '*BRASIL*.csv' inside the ZIP
    3. Extracts only that file
    4. Uploads it to S3 following the structure: bronze/tse/{domain}/ano={year}/
    5. Cleans up /tmp storage
    """
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
            # Look for any .csv file containing 'BRASIL' or 'BR' in its name
            for file_info in z.infolist():
                filename_upper = file_info.filename.upper()
                if filename_upper.endswith('.CSV') and ('BRASIL' in filename_upper or 'BR.CSV' in filename_upper):
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
