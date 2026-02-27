import pandas as pd
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.config import GCP_BUCKET_NAME, GCP_CREDENTIALS
from src.utils.data_helper import date_to_timestamp_ms, parse_german_date
from src.utils.http_utils import create_payload
from src.utils.gcp_utils import stream_api_to_temp_parquet, upload_parquet_to_gcs
from src.pipeline.energy.config import ENERGY_CSV_SETTING
from src.pipeline.energy.transform import transform_energy_chunk

    
def pipeline(start_time, end_time=None, target_main_cat=None, target_sub_cat=None):
    
    
    # --- Step 1: Convert Dates to Timestamps (if needed) ---
    start_dt = parse_german_date(start_time)
    if end_time:
        end_dt = parse_german_date(end_time)
        end_dt = end_dt + pd.Timedelta(days=1)
    else:
        end_dt = start_dt + pd.Timedelta(days=1)  # Default to 1 day later
    start_timestamp = date_to_timestamp_ms(start_dt)
    end_timestamp = date_to_timestamp_ms(end_dt)

    # --- Step 2: Create the Payload and url---
    payload, API_URL = create_payload(start_timestamp, end_timestamp, target_main_cat, target_sub_cat)

    # --- Step 3: Stream API to Parquet and Upload to GCS ---
    with stream_api_to_temp_parquet(API_URL, payload, ENERGY_CSV_SETTING, transform_energy_chunk, 10000) as temp_file_path:
        upload_parquet_to_gcs(temp_file_path, GCP_BUCKET_NAME, f"{target_main_cat}/{target_sub_cat}/{start_dt.strftime('%Y-%m-%d')}_to_{end_dt.strftime('%Y-%m-%d')}.parquet", GCP_CREDENTIALS)

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Ingest Parquet data to GCS")
    parser.add_argument("--start_time", required=True, type=str, help="Start time in DD/MM/YYYY format")
    parser.add_argument("--end_time", required=False, type=str, help="End time in DD/MM/YYYY format (default: start_time + 1 day)")
    parser.add_argument("--target_main_cat", required=True, type=str, help="Main category of the data to ingest")
    parser.add_argument("--target_sub_cat", required=True, type=str, help="Sub category of the data to ingest")

    args = parser.parse_args()
    pipeline(args.start_time, args.end_time, args.target_main_cat, args.target_sub_cat)