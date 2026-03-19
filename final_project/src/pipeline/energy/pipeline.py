import pandas as pd
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.config import GCP_BUCKET_NAME, GCP_CREDENTIALS
from src.utils.data_helper import date_to_timestamp_ms, parse_german_date
from src.utils.http_utils import create_energy_payload
from src.utils.gcp_utils import stream_chunks_to_parquet, upload_parquet_to_gcs, load_to_bigquery
from src.pipeline.energy.config import ENERGY_CSV_SETTING
from src.pipeline.energy.transform import transform_energy_chunk, energy_response_handler, get_energy_csv_stream

    
def pipeline(start_time, end_time=None, target_main_cat=None, target_sub_cat=None):
    
    
    # --- Step 1: Convert Dates to Timestamps (if needed) ---
    start_dt = parse_german_date(start_time)
    if end_time:
        end_dt = parse_german_date(end_time)
    else:
        end_dt = start_dt
    start_timestamp = date_to_timestamp_ms(start_dt)
    end_timestamp = date_to_timestamp_ms(end_dt + pd.Timedelta(days=1))  # API returns data in 24h chunks, so we add 1 day to the end time

    # --- Step 2: Create the Payload and url---
    payload, API_URL = create_energy_payload(start_timestamp, end_timestamp, target_main_cat, target_sub_cat)

    # --- Step 3: Stream API to Parquet and Upload to GCS --- 
    blob_name = f"energy/{target_main_cat}/{target_sub_cat}/{start_dt.strftime('%Y-%m-%d')}_to_{end_dt.strftime('%Y-%m-%d')}.parquet"
    
    energy_stream = get_energy_csv_stream(API_URL, 
                                          payload, 
                                          chunk_size=10000,  
                                          response_handler=energy_response_handler, 
                                          csv_kwargs=ENERGY_CSV_SETTING)
    
    for temp_path in stream_chunks_to_parquet(energy_stream, transform_func=transform_energy_chunk):    
        upload_parquet_to_gcs(temp_path, GCP_BUCKET_NAME, blob_name, GCP_CREDENTIALS)
    # --- load onto BigQuery ---
    gcs_uri = f"gs://{GCP_BUCKET_NAME}/{blob_name}"
    dataset = "energy_transition"
    table = f"{target_main_cat}_{target_sub_cat}"
    load_to_bigquery(gcs_uri, dataset, table, GCP_CREDENTIALS)
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Ingest Parquet data to GCS")
    parser.add_argument("--start_date", required=True, type=str, help="Start time in YYYY-MM-DD format")
    parser.add_argument("--end_date", required=False, type=str, help="End time in YYYY-MM-DD format (default: start_time + 1 day)")
    parser.add_argument("--target_main_cat", required=True, type=str, help="Main category of the data to ingest")
    parser.add_argument("--target_sub_cat", required=True, type=str, help="Sub category of the data to ingest")

    args = parser.parse_args()
    pipeline(args.start_time, args.end_time, args.target_main_cat, args.target_sub_cat)