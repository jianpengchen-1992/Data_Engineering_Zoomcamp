import pandas as pd
import sys
import os
import logging

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.config import GCP_BUCKET_NAME, GCP_CREDENTIALS
from src.utils.data_helper import parse_gmt_date
from src.utils.gcp_utils import upload_parquet_to_gcs, load_to_bigquery, stream_chunks_to_parquet
from src.pipeline.weather.config import CITIES
from src.pipeline.weather.transform import create_payload, get_weather_df
# 
def pipeline(start_date, end_date=None, target_cat="historical"):
    # --- Step 1: Parse Dates ---
    start_date = parse_gmt_date(start_date).strftime('%Y-%m-%d')
    if end_date:
        end_date = parse_gmt_date(end_date).strftime('%Y-%m-%d')
    else:
        end_date = start_date
    for city in CITIES:
        city_name = city["name"]
        lat = city["lat"]
        lon = city["lon"]
        payload, API_URL = create_payload(lat, lon, start_date, end_date, target_cat)

        blob_name = f"weather/{target_cat}/{city_name}/{start_date}_to_{end_date}.parquet"

        df = get_weather_df(API_URL, payload)

        for temp_path in stream_chunks_to_parquet([df]):         
            if temp_path:
                    logging.info(f"Uploading {blob_name} to GCS...")
                    upload_parquet_to_gcs(temp_path, GCP_BUCKET_NAME, blob_name, GCP_CREDENTIALS)
            else:
                logging.error("No data found to upload.")

        # --- load onto BigQuery ---
        gcs_uri = f"gs://{GCP_BUCKET_NAME}/{blob_name}"
        dataset = "weather_data"
        #table = f"{target_cat}_{city_name}"
        table = f"{target_cat}"
        load_to_bigquery(gcs_uri, dataset, table, GCP_CREDENTIALS)

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Ingest Parquet data to GCS")
    parser.add_argument("--start_date", required=True, type=str, help="Start date in DD-MM-YYYY format")
    parser.add_argument("--end_date", required=False, type=str, help="End date in DD-MM-YYYY format (default: start_date)")
    parser.add_argument("--target_cat", required=True, type=str, help="Main category of the data to ingest")

    args = parser.parse_args()
    pipeline(args.start_date, args.end_date, args.target_cat)