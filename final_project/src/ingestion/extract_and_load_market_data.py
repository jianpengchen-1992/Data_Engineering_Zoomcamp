from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.cloud import storage
import requests
import pandas as pd
import io
import pyarrow as pa
import json
from src.ingestion.mapping import get_ids_from_path
#config
BUCKET_NAME = "project-energy-transition-raw-data"
PROJECT_ID = "avid-circle-484315-e5"
CSV_REQUESTS_URL = "https://www.smard.de/nip-download-manager/nip/download/market-data"



# storage_client = storage.Client.from_service_account_json("credentials_gcp.json")

# def generate_parquet_schema(df):
#     """
#     Generates a schema where the first two columns are timestamps 
#     and all others are floats.
#     """
#     # 1. Define the known timestamp fields
#     # Using 'timestamp[us]' for microsecond precision (standard for Parquet)
#     fields = [
#         (df.columns[0], pa.timestamp('eu')),
#         (df.columns[1], pa.timestamp('eu'))
#     ]
    
#     # 2. Dynamically add all other columns as floats
#     # We skip the first two columns we already handled
#     for col_name in df.columns[2:]:
#         fields.append((col_name, pa.float64()))

#     # 3. Create the PyArrow Schema
#     return pa.schema(fields)

# # Example Usage:
# # schema = generate_parquet_schema(my_dataframe)
# # my_dataframe.to_parquet(temp_path, schema=schema, engine='pyarrow')

def create_payload(start_time, end_time, target_main_cat, target_sub_cat, target_region):
    filename='../config/market_data/market_data_configuration.json'
    with open(filename, 'r', encoding='utf-8') as f:
        json_data = json.load(f)
    ids = get_ids_from_path(
        json_data=json_data,
        target_main_cat=target_main_cat,
        target_sub_cat=target_sub_cat,
        target_region=target_region
    )
    payload = {
    "request_form": [{
        "format": "CSV",
        "moduleIds": ids,
        "region": "DE-LU",
        "timestamp_from": start_time,
        "timestamp_to": end_time,
        "type": "discrete",
        "language": "de",
        "resolution": ""
    }]
}
    return payload

#step 1, try to get the bucket, or create it if it doesn't exist
def market_data_to_gcs_parquet(url, payload, BUCKET_NAME, target_blob_name, credentials_json, schema=None):
    """
    Robustly streams CSV from API, converts to Parquet locally (temp file), 
    and uploads to GCS without eating up RAM.
    """
    
    # 1. Create a temporary file that exists on the local disk
    # 'delete=False' ensures we can close it and reopen it for uploading later
    temp_file = tempfile.NamedTemporaryFile(suffix='.parquet', delete=False)
    temp_path = temp_file.name
    temp_file.close() # Close it so Pandas can open it safely

    try:
        print("Starting download and conversion...")
        
        # 2. Stream the API response (stream=True is CRITICAL)
        # This keeps the connection open without downloading the whole body at once
        with requests.post(url, json=payload, stream=True) as r:
            r.raise_for_status()
            
            # 3. Process in Chunks
            # We read 100k rows at a time. RAM usage never exceeds this chunk size.
            chunk_iterator = pd.read_csv(r.raw, chunksize=10000, dtype=schema)
            
            for i, chunk in enumerate(chunk_iterator):
                # If it's the first chunk, create the file. If not, append to it.
                if i == 0:
                    chunk.to_parquet(temp_path, engine='fastparquet', index=False)
                else:
                    chunk.to_parquet(temp_path, engine='fastparquet', index=False, append=True)
                
                print(f"Processed chunk {i+1}...")

        print("Conversion complete. Uploading to GCP...")

        # 4. Upload the completed Parquet file to GCP
        storage_client = storage.Client.from_service_account_json(credentials_json)
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob(target_blob_name)
        
        # This uploads from the disk, not RAM
        blob.upload_from_filename(temp_path)
        print(f"Successfully uploaded to gs://{BUCKET_NAME}/{target_blob_name}")

    except Exception as e:
        print(f"An error occurred: {e}")
        raise # Re-raise the error so you know it failed

    finally:
        # 5. ALWAYS clean up the temp file
        # This runs whether the script succeeds or fails
        if os.path.exists(temp_path):
            os.remove(temp_path)
            print("Temporary file cleaned up.")



def main(year, month, colour="yellow"):
    # for m in range(1, month + 1):
    #     upload_to_gcs(year, m, colour)
    
    for m in range(1, month + 1):
        load_to_bigquery(year, m, colour)

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Ingest Parquet data to GCS and load into BigQuery")
    parser.add_argument("--year", required=True, type=int, help="Year of the data to ingest")
    parser.add_argument("--month", required=True, type=int, help="Number of months to ingest (1 to N)")
    parser.add_argument("--colour", required=True, type=str, help="Colour of the taxi data (yellow or green)")

    args = parser.parse_args()
    main(args.year, args.month, args.colour)