import os
import json
import requests
import tempfile
import pandas as pd
import logging
from google.cloud import storage
from contextlib import contextmanager
import pyarrow as pa
from pathlib import Path

def generate_parquet_schema_from_amount(amount_of_ids):
    """
    Generates a schema with 2 timestamps and i floats(accroding to the amount_of_ids).
    """


    # 1. Define the fixed leading columns
    fields = [
        pa.field(f"val_0", pa.timestamp('ns')),
        pa.field(f"val_1", pa.timestamp('ns'))
    ]
    
    
    # 2. Append the dynamic float columns
    for i in range(amount_of_ids):
        # Generates val_0, val_1, etc.
        fields.append(pa.field(f"val_{i+2}", pa.float64()))
    
    return pa.schema(fields)


# Setup Logging
logging.basicConfig(level=logging.INFO)

# --- Method 1: The Converter (Handles Logic & Temp File Lifecycle) ---
@contextmanager
def stream_api_to_temp_parquet(api_url, payload, schema=None, chunk_size=10000):
    """
    Streams CSV from API, converts to Parquet locally.
    Yields the path to the temp file so other functions can use it.
    Automatically cleans up the file when done.
    """
    temp_file = tempfile.NamedTemporaryFile(suffix='.parquet', delete=False)
    temp_path = temp_file.name
    temp_file.close() # Close so Pandas can lock/open it freely

    try:
        logging.info("Starting API stream and conversion...")
        
        with requests.post(api_url, json=payload, stream=True) as r:
            r.raise_for_status()
            
            # Use the iterator to keep RAM usage low
            csv_stream = pd.read_csv(r.raw, chunksize=chunk_size, dtype=schema)
            
            for i, chunk in enumerate(csv_stream):
                # Append logic: overwrite if first chunk, append if subsequent
                mode = 'w' if i == 0 else 'a' # specific to fastparquet (or imply via append=True)
                append = False if i == 0 else True
                
                # Note: 'fastparquet' or 'pyarrow' engines handle this slightly differently
                # This assumes fastparquet as per your original code
                chunk.to_parquet(
                    temp_path, 
                    engine='fastparquet', 
                    index=False, 
                    append=append
                )
                if i % 5 == 0: logging.info(f"Processed chunk {i}...")

        logging.info(f"Conversion finished. File ready at {temp_path}")
        
        # --- HAND OFF CONTROL ---
        yield temp_path 
        # ------------------------

    except Exception as e:
        logging.error(f"Conversion failed: {e}")
        raise
    finally:
        # --- CLEANUP (Runs automatically, even if upload fails) ---
        if os.path.exists(temp_path):
            os.remove(temp_path)
            logging.info("Temporary file wiped from disk.")


# --- Method 2: The Uploader (Pure Logic, knows nothing about APIs/Conversion) ---
def upload_parquet_to_gcs(local_path, bucket_name, destination_blob, credentials_json):
    """
    Uploads a local file to GCS.
    """
    if not os.path.exists(local_path):
        raise FileNotFoundError("There is no file to upload!")
    bucket_name = bucket_name
    destination_blob = destination_blob

    logging.info(f"Starting upload to gs://{bucket_name}/{destination_blob}")

    current_script_dir = Path(__file__).resolve().parent
    project_root = current_script_dir.parent.parent
    credentials_json = project_root / credentials_json
    
    storage_client = storage.Client.from_service_account_json(credentials_json) # Assumes env vars are set
    try:
        bucket = storage_client.get_bucket(bucket_name)
        print(f"Bucket '{bucket_name}' found.")
    except:
        print(f"Bucket '{bucket_name}' not found. Creating it...")
        bucket = storage_client.create_bucket(bucket_name, location="US")
        print(f"Bucket '{bucket_name}' created.")

    
    # Optional: Check if bucket exists (omitted for brevity)
    
    blob = bucket.blob(destination_blob)
    blob.upload_from_filename(local_path)
    
    logging.info("Upload Success!")

# --- The "Main" Script (Clean and Readable) ---

def run_pipeline():    
    # This "with" block ensures the file is deleted as soon as we exit indentation
    with stream_api_to_temp_parquet(API_URL, {}) as temp_file_path:
        
        # Now we just call the uploader
        upload_parquet_to_gcs(temp_file_path, BUCKET, BLOB_NAME)

if __name__ == "__main__":
    run_pipeline()

    # Example Usage:
    # schema_6 = generate_parquet_schema_from_amount(6)
    # print(f"Schema for 6 ids:\n{schema_6}")

    # schema_4 = generate_parquet_schema_from_amount(4)
    # print(f"\nSchema for 4 ids:\n{schema_4}")