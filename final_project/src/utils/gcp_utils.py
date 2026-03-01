import io
import os
import requests
import tempfile
import pandas as pd
import logging
from google.cloud import storage
from contextlib import contextmanager
from pathlib import Path
# Setup Logging
logging.basicConfig(level=logging.INFO)

# --- Method 1: The Converter (Handles Logic & Temp File Lifecycle) ---
@contextmanager
def stream_api_to_temp_parquet(
    api_url, 
    payload, 
    transform_func=None,
    csv_kwargs=None,
    response_handler=None,  # NEW: Callback to dynamically determine schema
    chunk_size=10000
):
    """
    Streams CSV from API, applies optional transformations, and converts to Parquet locally.
    Yields the path to the temp file so other functions can use it.
    """
    temp_file = tempfile.NamedTemporaryFile(suffix='.parquet', delete=False)
    temp_path = temp_file.name
    temp_file.close() 
    
    csv_kwargs = csv_kwargs or {}
    
    try:
        logging.info("Starting API stream and conversion...")
        
        with requests.post(api_url, json=payload, stream=True) as r:
            r.raise_for_status()
            r.raw.decode_content = True
            
            # --- DYNAMIC SCHEMA LOGIC ---
            if response_handler:
                # Let the custom callback inspect the response and return dynamic kwargs
                dynamic_kwargs = response_handler(r)
                csv_kwargs.update(dynamic_kwargs)
            # ----------------------------

            # Pass r.raw directly to Pandas to truly stream and save RAM!
            csv_stream = pd.read_csv(r.raw, chunksize=chunk_size, **csv_kwargs)
            
            chunks_processed = 0
            for i, chunk in enumerate(csv_stream): 
                
                if transform_func:
                    chunk = transform_func(chunk)
                
                append = False if i == 0 else True
                
                chunk.to_parquet(
                    temp_path, 
                    engine='fastparquet', 
                    index=False, 
                    append=append
                )
                
                chunks_processed += 1 
                if i % 5 == 0: 
                    logging.info(f"Processed chunk {i}...")
                
        if chunks_processed == 0:
            logging.warning("No chunks were processed! The DataFrame stream was empty.")
        else:
            logging.info(f"Conversion finished. {chunks_processed} chunks written to {temp_path}")
            
        yield temp_path 

    except Exception as e:
        logging.error(f"Conversion failed: {e}")
        raise
    finally:
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
        bucket = storage_client.create_bucket(bucket_name, location="europe-west3")
        print(f"Bucket '{bucket_name}' created.")

    
    # Optional: Check if bucket exists (omitted for brevity)
    
    blob = bucket.blob(destination_blob)
    blob.upload_from_filename(local_path)
    
    logging.info("Upload Success!")

def load_to_bigquery(gcs_uri, dataset_id, table_id, credentials_json):
    """
    Loads a Parquet file from GCS into BigQuery.
    Creates the dataset if it doesn't exist.
    Creates the table if it doesn't exist, appends if it does.
    """
    from google.cloud import bigquery

    logging.info(f"Starting BigQuery load from {gcs_uri} to {dataset_id}.{table_id}")

    # Resolve paths safely
    current_script_dir = Path(__file__).resolve().parent
    project_root = current_script_dir.parent.parent
    credentials_path = project_root / credentials_json
    
    # Initialize the client
    client = bigquery.Client.from_service_account_json(credentials_path)
    
    # --- 1. DATASET LOGIC: Create if missing ---
    dataset_ref = client.dataset(dataset_id)
    dataset = bigquery.Dataset(dataset_ref)
    
    # set location
    dataset.location = "EU" 
    
    # exists_ok=True prevents an error if the dataset is already there
    dataset = client.create_dataset(dataset, exists_ok=True) 
    logging.info(f"Dataset {dataset_id} is ready.")
    # -------------------------------------------

    table_ref = dataset_ref.table(table_id)

    # --- 2. TABLE LOGIC: Create or Append ---
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        
        # Append data to the table if it exists
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND, 
        
        # Create the table if it doesn't exist yet
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED 
    )
    # ----------------------------------------

    # Run the job
    load_job = client.load_table_from_uri(
        gcs_uri,
        table_ref,
        job_config=job_config
    )

    logging.info("Waiting for BigQuery load job to complete...")
    load_job.result()  # Waits for the job to complete

    logging.info(f"BigQuery load completed. Loaded {load_job.output_rows} rows into {dataset_id}.{table_id}")

def run_pipeline():    
    pass

if __name__ == "__main__":
    run_pipeline()

    # Example Usage:
    # schema_6 = generate_parquet_schema_from_amount(6)
    # print(f"Schema for 6 ids:\n{schema_6}")

    # schema_4 = generate_parquet_schema_from_amount(4)
    # print(f"\nSchema for 4 ids:\n{schema_4}")