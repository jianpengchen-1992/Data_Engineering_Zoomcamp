from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.cloud import storage
import requests

#config
BUCKET_NAME = "data-engineering-zoomcamp-jianpeng-20260122"
PARQUET_URL_PREFIX = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_"
PROJECT_ID = "avid-circle-484315-e5"
PARQUET_NAME_PREFIX = "yellow_taxi_data"
DATASET_NAME= "ny_taxi"
TABLE_NAME = "yellow_taxi_data"


storage_client = storage.Client(project=PROJECT_ID)

#step 1, try to get the bucket, or create it if it doesn't exist
def upload_to_gcs(year, month):
    try:
        bucket = storage_client.get_bucket(BUCKET_NAME)
        print(f"Bucket {BUCKET_NAME} exists.")
    except NotFound:
        print(f"Bucket {BUCKET_NAME} not found. Creating it...")
        # Note: location="US" (or "EU", "ASIA") is important for performance/compliance
        bucket = storage_client.create_bucket(BUCKET_NAME, location="US")
        print(f"Bucket {BUCKET_NAME} created successfully.")

    print(f"2. Uploading to Bucket {BUCKET_NAME} ...")
    blob = bucket.blob(f"{PARQUET_NAME_PREFIX}_{year}-{month:02d}.parquet")

    #upload form parquet url
    print(f"Starting stream from url")
    with requests.get(PARQUET_URL_PREFIX + f"{year}-{month:02d}.parquet", stream = True) as response:
        response.raise_for_status()
        response.raw.decode_content= True
        blob.upload_from_file(response.raw)
        print(f"   Parquet {year}-{month:02d} Upload complete.")

# --- STEP 3: LOAD INTO BIGQUERY ---
def load_to_bigquery(year, month):
    print("3. Instructing BigQuery to ingest...")
    bq_client = bigquery.Client(project=PROJECT_ID)

    # Create the Dataset if it doesn't exist
    dataset_ref = bq_client.dataset(DATASET_NAME)
    try:
        bq_client.get_dataset(dataset_ref)
        print(f"   Dataset {DATASET_NAME} already exists.")
    except Exception:
        bq_client.create_dataset(dataset_ref)
        print(f"   Created new dataset: {DATASET_NAME}")

    # Configure the Load Job
    table_ref = dataset_ref.table(TABLE_NAME)
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND, # Append to table if exists
    )
    uri = f"gs://{BUCKET_NAME}/{PARQUET_NAME_PREFIX}_{year}-{month:02d}.parquet"
    load_job = bq_client.load_table_from_uri(uri, table_ref, job_config=job_config)
    print("   ⏳ Job running...")
    load_job.result() # Waits for the job to complete

    print(f"   ✅ Job finished! Loaded {load_job.output_rows} rows into {DATASET_NAME}.{TABLE_NAME}")

def main(year, month):
    for m in range(1, month + 1):
        #upload_to_gcs(year, m)
        load_to_bigquery(year, m) 

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Ingest Parquet data to GCS and load into BigQuery")
    parser.add_argument("--year", required=True, type=int, help="Year of the data to ingest")
    parser.add_argument("--month", required=True, type=int, help="Number of months to ingest (1 to N)")

    args = parser.parse_args()
    main(args.year, args.month)