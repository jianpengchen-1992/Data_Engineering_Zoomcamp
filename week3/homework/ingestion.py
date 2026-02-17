from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.cloud import storage
import requests

#config
BUCKET_NAME = "project-energy-transition-raw-data"
PARQUET_URL_PREFIX = "https://d37ci6vzurychx.cloudfront.net/trip-data/"




storage_client = storage.Client.from_service_account_json("credentials_gcp.json")

#step 1, try to get the bucket, or create it if it doesn't exist
def upload_to_gcs(year, month, colour):
try:
bucket = storage_client.get_bucket(BUCKET_NAME)
print(f"Bucket {BUCKET_NAME} exists.")
except NotFound:
print(f"Bucket {BUCKET_NAME} not found. Creating it...")
# Note: location="US" (or "EU", "ASIA") is important for performance/compliance
bucket = storage_client.create_bucket(BUCKET_NAME, location="US")
print(f"Bucket {BUCKET_NAME} created successfully.")

print(f"2. Uploading to Bucket {BUCKET_NAME} ...")
blob = bucket.blob(f"{colour}_{PARQUET_NAME_PREFIX}_{year}-{month:02d}.parquet")

#upload form parquet url
print(f"Starting stream from url")
with requests.get(PARQUET_URL_PREFIX + f"{colour}_tripdata_{year}-{month:02d}.parquet", stream = True) as response:
response.raise_for_status()
response.raw.decode_content= True
blob.upload_from_file(response.raw)
print(f"   {colour} Parquet {year}-{month:02d} Upload complete.")

# --- STEP 3: LOAD INTO BIGQUERY ---
def load_to_bigquery(year, month, colour="yellow"):
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
table_ref = dataset_ref.table(f"{colour}_{TABLE_NAME_PREFIX}")
job_config = bigquery.LoadJobConfig(
source_format=bigquery.SourceFormat.PARQUET,
write_disposition=bigquery.WriteDisposition.WRITE_APPEND, # Append to table if exists
)
uri = f"gs://{BUCKET_NAME}/{colour}_{PARQUET_NAME_PREFIX}_{year}-{month:02d}.parquet"
load_job = bq_client.load_table_from_uri(uri, table_ref, job_config=job_config)
print("   ⏳ Job running...")
load_job.result() # Waits for the job to complete

print(f"   ✅ Job finished! Loaded {load_job.output_rows} rows into {DATASET_NAME}.{colour}_{TABLE_NAME_PREFIX}")

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