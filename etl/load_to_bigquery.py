import os
from dotenv import load_dotenv
from google.cloud import bigquery
from google.cloud import storage

load_dotenv()

BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET")
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

if not all([BUCKET_NAME, PROJECT_ID, BIGQUERY_DATASET, GOOGLE_APPLICATION_CREDENTIALS]):
    raise ValueError("Please set GCS_BUCKET_NAME, GCP_PROJECT_ID, BIGQUERY_DATASET, and GOOGLE_APPLICATION_CREDENTIALS in .env")


os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GOOGLE_APPLICATION_CREDENTIALS


storage_client = storage.Client(project=PROJECT_ID)
bucket = storage_client.bucket(BUCKET_NAME)
bq_client = bigquery.Client(project=PROJECT_ID)


processed_files = ["processed/P01_portfolio.csv", "processed/P02_portfolio.csv"]

for file_path in processed_files:
    portfolio_id = file_path.split("/")[1].split("_")[0]
    table_id = f"{PROJECT_ID}.{BIGQUERY_DATASET}.{portfolio_id}_portfolio"
    
    blob = bucket.blob(file_path)
    if not blob.exists():
        print(f"Warning: {file_path} not found in bucket")
        continue

    csv_bytes = blob.download_as_bytes()
    uri = f"gs://{BUCKET_NAME}/{file_path}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
    )

    load_job = bq_client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )
    load_job.result()
    print(f"Loaded {portfolio_id} to BigQuery table {table_id}")

print("All processed portfolios loaded to BigQuery")
