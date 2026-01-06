import os
import io
import yfinance as yf
import pandas as pd
from dotenv import load_dotenv
from google.cloud import storage


load_dotenv()
BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
PROJECT_ID = os.getenv("GCP_PROJECT_ID")

if not BUCKET_NAME:
    raise ValueError("GCS_BUCKET_NAME not set in .env")


storage_client = storage.Client(project=PROJECT_ID)
bucket = storage_client.bucket(BUCKET_NAME)


TICKERS = ["AAPL", "MSFT", "GOOG", "AMZN", "SPY"]

START_DATE = "2025-01-01"
END_DATE = "2025-12-31"

for ticker in TICKERS:
    print(f"Downloading {ticker}...")
    df = yf.download(ticker, start=START_DATE, end=END_DATE)

    if df.empty:
        print(f"No data for {ticker}, skipping.")
        continue

    df.reset_index(inplace=True)
    df["Ticker"] = ticker  

    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)

    blob = bucket.blob(f"raw/market/{ticker}.csv")
    blob.upload_from_string(csv_buffer.getvalue(), content_type="text/csv")
    print(f"Uploaded {ticker} data to GCS at raw/market/{ticker}.csv")

print("All tickers processed successfully!")
