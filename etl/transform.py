import os
import io
import pandas as pd
from dotenv import load_dotenv
from google.cloud import storage

load_dotenv()
BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
PROJECT_ID = os.getenv("GCP_PROJECT_ID")

if not BUCKET_NAME or not PROJECT_ID:
    raise ValueError("Please set GCS_BUCKET_NAME and GCP_PROJECT_ID in your .env")

storage_client = storage.Client(project=PROJECT_ID)
bucket = storage_client.bucket(BUCKET_NAME)

LOCAL_TRANSACTIONS_PATH = 'data/raw/internal/transactions.csv'
transactions = pd.read_csv(LOCAL_TRANSACTIONS_PATH, parse_dates=["transaction_date"])
transactions = transactions.rename(columns={'asset_id': 'Ticker', 'transaction_date': 'Date'})
print("Loaded internal transactions from local file")
market_files = ["raw/market/AAPL.csv", "raw/market/MSFT.csv", "raw/market/GOOG.csv", "raw/market/AMZN.csv", "raw/market/SPY.csv"]

market_data_list = []
for file_path in market_files:
    blob = bucket.blob(file_path)
    if not blob.exists():
        print(f"Warning: {file_path} not found in bucket")
        continue
    csv_bytes = blob.download_as_bytes()
    df = pd.read_csv(io.BytesIO(csv_bytes), parse_dates=["Date"])
    market_data_list.append(df)

market_data = pd.concat(market_data_list, ignore_index=True)
for col in ['Close', 'High', 'Low', 'Open', 'Volume']:
    market_data[col] = pd.to_numeric(market_data[col], errors='coerce')
print("Loaded market data from GCS")

transactions["signed_quantity"] = transactions.apply(
    lambda row: row["quantity"] if row["transaction_type"].upper() == "BUY" else -row["quantity"], axis=1
)

merged = pd.merge(
    transactions,
    market_data,
    on=["Date", "Ticker"],
    how="left"
)

merged.sort_values(["Ticker", "Date"], inplace=True)
merged["Close"] = merged.groupby("Ticker")["Close"].ffill()

merged["daily_value_contrib"] = merged["signed_quantity"] * merged["Close"]
portfolio_daily = merged.groupby(["portfolio_id", "Date"])["daily_value_contrib"].sum().reset_index()
portfolio_daily.rename(columns={"daily_value_contrib": "daily_value"}, inplace=True)

portfolio_daily["daily_return"] = portfolio_daily.groupby("portfolio_id")["daily_value"].pct_change()

for portfolio_id, df_portfolio in portfolio_daily.groupby("portfolio_id"):
    csv_buffer = io.StringIO()
    df_portfolio.to_csv(csv_buffer, index=False)
    blob = bucket.blob(f"processed/{portfolio_id}_portfolio.csv")
    blob.upload_from_string(csv_buffer.getvalue(), content_type="text/csv")
    print(f"Uploaded processed data for portfolio {portfolio_id} to processed/{portfolio_id}_portfolio.csv")

print("All portfolios processed and uploaded successfully!")
