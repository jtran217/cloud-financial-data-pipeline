import yfinance as yf
import os

tickers = ["AAPL", "MSFT", "GOOG", "AMZN", "SPY"]

start_date = "2025-01-01"
end_date = "2025-12-31"

raw_data_dir = "data/raw/market"
os.makedirs(raw_data_dir, exist_ok=True)

for ticker in tickers:
    print(f"Downloading {ticker}...")
    data = yf.download(ticker, start=start_date, end=end_date)
    
    data.reset_index(inplace=True)
    
    file_path = os.path.join(raw_data_dir, f"{ticker}.csv")
    data.to_csv(file_path, index=False)
    print(f"Saved {ticker} to {file_path}")