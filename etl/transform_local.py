import pandas as pd
import os

# Using mock internal transaction data
transactions = pd.read_csv('data/raw/internal/transactions.csv')
transactions['transaction_date'] = pd.to_datetime(transactions['transaction_date'])

# Pull from data lake
raw_data_dir = "data/raw/market"
tickers = ["AAPL", "MSFT", "GOOG", "AMZN", "SPY"]


dfs = []
for ticker in tickers:
    file_path = os.path.join(raw_data_dir, f"{ticker}.csv")
    df = pd.read_csv(file_path)
    df["Ticker"] = ticker  
    # Make date from string -> panda Date format
    df['Date'] = pd.to_datetime(df['Date'])
    df['Ticker'] = ticker
    dfs.append(df)

# Concat all data frame into one large one
market_data = pd.concat(dfs)
for col in ['Close', 'High', 'Low', 'Open', 'Volume']:
    market_data[col] = pd.to_numeric(market_data[col], errors='coerce')

# Rename mock data column to have columns we can join on
transactions = transactions.rename(columns={'asset_id': 'Ticker', 'transaction_date': 'Date'})
merged = pd.merge(transactions, market_data, on=['Date', 'Ticker'], how='left')
# Drop any row where NaN is present in close column
merged = merged.dropna(subset=['Close'])

merged['signed_quantity'] = merged.apply(lambda row: row['quantity'] if row['transaction_type']=='BUY' else -row['quantity'], axis=1)

# dollar value of each transaction, positive for buys and negative for sells.
merged['daily_value'] = merged['signed_quantity'] * merged['Close']

portfolio_daily = merged.groupby(['portfolio_id', 'Date'])['daily_value'].sum().reset_index()
portfolio_daily = portfolio_daily.sort_values(['portfolio_id', 'Date'])
portfolio_daily['daily_return'] = portfolio_daily.groupby('portfolio_id')['daily_value'].pct_change()


os.makedirs('data/processed', exist_ok=True)
portfolio_daily.to_csv('data/processed/portfolio_daily.csv', index=False)
