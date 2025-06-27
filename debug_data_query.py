import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import time

# Parameters
symbol = 'AAPL'
lookback_days = 20
db_path = 'market_data.db'

# Get date range
from_date = (datetime.now() - timedelta(days=lookback_days)).strftime('%Y-%m-%d')
print(f"Looking for data from {from_date} onwards")

# Connect to the database
conn = sqlite3.connect(db_path)

# First check how much data we have individually in each table
market_query = """
SELECT COUNT(*) FROM market_data
WHERE symbol = ? AND timestamp >= ?
"""

ta_query = """
SELECT COUNT(*) FROM technical_analysis
WHERE symbol = ? AND timestamp >= ?
"""

market_count = pd.read_sql_query(market_query, conn, params=(symbol, from_date)).iloc[0, 0]
ta_count = pd.read_sql_query(ta_query, conn, params=(symbol, from_date)).iloc[0, 0]

print(f"Market data rows for {symbol} since {from_date}: {market_count}")
print(f"Technical analysis rows for {symbol} since {from_date}: {ta_count}")

# Test both problematic queries
query1 = """
SELECT md.timestamp, md.symbol, md.open, md.high, md.low, md.close, md.volume,
       ta.rsi, ta.macd_line as macd
FROM market_data md
LEFT JOIN technical_analysis ta ON md.timestamp = ta.timestamp AND md.symbol = ta.symbol
WHERE md.timestamp >= ? AND md.symbol = ?
ORDER BY md.timestamp ASC
"""

query2 = """
SELECT md.timestamp, md.open, md.high, md.low, md.close, md.volume,
       ta.rsi, ta.macd_line as macd
FROM market_data md
LEFT JOIN technical_analysis ta ON md.timestamp = ta.timestamp AND md.symbol = ta.symbol
WHERE md.symbol = ? AND md.timestamp >= ?
ORDER BY md.timestamp ASC
"""

df1 = pd.read_sql_query(query1, conn, params=(from_date, symbol))
df2 = pd.read_sql_query(query2, conn, params=(symbol, from_date))

print(f"\nQuery 1 result count: {len(df1)}")
print(f"Query 2 result count: {len(df2)}")

# Check for null values in key columns
print("\nNull values in Query 1:")
print(df1[['timestamp', 'close', 'rsi', 'macd']].isnull().sum())

print("\nNull values in Query 2:")
print(df2[['timestamp', 'close', 'rsi', 'macd']].isnull().sum())

# Show first few rows of results
print("\nFirst 5 rows of Query 1:")
print(df1.head())

print("\nFirst 5 rows of Query 2:")
print(df2.head())

conn.close()
