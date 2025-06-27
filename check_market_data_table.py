"""
Check the structure of the market_data table to ensure compatibility with batch collector
"""

import sqlite3
import pandas as pd

DB_PATH = 'market_data.db'

def check_market_data_table():
    """Check the structure of the market_data table."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Get table info
    cursor.execute("PRAGMA table_info(market_data)")
    columns = cursor.fetchall()
    
    print("Market Data Table Structure:")
    for col in columns:
        print(f"{col[0]}: {col[1]} ({col[2]})")
    
    # Get row count
    cursor.execute("SELECT COUNT(*) FROM market_data")
    row_count = cursor.fetchone()[0]
    print(f"\nTotal rows: {row_count}")
    
    # Get sample data
    cursor.execute("SELECT * FROM market_data LIMIT 5")
    rows = cursor.fetchall()
    
    print("\nSample data:")
    for row in rows:
        print(row)
    
    # Get symbol count
    cursor.execute("SELECT COUNT(DISTINCT symbol) FROM market_data")
    symbol_count = cursor.fetchone()[0]
    print(f"\nDistinct symbols: {symbol_count}")
    
    # Get recent data for specific symbols to test compatibility
    symbols = ['AAPL', '^GDAXI', 'MSFT']
    for symbol in symbols:
        cursor.execute(
            "SELECT timestamp, symbol, open, high, low, close, volume FROM market_data WHERE symbol = ? ORDER BY timestamp DESC LIMIT 1", 
            (symbol,)
        )
        row = cursor.fetchone()
        if row:
            print(f"\nMost recent data for {symbol}: {row}")
        else:
            print(f"\nNo data found for {symbol}")
    
    conn.close()

def check_technical_analysis():
    """Check relation between market_data and technical_analysis."""
    conn = sqlite3.connect(DB_PATH)
    
    # Get latest data with technical analysis
    query = """
    SELECT m.timestamp, m.symbol, m.close, t.rsi, t.macd_line, t.signal_line, t.overall_signal
    FROM market_data m
    JOIN technical_analysis t ON m.symbol = t.symbol AND m.timestamp = t.timestamp
    ORDER BY m.timestamp DESC
    LIMIT 5
    """
    
    try:
        df = pd.read_sql_query(query, conn)
        print("\nJoined market data and technical analysis:")
        print(df)
    except Exception as e:
        print(f"Error joining tables: {e}")
    
    conn.close()

if __name__ == "__main__":
    check_market_data_table()
    check_technical_analysis()
