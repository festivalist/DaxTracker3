"""
Check compatibility between batch collected data and signal generation/ML prediction
"""

import sqlite3
import pandas as pd
import os

DB_PATH = 'market_data.db'

def get_market_data_for_symbol(symbol, limit=5):
    """Retrieve market data for a specific symbol."""
    conn = sqlite3.connect(DB_PATH)
    
    query = """
    SELECT timestamp, symbol, open, high, low, close, volume
    FROM market_data
    WHERE symbol = ?
    ORDER BY timestamp DESC
    LIMIT ?
    """
    
    df = pd.read_sql_query(query, conn, params=(symbol, limit))
    conn.close()
    
    return df

def check_technical_analysis_compatibility(symbol):
    """Check if technical analysis can be run on the collected data."""
    conn = sqlite3.connect(DB_PATH)
    
    # Get data used for technical analysis
    query = """
    SELECT m.timestamp, m.symbol, m.close 
    FROM market_data m
    WHERE m.symbol = ?
    ORDER BY m.timestamp DESC
    LIMIT 100
    """
    
    df = pd.read_sql_query(query, conn, params=(symbol,))
    conn.close()
    
    if df.empty:
        print(f"No market data found for {symbol}")
        return False
    
    # Check for required data
    if df['close'].isnull().sum() > 0:
        print(f"Warning: {df['close'].isnull().sum()} null close values found for {symbol}")
    
    print(f"Technical analysis compatibility for {symbol}: OK - {len(df)} data points available")
    return len(df) >= 50  # Assuming we need at least 50 data points for reliable TA

def check_ml_prediction_compatibility(symbol):
    """Check if ML prediction can be run on the collected data."""
    # Check if model exists
    model_path = os.path.join('checkpoints', f'market_lstm_{symbol}.pth')
    model_exists = os.path.exists(model_path)
    
    conn = sqlite3.connect(DB_PATH)
    
    # Check if technical analysis data exists (required for prediction)
    query = """
    SELECT COUNT(*) 
    FROM technical_analysis
    WHERE symbol = ?
    """
    
    cursor = conn.cursor()
    cursor.execute(query, (symbol,))
    ta_count = cursor.fetchone()[0]
    
    # Check if we have ML predictions already
    query = """
    SELECT COUNT(*) 
    FROM ml_predictions
    WHERE symbol = ?
    """
    
    cursor.execute(query, (symbol,))
    pred_count = cursor.fetchone()[0]
    
    conn.close()
    
    print(f"ML prediction compatibility for {symbol}:")
    print(f"  - Model exists: {model_exists}")
    print(f"  - Technical analysis data points: {ta_count}")
    print(f"  - Existing predictions: {pred_count}")
    
    return model_exists and ta_count > 0

def check_signal_generation_compatibility(symbol):
    """Check if signal generation can be run on the collected data."""
    conn = sqlite3.connect(DB_PATH)
    
    # Check requirements for signal generation
    cursor = conn.cursor()
    
    # Check technical analysis
    cursor.execute(
        "SELECT COUNT(*) FROM technical_analysis WHERE symbol = ?", 
        (symbol,)
    )
    ta_count = cursor.fetchone()[0]
    
    # Check ML predictions
    cursor.execute(
        "SELECT COUNT(*) FROM ml_predictions WHERE symbol = ?",
        (symbol,)
    )
    ml_count = cursor.fetchone()[0]
    
    # Check existing signals
    cursor.execute(
        "SELECT COUNT(*) FROM trading_signals WHERE symbol = ?",
        (symbol,)
    )
    signals_count = cursor.fetchone()[0]
    
    conn.close()
    
    print(f"Signal generation compatibility for {symbol}:")
    print(f"  - Technical analysis data: {ta_count > 0}")
    print(f"  - ML prediction data: {ml_count > 0}")
    print(f"  - Existing signals: {signals_count}")
    
    return ta_count > 0

def main():
    """Main function to check compatibility."""
    print("Checking compatibility between batch collected data and existing pipeline...")
    
    # Symbols to check
    symbols = ['AAPL', '^GDAXI', 'MSFT']
    
    for symbol in symbols:
        print(f"\n=== Checking compatibility for {symbol} ===")
        
        # Get sample data
        market_data = get_market_data_for_symbol(symbol)
        print(f"Recent market data ({len(market_data)} rows):")
        print(market_data.head())
        
        # Check technical analysis
        check_technical_analysis_compatibility(symbol)
        
        # Check ML prediction
        check_ml_prediction_compatibility(symbol)
        
        # Check signal generation
        check_signal_generation_compatibility(symbol)

if __name__ == "__main__":
    main()
