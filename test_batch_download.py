"""
Test script to evaluate yfinance's batch download capability for intraday data.
This script tests whether batch downloading works well with intraday data
and evaluates performance for a large list of symbols.
"""

import yfinance as yf
import pandas as pd
import time
import logging
import sqlite3
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('batch_download_test.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger('BatchDownloadTest')

# Sample of symbols to test with
SAMPLE_STOCKS = ['AAPL', 'MSFT', 'AMZN', 'GOOGL', 'NVDA']
SAMPLE_INDICES = ['^GDAXI', '^GSPC', '^DJI']

# Database connection
DB_PATH = 'market_data.db'

def connect_to_db():
    """Connect to the SQLite database."""
    try:
        conn = sqlite3.connect(DB_PATH)
        return conn
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        return None

def store_batch_data(data, conn):
    """Store batch downloaded data in the database."""
    if conn is None:
        logger.error("No database connection")
        return False
    
    cursor = conn.cursor()
    records_stored = 0
    
    try:
        # Process multi-level DataFrame (grouped by ticker)
        for symbol in data.columns.levels[0]:
            symbol_data = data[symbol]
            
            # Process each row
            for timestamp, row in symbol_data.iterrows():
                try:
                    cursor.execute('''
                    INSERT OR REPLACE INTO market_data 
                    (timestamp, symbol, open, high, low, close, volume)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                        symbol,
                        row.get('Open', None),
                        row.get('High', None),
                        row.get('Low', None),
                        row.get('Close', None),
                        row.get('Volume', None)
                    ))
                    records_stored += 1
                except Exception as e:
                    logger.warning(f"Error storing row for {symbol} at {timestamp}: {e}")
                    continue
        
        conn.commit()
        logger.info(f"Successfully stored {records_stored} records in database")
        return True
    except Exception as e:
        logger.error(f"Error storing batch data: {e}")
        conn.rollback()
        return False

def test_batch_download(symbols, interval='1m', period='1d'):
    """
    Test yfinance batch download with the given parameters.
    
    Args:
        symbols (list): List of symbols to download
        interval (str): Data interval (1m, 5m, 15m, 30m, 1h, 1d, etc)
        period (str): Period to download (1d, 5d, 1mo, 3mo, etc)
        
    Returns:
        pandas.DataFrame: The downloaded data
    """
    logger.info(f"Testing batch download for {len(symbols)} symbols with interval={interval}, period={period}")
    
    start_time = time.time()
    
    try:
        # Download data for all symbols in one call
        data = yf.download(
            tickers=symbols,
            period=period,
            interval=interval,
            group_by='ticker',
            auto_adjust=True,
            prepost=False,
            threads=True,
            progress=False
        )
        
        end_time = time.time()
        elapsed = end_time - start_time
        
        if data.empty:
            logger.warning(f"No data returned for any symbols")
            return None
        
        # Check which symbols returned data
        available_symbols = []
        missing_symbols = []
        
        # For multi-level DataFrame, first level contains symbols
        if isinstance(data.columns, pd.MultiIndex):
            # Count rows per symbol
            symbol_counts = {}
            for symbol in symbols:
                if symbol in data.columns.levels[0]:
                    symbol_data = data[symbol]
                    # Remove rows where all values are NaN
                    symbol_data = symbol_data.dropna(how='all')
                    rows = len(symbol_data)
                    symbol_counts[symbol] = rows
                    
                    if rows > 0:
                        available_symbols.append(symbol)
                    else:
                        missing_symbols.append(symbol)
                else:
                    missing_symbols.append(symbol)
                    symbol_counts[symbol] = 0
            
            logger.info(f"Download completed in {elapsed:.2f} seconds")
            logger.info(f"Available symbols: {len(available_symbols)}/{len(symbols)}")
            logger.info(f"Missing symbols: {len(missing_symbols)}/{len(symbols)}")
            
            for symbol, count in symbol_counts.items():
                if count > 0:
                    logger.info(f"{symbol}: {count} data points")
                else:
                    logger.warning(f"{symbol}: No data")
        else:
            # Single symbol result has different structure
            rows = len(data)
            if rows > 0:
                logger.info(f"Single symbol result with {rows} data points")
            else:
                logger.warning("No data returned")
        
        return data
    
    except Exception as e:
        end_time = time.time()
        elapsed = end_time - start_time
        logger.error(f"Error in batch download after {elapsed:.2f} seconds: {str(e)}")
        return None

def test_and_store(symbols, interval='1m', period='1d'):
    """Test download and store in database"""
    # Download data
    data = test_batch_download(symbols, interval, period)
    
    if data is None or data.empty:
        logger.warning("No data to store")
        return
    
    # Store in database
    conn = connect_to_db()
    if conn:
        store_batch_data(data, conn)
        conn.close()

def read_stock_symbols(csv_path='stocks.csv'):
    """Read stock symbols from CSV file"""
    try:
        df = pd.read_csv(csv_path, sep=';')
        if 'Symbol' in df.columns:
            symbols = df['Symbol'].tolist()
            return symbols
        else:
            logger.error("No Symbol column in CSV file")
            return []
    except Exception as e:
        logger.error(f"Error reading CSV file: {e}")
        return []

def run_tests():
    """Run a series of tests with different parameters"""
    # Test 1: Small batch with 1-minute data
    logger.info("=== TEST 1: Small batch with 1-minute intraday data ===")
    test_and_store(SAMPLE_STOCKS + SAMPLE_INDICES, '1m', '1d')
    
    # Test 2: Small batch with 5-minute data
    logger.info("=== TEST 2: Small batch with 5-minute intraday data ===")
    test_and_store(SAMPLE_STOCKS + SAMPLE_INDICES, '5m', '5d')
    
    # Test 3: Read all symbols from CSV and test batch size limits
    symbols = read_stock_symbols()
    if symbols:
        logger.info(f"Found {len(symbols)} symbols in CSV file")
        
        # Test with batches of 50 symbols
        batch_size = 50
        num_batches = (len(symbols) + batch_size - 1) // batch_size  # ceiling division
        
        for i in range(num_batches):
            start_idx = i * batch_size
            end_idx = min((i + 1) * batch_size, len(symbols))
            batch = symbols[start_idx:end_idx]
            
            logger.info(f"=== TEST 4.{i+1}: Batch {i+1}/{num_batches} ({len(batch)} symbols) ===")
            # Use 1-hour data for large batches to avoid excessive data volume
            test_batch_download(batch, '1h', '1d')
            time.sleep(2)  # Add delay between batches to avoid rate limiting

if __name__ == "__main__":
    logger.info("Starting batch download tests")
    run_tests()
    logger.info("Completed all batch download tests")
