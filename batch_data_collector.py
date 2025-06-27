"""
Enhanced data collector using batch download for efficient API usage.
This module provides an alternative to individual API calls, optimizing
for collecting data for large symbol lists while respecting rate limits.
"""

import yfinance as yf
import pandas as pd
import sqlite3
import datetime
import logging
import time
import random
import os
from pathlib import Path

# Configure logging
log_dir = Path("logs")
log_dir.mkdir(exist_ok=True)

# Configure file handler
file_handler = logging.FileHandler(log_dir / 'batch_collector.log')
file_handler.setLevel(logging.INFO)
file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(file_formatter)

# Configure console handler
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler.setFormatter(console_formatter)

# Set up logger
logger = logging.getLogger('BatchCollector')
logger.setLevel(logging.INFO)
logger.addHandler(file_handler)
logger.addHandler(console_handler)

# Prevent propagation to root logger to avoid duplicate logs
logger.propagate = False

class BatchDataCollector:
    """
    A class to collect market data for multiple symbols efficiently using
    yfinance's batch download capability.
    
    This class organizes symbols into optimal batches, handles rate limiting,
    and provides efficient storage into a SQLite database.
    """
    
    def __init__(self, db_path='market_data.db', max_batch_size=50, backoff_factor=2):
        """
        Initialize the BatchDataCollector.
        
        Args:
            db_path (str): Path to the SQLite database
            max_batch_size (int): Maximum number of symbols per batch
            backoff_factor (int): Multiplication factor for exponential backoff
        """
        self.db_path = db_path
        self.max_batch_size = max_batch_size
        self.backoff_factor = backoff_factor
        self.consecutive_errors = 0
        self.last_request_time = 0
        self.conn = None
        self.connect_to_db()
        
    def connect_to_db(self):
        """Connect to the SQLite database."""
        try:
            self.conn = sqlite3.connect(self.db_path)
            self.setup_database()
            return True
        except Exception as e:
            logger.error(f"Database connection error: {e}")
            return False
            
    def setup_database(self):
        """Set up database tables if they don't exist."""
        if self.conn is None:
            return False
            
        cursor = self.conn.cursor()
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS market_data (
            timestamp TEXT,
            symbol TEXT,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume INTEGER,
            PRIMARY KEY (timestamp, symbol)
        )
        ''')
        
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS collection_stats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            batch_size INTEGER,
            symbols_requested INTEGER,
            symbols_received INTEGER,
            elapsed_time REAL,
            interval TEXT,
            period TEXT,
            errors INTEGER
        )
        ''')
        
        self.conn.commit()
        return True
        
    def close(self):
        """Close the database connection."""
        if self.conn:
            self.conn.close()
            self.conn = None
            
    def __del__(self):
        """Destructor to ensure connection is closed."""
        self.close()
        
    def sleep_with_backoff(self):
        """
        Sleep between API calls with adaptive timing based on errors.
        Uses exponential backoff if encountering errors.
        """
        base_sleep = 1.0  # Base sleep time in seconds
        
        # Calculate adaptive sleep time based on consecutive errors
        if self.consecutive_errors > 0:
            # Exponential backoff with jitter
            max_sleep = base_sleep * (self.backoff_factor ** self.consecutive_errors)
            sleep_time = max_sleep * (0.5 + 0.5 * random.random())  # Add jitter
        else:
            sleep_time = base_sleep
            
        # Ensure at least some time has passed since the last request
        elapsed = time.time() - self.last_request_time
        if elapsed < sleep_time:
            remaining = sleep_time - elapsed
            logger.debug(f"Sleeping for {remaining:.2f} seconds")
            time.sleep(remaining)
            
        self.last_request_time = time.time()
        
    def store_batch_data(self, data):
        """
        Store batch downloaded data in the database.
        
        Args:
            data (pandas.DataFrame): Multi-level DataFrame from yf.download
            
        Returns:
            tuple: (success, records_stored, symbols_with_data)
        """
        if self.conn is None and not self.connect_to_db():
            logger.error("No database connection and reconnection failed")
            return False, 0, []
        
        cursor = self.conn.cursor()
        records_stored = 0
        symbols_with_data = []
        
        try:
            # For multi-level DataFrame (grouped by ticker)
            if isinstance(data.columns, pd.MultiIndex):
                for symbol in data.columns.levels[0]:
                    symbol_data = data[symbol]
                    symbol_data = symbol_data.dropna(how='all')  # Skip rows with all NaN
                    
                    if len(symbol_data) > 0:
                        symbols_with_data.append(symbol)
                        
                        for timestamp, row in symbol_data.iterrows():
                            try:
                                cursor.execute('''
                                INSERT OR REPLACE INTO market_data 
                                (timestamp, symbol, open, high, low, close, volume)
                                VALUES (?, ?, ?, ?, ?, ?, ?)
                                ''', (
                                    timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                                    symbol,
                                    float(row.get('Open')) if pd.notna(row.get('Open')) else None,
                                    float(row.get('High')) if pd.notna(row.get('High')) else None,
                                    float(row.get('Low')) if pd.notna(row.get('Low')) else None,
                                    float(row.get('Close')) if pd.notna(row.get('Close')) else None,
                                    int(row.get('Volume')) if pd.notna(row.get('Volume')) else None
                                ))
                                records_stored += 1
                            except Exception as e:
                                logger.warning(f"Error storing data for {symbol} at {timestamp}: {e}")
            # For single-level DataFrame (single symbol or merged result)
            else:
                for timestamp, row in data.iterrows():
                    try:
                        # In this case we don't have symbol information in the DataFrame
                        # We'd need to pass it separately or handle this case differently
                        logger.warning("Single-level DataFrame not supported for storage")
                    except Exception as e:
                        logger.warning(f"Error storing data at {timestamp}: {e}")
            
            self.conn.commit()
            logger.info(f"Stored {records_stored} records for {len(symbols_with_data)} symbols")
            return True, records_stored, symbols_with_data
            
        except Exception as e:
            logger.error(f"Error storing batch data: {e}")
            self.conn.rollback()
            return False, 0, []
            
    def log_collection_stats(self, batch_size, symbols_requested, symbols_received, 
                            elapsed_time, interval, period, errors):
        """Log collection statistics to the database."""
        if self.conn is None:
            return False
            
        try:
            cursor = self.conn.cursor()
            cursor.execute('''
            INSERT INTO collection_stats
            (timestamp, batch_size, symbols_requested, symbols_received, 
             elapsed_time, interval, period, errors)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                batch_size,
                symbols_requested,
                symbols_received,
                elapsed_time,
                interval,
                period,
                errors
            ))
            self.conn.commit()
            return True
        except Exception as e:
            logger.error(f"Error logging collection stats: {e}")
            return False
            
    def collect_batch(self, symbols, interval='1m', period='1d', retries=3):
        """
        Collect data for a batch of symbols.
        
        Args:
            symbols (list): List of symbols to collect
            interval (str): Data interval (1m, 5m, 15m, 30m, 1h, 1d, etc.)
            period (str): Period to download (1d, 5d, 1mo, 3mo, etc.)
            retries (int): Number of retries if download fails
            
        Returns:
            tuple: (success, data, symbols_with_data)
        """
        if not symbols:
            return False, None, []
            
        batch_size = len(symbols)
        attempt = 0
        errors = 0
        
        while attempt < retries:
            try:
                logger.info(f"Collecting data for batch of {batch_size} symbols (interval={interval}, period={period})")
                
                # Sleep with backoff before request
                self.sleep_with_backoff()
                
                # Track timing
                start_time = time.time()
                
                # Download data
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
                
                elapsed = time.time() - start_time
                logger.info(f"Download completed in {elapsed:.2f} seconds")
                
                # Check for empty result
                if data is None or data.empty:
                    logger.warning(f"No data returned for batch of {batch_size} symbols")
                    attempt += 1
                    errors += 1
                    self.consecutive_errors += 1
                    continue
                    
                # Count symbols with data
                symbols_with_data = []
                if isinstance(data.columns, pd.MultiIndex):
                    for symbol in symbols:
                        if symbol in data.columns.levels[0]:
                            symbol_data = data[symbol].dropna(how='all')
                            if len(symbol_data) > 0:
                                symbols_with_data.append(symbol)
                                
                # Store statistics
                self.log_collection_stats(
                    batch_size=batch_size,
                    symbols_requested=len(symbols),
                    symbols_received=len(symbols_with_data),
                    elapsed_time=elapsed,
                    interval=interval,
                    period=period,
                    errors=errors
                )
                
                # Store data in database
                success, records_stored, symbols_stored = self.store_batch_data(data)
                
                # Reset consecutive errors on success
                if success and records_stored > 0:
                    self.consecutive_errors = 0
                
                return success, data, symbols_stored
                
            except Exception as e:
                elapsed = time.time() - start_time
                attempt += 1
                errors += 1
                self.consecutive_errors += 1
                
                logger.error(f"Error in batch download (attempt {attempt}/{retries}): {str(e)}")
                
                # Longer backoff for retries
                time.sleep(2 ** attempt)
                
        # If all retries failed
        return False, None, []
        
    def collect_data(self, symbols, interval='1m', period='1d'):
        """
        Collect data for multiple symbols, automatically splitting into batches.
        
        Args:
            symbols (list): List of symbols to collect
            interval (str): Data interval (1m, 5m, 15m, 30m, 1h, 1d, etc.)
            period (str): Period to download (1d, 5d, 1mo, 3mo, etc.)
            
        Returns:
            tuple: (success, total_symbols_collected)
        """
        if not symbols:
            return False, 0
            
        total_symbols = len(symbols)
        total_collected = 0
        
        # Split into batches
        batches = [symbols[i:i+self.max_batch_size] 
                  for i in range(0, total_symbols, self.max_batch_size)]
        
        logger.info(f"Collecting data for {total_symbols} symbols in {len(batches)} batches")
        
        batch_results = []
        for i, batch in enumerate(batches):
            logger.info(f"Processing batch {i+1}/{len(batches)} ({len(batch)} symbols)")
            
            success, _, symbols_collected = self.collect_batch(batch, interval, period)
            total_collected += len(symbols_collected)
            
            batch_results.append({
                'batch_num': i+1,
                'symbols_requested': len(batch),
                'symbols_collected': len(symbols_collected),
                'success': success
            })
            
            # Add delay between batches
            if i < len(batches) - 1:
                delay = 2 + random.random() * 2  # 2-4 seconds between batches
                logger.debug(f"Sleeping {delay:.2f}s between batches")
                time.sleep(delay)
                
        # Summarize results
        logger.info(f"Collection completed: {total_collected}/{total_symbols} symbols with data")
        for result in batch_results:
            logger.debug(f"Batch {result['batch_num']}: " +
                       f"{result['symbols_collected']}/{result['symbols_requested']} " +
                       f"symbols collected, success: {result['success']}")
                       
        return total_collected > 0, total_collected
        
    def fetch_news(self, symbols):
        """
        Fetch news data for multiple symbols.
        Note: yfinance doesn't support batch news download, so this is done individually.
        
        Args:
            symbols (list): List of symbols to fetch news for
            
        Returns:
            int: Number of symbols with news data collected
        """
        if not symbols:
            return 0
            
        if self.conn is None and not self.connect_to_db():
            logger.error("No database connection and reconnection failed")
            return 0
            
        # Ensure news table exists
        cursor = self.conn.cursor()
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS news_data (
            timestamp TEXT,
            symbol TEXT,
            title TEXT,
            summary TEXT,
            url TEXT,
            PRIMARY KEY (timestamp, symbol, url)
        )
        ''')
        self.conn.commit()
        
        news_count = 0
        
        for symbol in symbols:
            try:
                # Sleep between requests
                self.sleep_with_backoff()
                
                logger.info(f"Fetching news for {symbol}")
                stock = yf.Ticker(symbol)
                news = stock.news
                
                if news:
                    cursor = self.conn.cursor()
                    for item in news:
                        timestamp = datetime.datetime.fromtimestamp(item.get('providerPublishTime', 0))
                        cursor.execute('''
                        INSERT OR IGNORE INTO news_data
                        (timestamp, symbol, title, summary, url)
                        VALUES (?, ?, ?, ?, ?)
                        ''', (
                            timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                            symbol,
                            item.get('title', ''),
                            item.get('summary', ''),
                            item.get('link', '')
                        ))
                    self.conn.commit()
                    news_count += 1
                    logger.info(f"Stored {len(news)} news items for {symbol}")
                else:
                    logger.info(f"No news items found for {symbol}")
                    
            except Exception as e:
                logger.error(f"Error fetching news for {symbol}: {e}")
                self.consecutive_errors += 1
                
        logger.info(f"News collection completed for {news_count}/{len(symbols)} symbols")
        return news_count

# Example usage
def main():
    # Initialize collector
    collector = BatchDataCollector(max_batch_size=40)
    
    # Sample usage
    sample_symbols = ['AAPL', 'MSFT', 'AMZN', '^GDAXI', '^GSPC']
    
    # Collect intraday data
    logger.info("Collecting 1-minute intraday data")
    collector.collect_data(sample_symbols, interval='1m', period='1d')
    
    # Collect daily data
    logger.info("Collecting daily data")
    collector.collect_data(sample_symbols, interval='1d', period='1mo')
    
    # Fetch news
    logger.info("Collecting news data")
    collector.fetch_news(sample_symbols[:3])  # Only for stocks, not indices
    
    # Clean up
    collector.close()

if __name__ == "__main__":
    main()
