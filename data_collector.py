"""
Data Collector Module

This module handles market data collection from Yahoo Finance and Alpha Vantage, storing it in a SQLite database.
It provides functionality to fetch market data (OHLCV) and news data for various symbols.

The module uses yfinance and requests for data fetching, and sqlite3 for data storage.

Example:
    collector = DataCollector("market_data.db")
    data = collector.fetch_market_data("^GDAXI", period="1d", interval="1m")
"""

import yfinance as yf
import pandas as pd
import sqlite3
import datetime
import logging
import requests
import symbol_mapping  # Import the symbol mapping module

ALPHA_VANTAGE_API_KEY = 'YOUR_ALPHA_VANTAGE_API_KEY'  # Replace with your real key

class DataCollector:
    """
    A class to collect and store market data from Yahoo Finance and Alpha Vantage.
    
    This class handles:
    - Database initialization and connection
    - Market data fetching and storage
    - News data fetching and storage
    - Data validation and existence checks
    
    Attributes:
        db_path (str): Path to the SQLite database file
        conn (sqlite3.Connection): Database connection object
    """
    
    def __init__(self, db_path):
        """
        Initializes the DataCollector with a database connection.
        
        Args:
            db_path (str): Path to the SQLite database file
        """
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.setup_database()
        
    def setup_database(self):
        """
        Sets up the database schema if it doesn't exist.
        
        Creates two tables:
        1. market_data: For storing OHLCV data
        2. news_data: For storing news articles and summaries
        """
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
        
    def has_symbol_data(self, symbol: str) -> bool:
        """
        Checks if data exists for a given symbol.
        
        Args:
            symbol (str): The trading symbol to check
            
        Returns:
            bool: True if data exists, False otherwise
        """
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT COUNT(*) FROM market_data WHERE symbol = ?",
            (symbol,)
        )
        count = cursor.fetchone()[0]
        return count > 0
        
    def fetch_market_data(self, symbol, period="1d", interval="1m"):
        """
        Fetches market data from Yahoo Finance for a given symbol.
        
        This method fetches OHLCV (Open, High, Low, Close, Volume) data
        and stores it in the database.
        
        Args:
            symbol (str): Trading symbol (e.g., "^GDAXI" for DAX)
            period (str, optional): Time period to fetch. Defaults to "1d".
                Valid values: e.g., "1d", "5d", "1mo", "3mo", "1y"
            interval (str, optional): Data interval. Defaults to "1m".
                Valid values: e.g., "1m", "5m", "15m", "30m", "1h"
                
        Returns:
            pandas.DataFrame: The fetched market data, or None if failed
            
        Example:
            >>> collector = DataCollector("market_data.db")
            >>> data = collector.fetch_market_data("^GDAXI", "1d", "1m")
            >>> print(data.head())
        """
        # Wichtiger Fix für die neue yfinance-Version 0.2.63
        try:
            stock = yf.Ticker(symbol)
            data = stock.history(period=period, interval=interval, auto_adjust=True)
            
            # Multi-Index-Struktur behandeln
            if isinstance(data.columns, pd.MultiIndex):
                data.columns = ['_'.join(col).strip() for col in data.columns.values]
            
            # Daten in die Datenbank speichern
            for timestamp, row in data.iterrows():
                cursor = self.conn.cursor()
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
            self.conn.commit()
            return True
        except Exception as e:
            return False
    
    def fetch_news(self, symbol):
        """
        Fetches news data from Yahoo Finance for a given symbol.
        
        This method retrieves the latest news articles and summaries
        and stores them in the database.
        
        Args:
            symbol (str): Trading symbol (e.g., "^GDAXI" for DAX)
                
        Returns:
            bool: True if news data was fetched and stored, False otherwise
            
        Example:
            >>> collector = DataCollector("market_data.db")
            >>> collector.fetch_news("^GDAXI")
        """
        # Nachrichtendaten von Yahoo Finance abrufen
        try:
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
                return True
            return False
        except Exception as e:
            return False
    
    def fetch_yahoo(self, symbol, start=None, end=None, interval='1d'):
        """
        Fetches OHLCV data from Yahoo Finance and stores it in the database.
        """
        try:
            # Get the correct Yahoo Finance symbol
            yahoo_symbol = symbol_mapping.get_yahoo_symbol(symbol)
            
            stock = yf.Ticker(yahoo_symbol)
            data = stock.history(start=start, end=end, interval=interval, auto_adjust=True)
            if data.empty:
                print(f"No data for {symbol} from Yahoo Finance.")
                return False
            for timestamp, row in data.iterrows():
                cursor = self.conn.cursor()
                cursor.execute('''
                INSERT OR REPLACE INTO market_data 
                (timestamp, symbol, open, high, low, close, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (
                    timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                    symbol,  # Store with the original symbol
                    row.get('Open', None),
                    row.get('High', None),
                    row.get('Low', None),
                    row.get('Close', None),
                    row.get('Volume', None)
                ))
            self.conn.commit()
            print(f"Inserted {len(data)} rows for {symbol} from Yahoo Finance.")
            return True
        except Exception as e:
            print(f"Yahoo Finance error: {e}")
            return False

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Fetch and store market data from Yahoo Finance.")
    parser.add_argument('--symbol', required=True, help='Stock symbol (e.g. AAPL)')
    parser.add_argument('--start', help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end', help='End date (YYYY-MM-DD)')
    parser.add_argument('--interval', default='1d', help='Data interval (e.g. 1d, 1h, 5m)')
    parser.add_argument('--ml-export', action='store_true', help='Export data for ML pipeline')
    args = parser.parse_args()
    collector = DataCollector('market_data.db')
    collector.fetch_yahoo(args.symbol, start=args.start, end=args.end, interval=args.interval)
    if args.ml_export:
        df = collector.export_for_ml(args.symbol, args.start, args.end)
        print(df.head())
