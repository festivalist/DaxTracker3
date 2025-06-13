import yfinance as yf
import pandas as pd
import sqlite3
import datetime
import logging

# Logger konfigurieren
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename='data_collector.log'
)
logger = logging.getLogger('DataCollector')

class DataCollector:
    def __init__(self, db_path):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.setup_database()
        logger.info("DataCollector initialized with database at %s", db_path)
        
    def setup_database(self):
        """Erstellt die benötigten Tabellen in der SQLite-Datenbank"""
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
        logger.info("Database tables created or already exist")
        
    def fetch_market_data(self, symbol, period="1d", interval="1m"):
        """Holt Marktdaten für ein Symbol von Yahoo Finance"""
        try:
            # Daten von Yahoo Finance abrufen
            stock = yf.Ticker(symbol)
            data = stock.history(period=period, interval=interval, auto_adjust=True)
            
            # Behandeln der Multi-Index-Struktur, die in der neuesten yfinance-Version eingeführt wurde
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
            logger.info(f"Successfully fetched and stored market data for {symbol}")
            return True
        except Exception as e:
            logger.error(f"Error fetching market data for {symbol}: {str(e)}")
            return False
    
    def fetch_news(self, symbol):
        """Holt Nachrichtendaten für ein Symbol von Yahoo Finance"""
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
                logger.info(f"Successfully fetched and stored news for {symbol}")
                return True
            logger.warning(f"No news found for {symbol}")
            return False
        except Exception as e:
            logger.error(f"Error fetching news for {symbol}: {str(e)}")
            return False
