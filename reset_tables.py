"""
Script to drop and recreate all relevant tables in market_data.db with the correct schema.
Run this before inserting demo data if you have schema errors.
"""
import sqlite3

conn = sqlite3.connect('market_data.db')
c = conn.cursor()

# Drop tables if they exist
c.execute("DROP TABLE IF EXISTS trading_signals")
c.execute("DROP TABLE IF EXISTS technical_analysis")
c.execute("DROP TABLE IF EXISTS sentiment_results")
c.execute("DROP TABLE IF EXISTS news_data")

# Recreate tables with correct schema
c.execute("""
CREATE TABLE trading_signals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT,
    timestamp DATETIME,
    signal_type TEXT,
    confidence REAL,
    close_price REAL,
    technical_signal TEXT,
    sentiment_signal TEXT,
    reason TEXT,
    notified INTEGER,
    verified INTEGER,
    outcome TEXT
)
""")
c.execute("""
CREATE TABLE technical_analysis (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT,
    timestamp DATETIME,
    close_price REAL,
    sma_20 REAL,
    sma_50 REAL,
    rsi REAL,
    macd_line REAL,
    signal_line REAL,
    overall_signal TEXT
)
""")
c.execute("""
CREATE TABLE sentiment_results (
    news_id INTEGER,
    symbol TEXT,
    negative_score REAL,
    neutral_score REAL,
    positive_score REAL,
    dominant_sentiment TEXT,
    confidence REAL,
    timestamp DATETIME
)
""")
c.execute("""
CREATE TABLE news_data (
    rowid INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT,
    summary TEXT
)
""")

conn.commit()
conn.close()
print("Tables dropped and recreated successfully.")
