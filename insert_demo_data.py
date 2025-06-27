"""
Script to insert demo data into market_data.db for dashboard testing.
"""
import sqlite3
from datetime import datetime, timedelta

conn = sqlite3.connect('market_data.db')
c = conn.cursor()

# Insert demo trading signals
demo_signals = [
    ("DAX", datetime.now() - timedelta(days=i), "BUY" if i % 2 == 0 else "SELL", 0.8, 18000 + i * 10, "bullish", "positive", "Test reason", 1, 1, "SUCCESS" if i % 3 != 0 else "FAIL")
    for i in range(10)
]
c.executemany("""
INSERT INTO trading_signals (symbol, timestamp, signal_type, confidence, close_price, technical_signal, sentiment_signal, reason, notified, verified, outcome)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
""", demo_signals)

# Insert demo technical analysis
demo_tech = [
    ("DAX", datetime.now() - timedelta(days=i), 18000 + i * 10, 17900 + i * 10, 17800 + i * 10, 55 + i, 1.2 + i * 0.1, 1.1 + i * 0.1, "BUY" if i % 2 == 0 else "SELL")
    for i in range(10)
]
c.executemany("""
INSERT INTO technical_analysis (symbol, timestamp, close_price, sma_20, sma_50, rsi, macd_line, signal_line, overall_signal)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
""", demo_tech)

# Insert demo news data
news = [
    (f"News Title {i}", f"Summary for news {i}") for i in range(10)
]
c.executemany("INSERT INTO news_data (title, summary) VALUES (?, ?)", news)

# Insert demo sentiment results
for i in range(10):
    c.execute("""
    INSERT INTO sentiment_results (news_id, symbol, negative_score, neutral_score, positive_score, dominant_sentiment, confidence, timestamp)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        i+1, "DAX", 0.1*i, 0.2*i, 0.7*i, "positive" if i % 2 == 0 else "neutral", 0.8, datetime.now() - timedelta(days=i)
    ))

conn.commit()
conn.close()
print("Demo data inserted successfully.")
