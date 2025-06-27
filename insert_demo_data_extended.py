"""
Script to insert extended demo data for all dashboard tabs, including technical, sentiment, and backtest tables.
Also generates extended demo market data and technical analysis for AAPL to enable ML metrics testing.
"""
import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import subprocess

conn = sqlite3.connect('market_data.db')
c = conn.cursor()

now = datetime.now()

def insert_extended_aapl_data():
    """
    Insert extended demo data for AAPL stock to enable 
    ML model evaluation tests
    """
    print("Inserting extended demo data for AAPL...")
    
    # Check last date in the database
    c.execute("SELECT MAX(timestamp) FROM market_data WHERE symbol = 'AAPL'")
    last_date_str = c.fetchone()[0]
    
    if last_date_str:
        last_date = datetime.strptime(last_date_str.split(' ')[0], '%Y-%m-%d')
    else:
        # If no data, start from 3 months ago
        last_date = datetime.now() - timedelta(days=90)
    
    # Generate data for 90 days
    start_date = last_date + timedelta(days=1)
    end_date = start_date + timedelta(days=90)
    
    # Starting values
    last_open = 180.0
    last_high = 185.0
    last_low = 178.0
    last_close = 182.0
    last_volume = 30000000
    
    # Volatility
    volatility = 0.02
    
    # Data to insert
    data_to_insert = []
    
    current_date = start_date
    
    while current_date < end_date:
        # Skip weekends
        if current_date.weekday() >= 5:
            current_date += timedelta(days=1)
            continue
        
        # Generate OHLCV with realistic price movements
        change_pct = np.random.normal(0, volatility)
        
        # New values
        open_price = last_close * (1 + np.random.normal(0, volatility/2))
        close_price = open_price * (1 + change_pct)
        high_price = max(open_price, close_price) * (1 + abs(np.random.normal(0, volatility/2)))
        low_price = min(open_price, close_price) * (1 - abs(np.random.normal(0, volatility/2)))
        volume = int(last_volume * (1 + np.random.normal(0, 0.3)))
        
        # Ensure values make sense
        if low_price <= 0:
            low_price = min(open_price, close_price) * 0.95
        
        # Format timestamp
        timestamp = current_date.strftime('%Y-%m-%d 16:00:00')
        
        # Add to data
        data_to_insert.append(
            ('AAPL', timestamp, round(open_price, 2), round(high_price, 2), 
             round(low_price, 2), round(close_price, 2), volume)
        )
        
        # Update last values
        last_open = open_price
        last_high = high_price
        last_low = low_price
        last_close = close_price
        last_volume = volume
        
        # Next day
        current_date += timedelta(days=1)
    
    # Insert into database
    c.executemany(
        """
        INSERT INTO market_data 
        (symbol, timestamp, open, high, low, close, volume) 
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """, 
        data_to_insert
    )
    conn.commit()
    
    print(f"Inserted {len(data_to_insert)} days of extended data for AAPL")
    

# Insert more demo trading signals (10 days, 2 per day)
signals = []
for i in range(10):
    for t in [0, 12]:
        ts = now - timedelta(days=i, hours=t)
        signals.append(("DAX", ts, "BUY" if (i+t)%2==0 else "SELL", round(random.uniform(0.6, 0.9),2), 18000+i*10+t, "bullish", "positive", "Reason", 1, 1, "SUCCESS" if (i+t)%3!=0 else "FAIL"))
c.executemany("""
INSERT INTO trading_signals (symbol, timestamp, signal_type, confidence, close_price, technical_signal, sentiment_signal, reason, notified, verified, outcome)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
""", signals)

# Insert more technical analysis data (10 days)
techs = []
for i in range(10):
    ts = now - timedelta(days=i)
    techs.append(("DAX", ts, 18000+i*10, 17900+i*10, 17800+i*10, 50+random.randint(-10,10), 1.2+i*0.1, 1.1+i*0.1, "BUY" if i%2==0 else "SELL"))
c.executemany("""
INSERT INTO technical_analysis (symbol, timestamp, close_price, sma_20, sma_50, rsi, macd_line, signal_line, overall_signal)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
""", techs)

# Insert more news data and sentiment results
for i in range(10):
    c.execute("INSERT INTO news_data (title, summary) VALUES (?, ?)", (f"News Title {i+10}", f"Summary for news {i+10}"))
    news_id = c.lastrowid
    c.execute("""
    INSERT INTO sentiment_results (news_id, symbol, negative_score, neutral_score, positive_score, dominant_sentiment, confidence, timestamp)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (news_id, "DAX", round(random.uniform(0,0.3),2), round(random.uniform(0,0.3),2), round(random.uniform(0.4,0.9),2), random.choice(["positive","neutral","negative"]), round(random.uniform(0.6,0.95),2), now - timedelta(days=i)))

# Insert demo backtest results (simulate 10 trades)
for i in range(10):
    c.execute("""
    INSERT INTO backtest_results (timestamp, symbol, signal_type, entry_price, exit_price, quantity, profit_loss, hold_time_hours, market_condition, confidence, success)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (now - timedelta(days=i), "DAX", random.choice(["BUY","SELL"]), 18000+i*10, 18010+i*10, 1, random.uniform(-100,200), random.uniform(1,24), random.choice(["bullish","bearish","neutral"]), round(random.uniform(0.6,0.9),2), random.choice([0,1])))

# Insert demo backtest metrics (simulate 3 runs)
for i in range(3):
    c.execute("""
    INSERT INTO backtest_metrics (timestamp, total_trades, winning_trades, losing_trades, total_profit_loss, win_rate, avg_profit_per_trade, max_drawdown, sharpe_ratio, initial_capital, final_capital)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (now - timedelta(days=i*10), 10, 6, 4, 500.0, 0.6, 50.0, 0.05, 1.2, 100000, 100500))

conn.commit()
print("Extended demo data inserted for standard tables.")

# Insert extended AAPL data and generate technical analysis
insert_extended_aapl_data()

# Run technical analysis on the new AAPL data
conn.close()
print("Generating technical analysis for AAPL...")
subprocess.run(["python", "quick_technical_analysis.py"])

print("All extended demo data inserted successfully.")
