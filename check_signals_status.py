import sqlite3
import datetime

# Connect to the database
conn = sqlite3.connect('market_data.db')
cursor = conn.cursor()

# Check trading_signals table status
print("TRADING SIGNALS TABLE STATUS:")
print("----------------------------")

# Get distinct symbols
cursor.execute("SELECT COUNT(DISTINCT symbol) FROM trading_signals")
symbol_count = cursor.fetchone()[0]
print(f"Number of unique symbols with signals: {symbol_count}")

# Get total signals
cursor.execute("SELECT COUNT(*) FROM trading_signals")
signal_count = cursor.fetchone()[0]
print(f"Total number of signal entries: {signal_count}")

# Get signal types breakdown
cursor.execute("""
    SELECT signal_type, COUNT(*) 
    FROM trading_signals 
    GROUP BY signal_type
    ORDER BY COUNT(*) DESC
""")
signal_types = cursor.fetchall()
print("\nSignal Types Breakdown:")
for signal_type, count in signal_types:
    print(f"  {signal_type}: {count}")

# Show example symbols with their signal counts
cursor.execute("""
    SELECT symbol, COUNT(*) as signal_count
    FROM trading_signals
    GROUP BY symbol
    ORDER BY signal_count DESC
    LIMIT 10
""")
signal_symbols = cursor.fetchall()
print("\nTop 10 symbols by signal count:")
for symbol, count in signal_symbols:
    print(f"  {symbol}: {count} signals")

# Check for recent signals (last 24 hours)
yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
yesterday_str = yesterday.strftime('%Y-%m-%d %H:%M:%S')
cursor.execute(f"""
    SELECT COUNT(*) 
    FROM trading_signals 
    WHERE timestamp > '{yesterday_str}'
""")
recent_count = cursor.fetchone()[0]
print(f"\nSignals generated in the last 24 hours: {recent_count}")

# Check market_data table for comparison
cursor.execute("SELECT COUNT(DISTINCT symbol) FROM market_data")
market_data_symbols = cursor.fetchone()[0]
print(f"\nSymbols in market_data table: {market_data_symbols}")

# Check which symbols have market data but no signals
cursor.execute("""
    SELECT symbol 
    FROM (
        SELECT DISTINCT symbol FROM market_data
        EXCEPT
        SELECT DISTINCT symbol FROM trading_signals
    )
    LIMIT 10
""")
missing_signals = cursor.fetchall()
print(f"\nSymbols with market data but no signals (showing first 10 of potentially many):")
for symbol in missing_signals:
    print(f"  {symbol[0]}")

conn.close()
