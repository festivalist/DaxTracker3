import sqlite3
from signal_generator import SignalGenerator

# Get available symbols
conn = sqlite3.connect('market_data.db')
cursor = conn.cursor()
cursor.execute("SELECT DISTINCT symbol FROM market_data")
symbols = [row[0] for row in cursor.fetchall()]
conn.close()

print(f"Found {len(symbols)} symbols with market data")

# Generate signals for each symbol
signal_gen = SignalGenerator(db_path='market_data.db')
count = 0

# Process a subset for testing (first 5)
for symbol in symbols[:5]:
    print(f"\nProcessing {symbol}")
    try:
        signal_gen.generate_signals(symbol)
        count += 1
        print(f"Success for {symbol}")
    except Exception as e:
        print(f"Error for {symbol}: {e}")

print(f"\nGenerated signals for {count} out of 5 symbols")
