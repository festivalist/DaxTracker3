import sqlite3
from signal_generator import SignalGenerator

# Get available symbols
conn = sqlite3.connect('market_data.db')
cursor = conn.cursor()
cursor.execute("SELECT DISTINCT symbol FROM market_data")
symbols = [row[0] for row in cursor.fetchall()]
conn.close()

print(f"Found {len(symbols)} symbols with market data")

# Generate signals for all symbols at once
signal_gen = SignalGenerator(db_path='market_data.db')
count = 0

# Process symbols in batches to avoid memory issues
batch_size = 10
for i in range(0, len(symbols), batch_size):
    batch = symbols[i:i+batch_size]
    print(f"\nProcessing batch of {len(batch)} symbols ({i+1}-{i+len(batch)} of {len(symbols)})")
    try:
        # Call generate_signals with list of symbols (first version of the method)
        signals = signal_gen.generate_signals(batch)
        if signals:
            count += len(signals)
        print(f"Batch processed successfully")
    except Exception as e:
        print(f"Error processing batch: {e}")

print(f"\nGenerated signals for approximately {count} out of {len(symbols)} symbols")

# Now check if trading_signals table has entries for multiple symbols
conn = sqlite3.connect('market_data.db')
cursor = conn.cursor()
cursor.execute("SELECT DISTINCT symbol FROM trading_signals")
signal_symbols = [row[0] for row in cursor.fetchall()]
print(f"\nTrading signals table now has {len(signal_symbols)} unique symbols")
if len(signal_symbols) > 0:
    print("Sample symbols in trading_signals:")
    for symbol in signal_symbols[:10]:  # Show first 10
        print(f"- {symbol}")
conn.close()
