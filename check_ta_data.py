import sqlite3
import pandas as pd

# Connect to the database
conn = sqlite3.connect('market_data.db')

# Check available technical analysis data for AAPL
query = """
SELECT COUNT(*) 
FROM technical_analysis
WHERE symbol = 'AAPL'
"""

count = pd.read_sql_query(query, conn).iloc[0, 0]
print(f"AAPL technical analysis rows: {count}")

# Show the most recent data
query = """
SELECT * 
FROM technical_analysis
WHERE symbol = 'AAPL'
ORDER BY timestamp DESC
LIMIT 10
"""

ta_data = pd.read_sql_query(query, conn)
print("\nMost recent technical analysis data for AAPL:")
print(ta_data)

conn.close()
