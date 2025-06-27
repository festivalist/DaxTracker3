import sqlite3

# Connect to the database
conn = sqlite3.connect('market_data.db')
cursor = conn.cursor()

# List all tables
print("Tables in database:")
cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
tables = cursor.fetchall()
for table in tables:
    print(f"- {table[0]}")

# Close connection
conn.close()
