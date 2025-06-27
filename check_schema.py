import sqlite3

# Connect to the database
conn = sqlite3.connect('market_data.db')
cursor = conn.cursor()

# Check if technical_analysis table exists
cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='technical_analysis'")
if cursor.fetchone():
    print("Table technical_analysis exists")
    
    # Get table schema
    cursor.execute('PRAGMA table_info(technical_analysis)')
    columns = cursor.fetchall()
    print("\nColumns:")
    for col in columns:
        print(f"{col[0]}: {col[1]} ({col[2]})")
else:
    print("Table technical_analysis does not exist")

# Close connection
conn.close()
