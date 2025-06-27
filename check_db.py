import sqlite3
import pandas as pd
import argparse
import datetime

# Parse command line arguments
parser = argparse.ArgumentParser(description='Check DaxTracker3 database status')
parser.add_argument('--check-all', action='store_true', help='Check all aspects of the database')
parser.add_argument('--check-ml', action='store_true', help='Check ML metrics and predictions')
args = parser.parse_args()

# Connect to the database
conn = sqlite3.connect('market_data.db')
cursor = conn.cursor()

# List all tables
print("Tables in database:")
cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
tables = cursor.fetchall()
for table in tables:
    print(f"- {table[0]}")

# Check the latest market data
print("\nLatest market data:")
try:
    # Query for AAPL latest data
    aapl_df = pd.read_sql_query(
        'SELECT timestamp, close FROM market_data WHERE symbol="AAPL" ORDER BY timestamp DESC LIMIT 10', 
        conn
    )
    print("\nLatest AAPL data:")
    print(aapl_df)
    
    # Query for ^GDAXI latest data (DAX)
    dax_df = pd.read_sql_query(
        'SELECT timestamp, close FROM market_data WHERE symbol="^GDAXI" ORDER BY timestamp DESC LIMIT 10', 
        conn
    )
    print("\nLatest ^GDAXI (DAX) data:")
    print(dax_df)
    
    # Check if AAPL data from today exists
    today_data = pd.read_sql_query(
        "SELECT COUNT(*) FROM market_data WHERE symbol='AAPL' AND date(timestamp)='2025-06-27'",
        conn
    )
    print("\nAAPL data count for today (2025-06-27):", today_data.iloc[0, 0])
except Exception as e:
    print(f"Error checking market data: {e}")

# Check technical_analysis table
print("\nLatest technical analysis data:")
try:
    cursor.execute("SELECT COUNT(*) FROM trading_signals")
    count = cursor.fetchone()[0]
    print(f"Total rows in trading_signals: {count}")
    
    cursor.execute("SELECT DISTINCT symbol FROM trading_signals")
    symbols = cursor.fetchall()
    print(f"Number of unique symbols in signals: {len(symbols)}")
    print("Sample symbols in signals:")
    for i, symbol in enumerate(symbols[:10]):  # Show first 10
        print(f"- {symbol[0]}")
except Exception as e:
    print(f"Error checking trading_signals: {e}")

# Check ML-related tables if requested
if args.check_ml or args.check_all:
    print("\nChecking ML metrics and predictions:")
    try:
        # Check if ml_model_metrics table exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='ml_model_metrics'")
        if cursor.fetchone():
            cursor.execute("SELECT COUNT(*) FROM ml_model_metrics")
            count = cursor.fetchone()[0]
            print(f"Total rows in ml_model_metrics: {count}")
            
            if count > 0:
                # Get the latest metrics for each symbol
                metrics_df = pd.read_sql_query(
                    '''SELECT symbol, evaluation_date, accuracy, precision, recall, f1_score 
                    FROM ml_model_metrics ORDER BY evaluation_date DESC LIMIT 10''',
                    conn
                )
                print("\nLatest ML metrics:")
                print(metrics_df)
        else:
            print("ML model metrics table does not exist yet")
        
        # Check if ml_confusion_matrix table exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='ml_confusion_matrix'")
        if cursor.fetchone():
            cursor.execute("SELECT COUNT(*) FROM ml_confusion_matrix")
            count = cursor.fetchone()[0]
            print(f"Total rows in ml_confusion_matrix: {count}")
            
            if count > 0:
                # Get the latest predictions
                matrix_df = pd.read_sql_query(
                    '''SELECT symbol, evaluation_date, matrix_json 
                    FROM ml_confusion_matrix ORDER BY evaluation_date DESC LIMIT 10''',
                    conn
                )
                print("\nLatest ML confusion matrices:")
                print(matrix_df)
        else:
            print("ML confusion matrix table does not exist yet")
    except Exception as e:
        print(f"Error checking ML data: {e}")

conn.close()
