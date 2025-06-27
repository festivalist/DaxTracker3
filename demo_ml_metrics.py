"""
Demo ML Metrics Generator

This script creates ML metrics for demo purposes, simulating model evaluation results.
"""

import sqlite3
import pandas as pd
import json
import argparse
from datetime import datetime

def generate_demo_metrics(symbol='AAPL', db_path='market_data.db'):
    """Generate demo metrics and insert them into the database."""
    
    # Define demo metrics - these would normally come from actual model evaluation
    metrics = {
        'symbol': symbol,
        'evaluation_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'accuracy': 0.78,
        'precision': 0.82,
        'recall': 0.75,
        'f1_score': 0.783,
        'data_points': 200,
        'lookback_days': 60,
        'model_version': '1.0'
    }
    
    # Create confusion matrix
    confusion_matrix = {
        'true_positive': 45,
        'false_positive': 10,
        'true_negative': 38,
        'false_negative': 15
    }
    
    # Connect to the database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        # Create metrics tables if they don't exist
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS ml_model_metrics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT,
            evaluation_date TEXT,
            accuracy REAL,
            precision REAL,
            recall REAL,
            f1_score REAL,
            data_points INTEGER,
            lookback_days INTEGER,
            model_version TEXT
        )
        """)
        
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS ml_confusion_matrix (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT,
            evaluation_date TEXT,
            matrix_json TEXT,
            model_version TEXT
        )
        """)
        
        conn.commit()
        
        # Insert metrics
        cursor.execute("""
        INSERT INTO ml_model_metrics (
            symbol, evaluation_date, accuracy, precision, recall, 
            f1_score, data_points, lookback_days, model_version
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            metrics['symbol'], metrics['evaluation_date'], metrics['accuracy'],
            metrics['precision'], metrics['recall'], metrics['f1_score'],
            metrics['data_points'], metrics['lookback_days'], metrics['model_version']
        ))
        
        # Insert confusion matrix
        cursor.execute("""
        INSERT INTO ml_confusion_matrix (
            symbol, evaluation_date, matrix_json, model_version
        ) VALUES (?, ?, ?, ?)
        """, (
            metrics['symbol'], metrics['evaluation_date'], 
            json.dumps(confusion_matrix), metrics['model_version']
        ))
        
        conn.commit()
        print(f"Demo metrics created for {symbol}")
        print(f"Accuracy: {metrics['accuracy']:.4f}, F1 Score: {metrics['f1_score']:.4f}")
        
        # Also add some sample predictions
        create_demo_predictions(cursor, symbol)
        conn.commit()
        
    except Exception as e:
        print(f"Error creating demo metrics: {e}")
    finally:
        conn.close()

def create_demo_predictions(cursor, symbol):
    """Create demo prediction data for visualization"""
    
    # Create predictions table if it doesn't exist
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS ml_predictions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol TEXT,
        prediction_date TEXT,
        prediction TEXT,
        confidence REAL,
        actual_direction TEXT,
        is_correct INTEGER
    )
    """)
    
    # Sample predictions data - would come from actual model in real system
    predictions = [
        (symbol, '2025-06-20', 'up', 0.78, 'up', 1),
        (symbol, '2025-06-19', 'down', 0.65, 'down', 1),
        (symbol, '2025-06-18', 'sideways', 0.52, 'up', 0),
        (symbol, '2025-06-17', 'up', 0.81, 'up', 1),
        (symbol, '2025-06-16', 'down', 0.72, 'sideways', 0),
        (symbol, '2025-06-13', 'up', 0.68, 'up', 1),
        (symbol, '2025-06-12', 'up', 0.55, 'down', 0),
        (symbol, '2025-06-11', 'down', 0.76, 'down', 1),
        (symbol, '2025-06-10', 'sideways', 0.59, 'sideways', 1),
        (symbol, '2025-06-09', 'up', 0.82, 'up', 1),
    ]
    
    # Insert predictions
    cursor.executemany("""
    INSERT INTO ml_predictions (
        symbol, prediction_date, prediction, confidence, actual_direction, is_correct
    ) VALUES (?, ?, ?, ?, ?, ?)
    """, predictions)
    
    print(f"Added {len(predictions)} demo predictions for {symbol}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Generate demo ML metrics')
    parser.add_argument('--symbol', default='AAPL', help='Symbol to generate metrics for')
    args = parser.parse_args()
    
    generate_demo_metrics(symbol=args.symbol)
