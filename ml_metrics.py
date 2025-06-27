"""
ML Model Performance Metrics

This script evaluates ML models and stores performance metrics in the database.
It's designed to run after model training or as a scheduled task.
"""

import os
import sys
import sqlite3
import argparse
import logging
import json
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, confusion_matrix
from market_predictor import MarketPredictor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename='ml_metrics.log'
)
logger = logging.getLogger('ML_Metrics')
console = logging.StreamHandler()
console.setLevel(logging.INFO)
logger.addHandler(console)

def evaluate_model(symbol, db_path='market_data.db', checkpoint_dir='checkpoints', 
                  lookback_days=90, save_to_db=True):
    """
    Evaluates a model's performance and saves metrics to database
    
    Args:
        symbol: Stock symbol to evaluate
        db_path: Path to database
        checkpoint_dir: Model checkpoint directory
        lookback_days: Days of historical data to evaluate
        save_to_db: Whether to save results to database
        
    Returns:
        dict with evaluation metrics
    """
    logger.info(f"Evaluating model for {symbol} using {lookback_days} days lookback")
    
    try:
        # Initialize market predictor
        predictor = MarketPredictor(db_path, checkpoint_dir)
        
        # For demonstration, use a smaller sequence length if data is limited
        if lookback_days <= 30:
            predictor.sequence_length = 5  # Override default sequence length for small datasets
            print(f"Using reduced sequence length of 5 for limited data")
        
        # Load the model for this symbol
        model_loaded = predictor._load_model(symbol=symbol)
        
        if not model_loaded:
            logger.warning(f"No specific model found for {symbol}, trying generic model")
            model_loaded = predictor._load_model()
            
            if not model_loaded:
                logger.error(f"No model available for {symbol}")
                return None
        
        # Connect to database
        conn = sqlite3.connect(db_path)
        
        # Get historical data for evaluation
        from_date = (datetime.now() - timedelta(days=lookback_days)).strftime('%Y-%m-%d')
        
        # Get market data with timestamp, OHLCV
        query = """
        SELECT md.timestamp, md.open, md.high, md.low, md.close, md.volume,
               ta.rsi, ta.macd_line as macd
        FROM market_data md
        LEFT JOIN technical_analysis ta ON md.timestamp = ta.timestamp AND md.symbol = ta.symbol
        WHERE md.symbol = ? AND md.timestamp >= ?
        ORDER BY md.timestamp ASC
        """
        
        df = pd.read_sql_query(query, conn, params=(symbol, from_date))
        
        if len(df) < predictor.sequence_length + 10:  # Need sequence length plus some test days
            logger.warning(f"Insufficient data for {symbol}: only {len(df)} records found")
            conn.close()
            return None
        
        # Calculate actual movement (target variable)
        df['returns'] = df['close'].pct_change()
        df['actual_movement'] = pd.cut(
            df['returns'], 
            bins=[-np.inf, -0.001, 0.001, np.inf],
            labels=[0, 1, 2]  # Down, Sideways, Up
        )
        df = df.dropna()
        
        # Generate predictions for each day in the test set
        predictions = []
        actuals = []
        
        # Use the last 30% of data as test set
        test_start_idx = int(len(df) * 0.7)
        
        for i in range(test_start_idx, len(df)):
            # Get historical data up to this point for prediction
            hist_data = df.iloc[i-predictor.sequence_length:i][['open', 'high', 'low', 'close', 'volume', 'rsi', 'macd']]
            
            # Make prediction
            prediction = predictor.predict(latest_data=hist_data, symbol=symbol)
            
            # Convert string prediction to numeric for confusion matrix
            pred_map = {'down': 0, 'sideways': 1, 'up': 2}
            pred_value = pred_map.get(prediction['prediction'], 1)  # Default to sideways
            
            # Store prediction
            predictions.append(pred_value)
            
            # Store actual
            actuals.append(df.iloc[i]['actual_movement'])
        
        # Calculate metrics
        if len(predictions) > 0:
            # Get confusion matrix
            cm = confusion_matrix(actuals, predictions, labels=[0, 1, 2])
            
            # Calculate metrics
            accuracy = accuracy_score(actuals, predictions)
            precision = precision_score(actuals, predictions, average='weighted', zero_division=0)
            recall = recall_score(actuals, predictions, average='weighted', zero_division=0)
            f1 = f1_score(actuals, predictions, average='weighted', zero_division=0)
            
            # Store metrics in database if requested
            if save_to_db:
                cursor = conn.cursor()
                
                # Store metrics
                cursor.execute("""
                INSERT INTO ml_model_metrics 
                (symbol, evaluation_date, accuracy, precision, recall, f1_score, 
                 data_points, lookback_days, model_version)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    symbol, 
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    float(accuracy),
                    float(precision),
                    float(recall),
                    float(f1),
                    len(predictions),
                    lookback_days,
                    '1.0'  # Version placeholder
                ))
                
                # Store confusion matrix as JSON
                cursor.execute("""
                INSERT INTO ml_confusion_matrix
                (symbol, evaluation_date, matrix_json, model_version)
                VALUES (?, ?, ?, ?)
                """, (
                    symbol,
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    json.dumps(cm.tolist()),
                    '1.0'  # Version placeholder
                ))
                
                conn.commit()
                
            conn.close()
            
            # Return metrics
            return {
                'symbol': symbol,
                'accuracy': accuracy,
                'precision': precision,
                'recall': recall,
                'f1_score': f1,
                'data_points': len(predictions),
                'confusion_matrix': cm.tolist(),
                'evaluation_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
        else:
            logger.warning(f"No predictions generated for {symbol}")
            conn.close()
            return None
    
    except Exception as e:
        logger.error(f"Error evaluating model for {symbol}: {e}")
        return None

def evaluate_all_models(db_path='market_data.db', checkpoint_dir='checkpoints', 
                       lookback_days=90, max_symbols=None):
    """
    Evaluates all available ML models
    
    Args:
        db_path: Path to database
        checkpoint_dir: Model checkpoint directory
        lookback_days: Days of historical data to evaluate
        max_symbols: Maximum number of symbols to process
    
    Returns:
        List of evaluation results
    """
    logger.info(f"Starting batch evaluation of ML models")
    
    # Connect to database and verify tables
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
    except Exception as e:
        logger.error(f"Error creating metrics tables: {e}")
        conn.close()
        sys.exit(1)
    
    # Get list of symbols with trained models
    models = []
    if os.path.exists(checkpoint_dir):
        for file in os.listdir(checkpoint_dir):
            if file.startswith('market_lstm_') and file.endswith('.pth'):
                # Extract symbol from filename
                symbol = file.replace('market_lstm_', '').replace('.pth', '')
                models.append(symbol)
    
    # If no specific models found, get symbols from database
    if not models:
        cursor.execute("""
        SELECT DISTINCT symbol FROM market_data
        WHERE symbol IS NOT NULL
        ORDER BY symbol
        """)
        models = [row[0] for row in cursor.fetchall()]
    
    conn.close()
    
    # Limit number of symbols if specified
    if max_symbols and len(models) > max_symbols:
        models = models[:max_symbols]
    
    logger.info(f"Found {len(models)} models to evaluate")
    
    # Process each model
    results = []
    for i, symbol in enumerate(models, 1):
        logger.info(f"[{i}/{len(models)}] Evaluating {symbol}")
        
        result = evaluate_model(
            symbol=symbol,
            db_path=db_path,
            checkpoint_dir=checkpoint_dir,
            lookback_days=lookback_days
        )
        
        if result:
            results.append(result)
            logger.info(f"{symbol}: Accuracy={result['accuracy']:.4f}, F1={result['f1_score']:.4f}")
        else:
            logger.warning(f"No results for {symbol}")
    
    # Summarize results
    if results:
        avg_accuracy = np.mean([r['accuracy'] for r in results])
        avg_f1 = np.mean([r['f1_score'] for r in results])
        
        logger.info(f"Evaluation complete: {len(results)} models evaluated")
        logger.info(f"Average Accuracy: {avg_accuracy:.4f}, Average F1: {avg_f1:.4f}")
    else:
        logger.warning("No models were successfully evaluated")
    
    return results

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='ML Model Performance Metrics')
    parser.add_argument('--symbol', help='Symbol to evaluate (omit for all)')
    parser.add_argument('--lookback', type=int, default=90, help='Days of historical data to evaluate')
    parser.add_argument('--database', default='market_data.db', help='Database path')
    parser.add_argument('--max', type=int, default=None, help='Maximum number of symbols to process')
    
    args = parser.parse_args()
    
    if args.symbol:
        # Evaluate single model
        result = evaluate_model(
            symbol=args.symbol,
            db_path=args.database,
            lookback_days=args.lookback
        )
        
        if result:
            print(f"\nEvaluation results for {args.symbol}:")
            print(f"- Accuracy: {result['accuracy']:.4f}")
            print(f"- Precision: {result['precision']:.4f}")
            print(f"- Recall: {result['recall']:.4f}")
            print(f"- F1 Score: {result['f1_score']:.4f}")
            print(f"- Data points: {result['data_points']}")
        else:
            print(f"\nEvaluation failed for {args.symbol}")
    
    else:
        # Evaluate all models
        results = evaluate_all_models(
            db_path=args.database,
            lookback_days=args.lookback,
            max_symbols=args.max
        )
        
        if results:
            # Display top 5 models
            print("\nTop 5 models by accuracy:")
            sorted_results = sorted(results, key=lambda x: x['accuracy'], reverse=True)
            for i, result in enumerate(sorted_results[:5], 1):
                print(f"{i}. {result['symbol']}: Accuracy={result['accuracy']:.4f}, F1={result['f1_score']:.4f}")
        else:
            print("\nNo models were successfully evaluated")
