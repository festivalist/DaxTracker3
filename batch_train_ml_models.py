"""
Batch ML Model Training Script

This script trains ML models for all or selected symbols in the database.
It supports training with various configurations and validation periods.
"""

import os
import sys
import time
import sqlite3
import logging
import argparse
from datetime import datetime, timedelta
import pandas as pd
from market_predictor import MarketPredictor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] BatchTrainer: %(message)s',
    filename='ml_training.log'
)
logger = logging.getLogger('BatchTrainer')
console = logging.StreamHandler()
console.setLevel(logging.INFO)
logger.addHandler(console)

class BatchMLTrainer:
    """Trains ML models for multiple symbols."""
    
    def __init__(self, db_path='market_data.db', checkpoint_dir='checkpoints'):
        """
        Initialize the batch trainer.
        
        Args:
            db_path: Path to SQLite database with market data
            checkpoint_dir: Directory for model checkpoints
        """
        self.db_path = db_path
        self.checkpoint_dir = checkpoint_dir
        
        # Create checkpoints directory if it doesn't exist
        os.makedirs(self.checkpoint_dir, exist_ok=True)
        
        # Get available symbols
        self.symbols = self._get_available_symbols()
        logger.info(f"Found {len(self.symbols)} symbols with data")
    
    def _get_available_symbols(self):
        """Get list of symbols with sufficient data for training."""
        try:
            conn = sqlite3.connect(self.db_path)
            query = """
            SELECT DISTINCT symbol 
            FROM market_data
            GROUP BY symbol 
            HAVING COUNT(*) >= 120
            ORDER BY symbol
            """
            
            df = pd.read_sql_query(query, conn)
            conn.close()
            
            return df['symbol'].tolist()
        except Exception as e:
            logger.error(f"Error getting available symbols: {e}")
            return []
    
    def train_symbol(self, symbol, epochs=50, validation_split=0.2):
        """
        Train model for a specific symbol.
        
        Args:
            symbol: Stock symbol to train for
            epochs: Number of training epochs
            validation_split: Fraction of data to use for validation
            
        Returns:
            dict with training results and metrics
        """
        logger.info(f"Training model for {symbol}...")
        
        try:
            # Initialize predictor for this symbol
            predictor = MarketPredictor(self.db_path, self.checkpoint_dir)
            
            # Train the model
            start_time = time.time()
            result = predictor.train(symbol=symbol, epochs=epochs, 
                                    validation_split=validation_split)
            
            elapsed = time.time() - start_time
            
            if result and isinstance(result, dict):
                accuracy = result.get('validation_accuracy', 0)
                logger.info(f"Model for {symbol} trained successfully. "
                           f"Validation accuracy: {accuracy:.2f}, Time: {elapsed:.1f}s")
                return {
                    'symbol': symbol,
                    'success': True,
                    'accuracy': accuracy,
                    'training_time': elapsed,
                    'epochs': epochs,
                    **result
                }
            else:
                logger.warning(f"Training for {symbol} completed but no metrics returned")
                return {
                    'symbol': symbol,
                    'success': True,
                    'training_time': elapsed,
                    'epochs': epochs
                }
                
        except Exception as e:
            logger.error(f"Error training model for {symbol}: {e}")
            return {
                'symbol': symbol,
                'success': False,
                'error': str(e)
            }
    
    def batch_train(self, symbols=None, max_symbols=None, min_data_points=200, epochs=50):
        """
        Train models for multiple symbols.
        
        Args:
            symbols: List of specific symbols to train (None for all)
            max_symbols: Maximum number of symbols to process
            min_data_points: Minimum required data points
            epochs: Number of training epochs per model
            
        Returns:
            list of training results by symbol
        """
        # If no symbols specified, use all available
        if symbols is None:
            symbols = self.symbols
        
        # Filter symbols by data availability
        if min_data_points > 0:
            filtered_symbols = []
            conn = sqlite3.connect(self.db_path)
            
            for symbol in symbols:
                query = f"SELECT COUNT(*) FROM market_data WHERE symbol='{symbol}'"
                cursor = conn.cursor()
                cursor.execute(query)
                count = cursor.fetchone()[0]
                
                if count >= min_data_points:
                    filtered_symbols.append(symbol)
                else:
                    logger.warning(f"Skipping {symbol}: insufficient data ({count} < {min_data_points})")
            
            conn.close()
            symbols = filtered_symbols
        
        # Limit number of symbols if specified
        if max_symbols and len(symbols) > max_symbols:
            logger.info(f"Limiting training to {max_symbols} symbols")
            symbols = symbols[:max_symbols]
        
        # Initialize results storage
        results = []
        start_time = time.time()
        total = len(symbols)
        
        logger.info(f"Starting batch training for {total} symbols")
        
        # Train models for each symbol
        for i, symbol in enumerate(symbols, 1):
            logger.info(f"[{i}/{total}] Training model for {symbol}")
            
            result = self.train_symbol(symbol, epochs=epochs)
            results.append(result)
            
            # Progress update
            elapsed = time.time() - start_time
            avg_time = elapsed / i
            remaining = avg_time * (total - i)
            
            logger.info(f"Progress: {i}/{total} ({i/total*100:.1f}%), "
                       f"Elapsed: {elapsed:.1f}s, Remaining: {remaining:.1f}s")
        
        # Summarize results
        success_count = sum(1 for r in results if r.get('success', False))
        if success_count > 0:
            mean_accuracy = sum(r.get('accuracy', 0) for r in results if r.get('success', False)) / success_count
        else:
            mean_accuracy = 0
            
        logger.info(f"Batch training completed: {success_count}/{total} successful, "
                   f"Mean accuracy: {mean_accuracy:.4f}")
        
        return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Batch Train ML Models")
    parser.add_argument("--symbols", help="Comma-separated list of symbols to train")
    parser.add_argument("--max", type=int, default=None, help="Maximum number of symbols to train")
    parser.add_argument("--min-data", type=int, default=200, 
                      help="Minimum data points required for training")
    parser.add_argument("--epochs", type=int, default=50, help="Training epochs per model")
    parser.add_argument("--database", default="market_data.db", help="Database file path")
    
    args = parser.parse_args()
    
    # Create trainer
    trainer = BatchMLTrainer(db_path=args.database)
    
    # Get list of symbols if specified
    symbols = None
    if args.symbols:
        symbols = [s.strip() for s in args.symbols.split(',')]
    
    # Start batch training
    results = trainer.batch_train(
        symbols=symbols, 
        max_symbols=args.max, 
        min_data_points=args.min_data, 
        epochs=args.epochs
    )
    
    # Count successful models
    success_count = sum(1 for r in results if r.get('success', False))
    
    print(f"\nTraining Summary:")
    print(f"- Models trained successfully: {success_count} / {len(results)}")
    
    if success_count > 0:
        # Calculate average metrics
        avg_accuracy = sum(r.get('accuracy', 0) for r in results if r.get('success', False)) / success_count
        print(f"- Average validation accuracy: {avg_accuracy:.4f}")
        
        # Find best performing model
        best_model = max((r for r in results if r.get('success', False)), 
                        key=lambda x: x.get('accuracy', 0), 
                        default=None)
        
        if best_model:
            print(f"- Best model: {best_model['symbol']}, accuracy: {best_model['accuracy']:.4f}")
