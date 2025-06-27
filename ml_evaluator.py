"""
ML Model Evaluator for Market Predictor

This module provides tools to evaluate the performance of trained ML models,
visualize predictions versus actual outcomes, and generate performance metrics.
"""

import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import sqlite3
import logging
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, confusion_matrix
import seaborn as sns
from datetime import datetime, timedelta
from market_predictor import MarketPredictor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    handlers=[
        logging.FileHandler("ml_evaluation.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("MLEvaluator")


class ModelEvaluator:
    """Evaluates ML model performance on historical data"""
    
    def __init__(self, db_path='market_data.db', checkpoint_dir='checkpoints'):
        """
        Initialize the model evaluator
        
        Args:
            db_path: Path to the market data database
            checkpoint_dir: Directory for model checkpoints
        """
        self.db_path = db_path
        self.checkpoint_dir = checkpoint_dir
        self.reports_dir = os.path.join(checkpoint_dir, 'evaluation_reports')
        os.makedirs(self.reports_dir, exist_ok=True)
        
        # Get predictor instance
        self.predictor = MarketPredictor(db_path, checkpoint_dir)
    
    def get_test_data(self, symbol=None, start_date=None, end_date=None, test_size=0.2):
        """
        Get test data for model evaluation
        
        Args:
            symbol: Symbol to test (None for all available symbols)
            start_date: Start date for test period (str 'YYYY-MM-DD')
            end_date: End date for test period (str 'YYYY-MM-DD')
            test_size: Proportion of data to use for testing if dates not specified
            
        Returns:
            DataFrame with test data and actual outcomes
        """
        try:
            conn = sqlite3.connect(self.db_path)
            
            # Build query based on parameters
            query_parts = [
                """
                SELECT md.timestamp, md.symbol, md.open, md.high, md.low, md.close, md.volume,
                       ta.rsi_14 as rsi, ta.macd_line as macd,
                       LEAD(md.close, 1) OVER (PARTITION BY md.symbol ORDER BY md.timestamp) as next_close
                FROM market_data md
                LEFT JOIN technical_analysis ta ON md.timestamp = ta.timestamp AND md.symbol = ta.symbol
                WHERE 1=1
                """
            ]
            params = []
            
            # Add symbol filter if specified
            if symbol:
                query_parts.append("AND md.symbol = ?")
                params.append(symbol)
            
            # Add date range if specified
            if start_date:
                query_parts.append("AND md.timestamp >= ?")
                params.append(start_date)
            if end_date:
                query_parts.append("AND md.timestamp <= ?")
                params.append(end_date)
            
            # Complete the query
            query_parts.append("ORDER BY md.symbol, md.timestamp")
            query = " ".join(query_parts)
            
            # Execute query
            df = pd.read_sql_query(query, conn, params=params)
            conn.close()
            
            # Calculate returns and movement labels
            df['returns'] = (df['next_close'] - df['close']) / df['close']
            df['actual_movement'] = pd.cut(
                df['returns'], 
                bins=[-np.inf, -0.001, 0.001, np.inf],
                labels=['down', 'sideways', 'up']
            )
            
            # Drop rows where we can't calculate returns (last day for each symbol)
            df = df.dropna(subset=['returns', 'actual_movement'])
            
            # If no date range specified, use test_size to split
            if not start_date and not end_date:
                symbols = df['symbol'].unique()
                test_data = pd.DataFrame()
                
                for sym in symbols:
                    sym_data = df[df['symbol'] == sym]
                    test_start_idx = int(len(sym_data) * (1 - test_size))
                    sym_test = sym_data.iloc[test_start_idx:]
                    test_data = pd.concat([test_data, sym_test])
                
                df = test_data
            
            return df
            
        except Exception as e:
            logger.error(f"Error getting test data: {e}")
            return pd.DataFrame()
    
    def evaluate_model(self, symbol=None, start_date=None, end_date=None, save_report=True):
        """
        Evaluate model on historical data
        
        Args:
            symbol: Symbol to evaluate (None for all)
            start_date: Start date for evaluation
            end_date: End date for evaluation
            save_report: Whether to save evaluation report
            
        Returns:
            Dict with evaluation metrics
        """
        # Get test data
        test_data = self.get_test_data(symbol, start_date, end_date)
        if test_data.empty:
            logger.error("No test data available for evaluation")
            return None
        
        logger.info(f"Evaluating model on {len(test_data)} data points")
        
        # Generate predictions for each symbol
        predictions = []
        symbols = test_data['symbol'].unique()
        
        for sym in symbols:
            logger.info(f"Evaluating symbol: {sym}")
            sym_data = test_data[test_data['symbol'] == sym]
            
            # We need a sequence_length history to make a prediction
            # so we'll use predictor.predict() on each day's data
            sequence_length = self.predictor.sequence_length
            
            for i in range(len(sym_data)):
                try:
                    # Get the current timestamp to predict
                    current_ts = sym_data.iloc[i]['timestamp']
                    
                    # Query for sequence_length days of data up to this timestamp
                    conn = sqlite3.connect(self.db_path)
                    features_query = """
                    SELECT timestamp, open, high, low, close, volume,
                           ta.rsi_14 as rsi, ta.macd_line as macd
                    FROM market_data md
                    LEFT JOIN technical_analysis ta ON md.timestamp = ta.timestamp AND md.symbol = ta.symbol
                    WHERE md.symbol = ? AND md.timestamp <= ?
                    ORDER BY md.timestamp DESC
                    LIMIT ?
                    """
                    
                    history_data = pd.read_sql_query(
                        features_query, 
                        conn, 
                        params=(sym, current_ts, sequence_length)
                    )
                    conn.close()
                    
                    # If we have enough history, make a prediction
                    if len(history_data) == sequence_length:
                        history_data = history_data.iloc[::-1]  # Reorder to ascending
                        pred = self.predictor.predict(sym, history_data)
                        
                        if pred and 'prediction' in pred:
                            predictions.append({
                                'symbol': sym,
                                'timestamp': current_ts,
                                'predicted': pred['prediction'],
                                'confidence': pred['confidence'],
                                'actual': sym_data.iloc[i]['actual_movement']
                            })
                except Exception as e:
                    logger.error(f"Error predicting for {sym} at {current_ts}: {e}")
                    continue
        
        # Convert predictions to DataFrame
        if not predictions:
            logger.error("No predictions could be generated for evaluation")
            return None
            
        pred_df = pd.DataFrame(predictions)
        
        # Calculate metrics
        metrics = self._calculate_metrics(pred_df)
        
        # Generate and save report if requested
        if save_report and metrics:
            self._save_evaluation_report(pred_df, metrics, symbol)
        
        return metrics
    
    def _calculate_metrics(self, predictions_df):
        """Calculate evaluation metrics from predictions"""
        try:
            # Overall metrics
            accuracy = accuracy_score(predictions_df['actual'], predictions_df['predicted'])
            precision = precision_score(
                predictions_df['actual'], 
                predictions_df['predicted'],
                labels=['up', 'sideways', 'down'], 
                average='weighted'
            )
            recall = recall_score(
                predictions_df['actual'], 
                predictions_df['predicted'],
                labels=['up', 'sideways', 'down'], 
                average='weighted'
            )
            f1 = f1_score(
                predictions_df['actual'], 
                predictions_df['predicted'],
                labels=['up', 'sideways', 'down'], 
                average='weighted'
            )
            
            # Confusion matrix
            cm = confusion_matrix(
                predictions_df['actual'], 
                predictions_df['predicted'],
                labels=['up', 'sideways', 'down']
            )
            
            # Metrics per symbol
            per_symbol = {}
            for symbol in predictions_df['symbol'].unique():
                sym_df = predictions_df[predictions_df['symbol'] == symbol]
                sym_accuracy = accuracy_score(sym_df['actual'], sym_df['predicted'])
                per_symbol[symbol] = {
                    'accuracy': sym_accuracy,
                    'sample_count': len(sym_df)
                }
            
            # Compose metrics dictionary
            metrics = {
                'overall': {
                    'accuracy': accuracy,
                    'precision': precision,
                    'recall': recall,
                    'f1': f1,
                    'sample_count': len(predictions_df)
                },
                'confusion_matrix': cm,
                'per_symbol': per_symbol
            }
            
            return metrics
            
        except Exception as e:
            logger.error(f"Error calculating metrics: {e}")
            return None
    
    def _save_evaluation_report(self, predictions_df, metrics, symbol=None):
        """Save evaluation report to disk"""
        try:
            # Create timestamp for report
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            symbol_str = symbol if symbol else 'all_symbols'
            report_file = os.path.join(self.reports_dir, f'eval_{symbol_str}_{timestamp}')
            
            # Save metrics to text file
            with open(f'{report_file}.txt', 'w') as f:
                f.write(f"MODEL EVALUATION REPORT - {timestamp}\n")
                f.write(f"Symbol(s): {symbol if symbol else 'All Symbols'}\n\n")
                
                f.write("OVERALL METRICS:\n")
                f.write(f"Accuracy: {metrics['overall']['accuracy']:.4f}\n")
                f.write(f"Precision: {metrics['overall']['precision']:.4f}\n")
                f.write(f"Recall: {metrics['overall']['recall']:.4f}\n")
                f.write(f"F1 Score: {metrics['overall']['f1']:.4f}\n")
                f.write(f"Sample Count: {metrics['overall']['sample_count']}\n\n")
                
                f.write("PER-SYMBOL METRICS:\n")
                for sym, sym_metrics in metrics['per_symbol'].items():
                    f.write(f"{sym}: Accuracy = {sym_metrics['accuracy']:.4f} ")
                    f.write(f"(Samples: {sym_metrics['sample_count']})\n")
            
            # Generate and save confusion matrix plot
            plt.figure(figsize=(10, 8))
            sns.heatmap(
                metrics['confusion_matrix'],
                annot=True,
                fmt='d',
                cmap='Blues',
                xticklabels=['Up', 'Sideways', 'Down'],
                yticklabels=['Up', 'Sideways', 'Down']
            )
            plt.xlabel('Predicted')
            plt.ylabel('Actual')
            plt.title('Confusion Matrix')
            plt.tight_layout()
            plt.savefig(f'{report_file}_confusion.png')
            plt.close()
            
            # Save predictions to CSV for further analysis
            predictions_df.to_csv(f'{report_file}_predictions.csv', index=False)
            
            logger.info(f"Saved evaluation report to {report_file}")
            return True
            
        except Exception as e:
            logger.error(f"Error saving evaluation report: {e}")
            return False


def main():
    """Run a model evaluation"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Evaluate ML model performance')
    parser.add_argument('--db_path', type=str, default='market_data.db', help='Path to database')
    parser.add_argument('--checkpoint_dir', type=str, default='checkpoints', help='Model checkpoint directory')
    parser.add_argument('--symbol', type=str, default=None, help='Symbol to evaluate (default: all)')
    parser.add_argument('--start_date', type=str, default=None, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end_date', type=str, default=None, help='End date (YYYY-MM-DD)')
    
    args = parser.parse_args()
    
    evaluator = ModelEvaluator(args.db_path, args.checkpoint_dir)
    metrics = evaluator.evaluate_model(args.symbol, args.start_date, args.end_date)
    
    if metrics:
        print(f"\nEvaluation Results for {args.symbol if args.symbol else 'all symbols'}:")
        print(f"Accuracy: {metrics['overall']['accuracy']:.4f}")
        print(f"Precision: {metrics['overall']['precision']:.4f}")
        print(f"Recall: {metrics['overall']['recall']:.4f}")
        print(f"F1 Score: {metrics['overall']['f1']:.4f}")
        print(f"Sample Count: {metrics['overall']['sample_count']}")
        return 0
    else:
        print("Evaluation failed. Check logs for details.")
        return 1


if __name__ == "__main__":
    main()
