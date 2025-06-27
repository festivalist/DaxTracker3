"""
ML Model Training Script for Market Predictor

This script trains the LSTM model for market prediction and saves the checkpoint.
Run this script to update your model based on the latest market data.
"""

import os
import argparse
import logging
from market_predictor import MarketPredictor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    handlers=[
        logging.FileHandler("ml_training.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("TrainML")

def main(args):
    logger.info(f"Starting ML model training for symbol: {args.symbol if args.symbol else 'all symbols'}")
    
    # Initialize the market predictor
    predictor = MarketPredictor(
        db_path=args.db_path,
        checkpoint_dir=args.checkpoint_dir,
        sequence_length=args.sequence_length
    )
    
    # Train the model
    logger.info(f"Training model with {args.epochs} epochs and batch size {args.batch_size}")
    try:
        predictor.train(
            symbol=args.symbol,  # None will use all available symbols
            epochs=args.epochs,
            batch_size=args.batch_size,
            validation_split=args.validation_split
        )
        logger.info("Model training complete")
    except Exception as e:
        logger.error(f"Error during model training: {e}")
        return 1
    
    # Test the model on a symbol
    test_symbol = args.symbol or "DAX"
    logger.info(f"Testing model prediction for {test_symbol}")
    prediction = predictor.predict(test_symbol)
    
    if prediction:
        logger.info(f"Prediction for {test_symbol}: {prediction['prediction']} with confidence {prediction['confidence']:.2f}")
    else:
        logger.warning(f"Could not generate prediction for {test_symbol}")
    
    return 0

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Train the ML market prediction model')
    parser.add_argument('--db_path', type=str, default='market_data.db', help='Path to the market data database')
    parser.add_argument('--checkpoint_dir', type=str, default='checkpoints', help='Directory for model checkpoints')
    parser.add_argument('--symbol', type=str, default=None, help='Symbol to train on (default: all)')
    parser.add_argument('--epochs', type=int, default=50, help='Number of training epochs')
    parser.add_argument('--batch_size', type=int, default=32, help='Training batch size')
    parser.add_argument('--sequence_length', type=int, default=60, help='Sequence length for LSTM')
    parser.add_argument('--validation_split', type=float, default=0.2, help='Validation data split ratio')
    
    args = parser.parse_args()
    exit(main(args))
