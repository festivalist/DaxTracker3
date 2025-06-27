"""
Script to generate trading signals for all available symbols in the database.
This will ensure the dashboard can display data for all symbols.
"""
import sqlite3
import pandas as pd
from signal_generator import SignalGenerator
import logging

logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

logger = logging.getLogger(__name__)

def get_all_symbols_with_data():
    """Get all symbols that have market data available."""
    conn = sqlite3.connect('market_data.db')
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT symbol FROM market_data")
    symbols = [row[0] for row in cursor.fetchall()]
    conn.close()
    return symbols

def generate_signals_for_all():
    """Generate trading signals for all symbols with market data."""    symbols = get_all_symbols_with_data()
    logger.info(f"Found {len(symbols)} symbols with market data")
    signal_gen = SignalGenerator(db_path='market_data.db')
    success_count = 0
    
    for symbol in symbols:
        try:
            logger.info(f"Generating signals for {symbol}")
            signal_gen.generate_signals(symbol)
            success_count += 1
            logger.info(f"Successfully generated signals for {symbol}")
        except Exception as e:
            logger.error(f"Failed to generate signals for {symbol}: {e}")
    
    logger.info(f"Successfully generated signals for {success_count} out of {len(symbols)} symbols")

if __name__ == "__main__":
    generate_signals_for_all()
