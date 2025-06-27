"""
Generate trading signals for all symbols with relaxed validation.

This script is designed to generate signals for all symbols in the database,
using relaxed validation criteria to ensure most symbols get signals.
"""

import sqlite3
import logging
import sys
import argparse

from signal_generator import SignalGenerator

# Parse command line arguments
parser = argparse.ArgumentParser(description="Generate trading signals with relaxed validation")
parser.add_argument("--single-symbol", help="Process only a single symbol", default=None)
args = parser.parse_args()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('generate_all_signals_relaxed.log')
    ]
)
logger = logging.getLogger(__name__)

def get_all_symbols():
    """Get all symbols from market_data table."""
    conn = sqlite3.connect('market_data.db')
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT symbol FROM market_data")
    symbols = [row[0] for row in cursor.fetchall()]
    conn.close()
    logger.info(f"Found {len(symbols)} symbols with market data")
    return symbols

def check_signal_counts():
    """Check how many symbols have signals."""
    conn = sqlite3.connect('market_data.db')
    cursor = conn.cursor()
    
    # Count symbols in market_data
    cursor.execute("SELECT COUNT(DISTINCT symbol) FROM market_data")
    market_data_count = cursor.fetchone()[0]
    
    # Count symbols in trading_signals
    cursor.execute("SELECT COUNT(DISTINCT symbol) FROM trading_signals")
    signal_count = cursor.fetchone()[0]
    
    # Get signals breakdown
    cursor.execute("""
        SELECT signal_type, COUNT(*) 
        FROM trading_signals 
        GROUP BY signal_type
        ORDER BY COUNT(*) DESC
    """)
    signal_types = cursor.fetchall()
    
    # Get symbol breakdown
    cursor.execute("""
        SELECT symbol, COUNT(*) 
        FROM trading_signals 
        GROUP BY symbol
        ORDER BY COUNT(*) DESC
        LIMIT 10
    """)
    top_symbols = cursor.fetchall()
    
    conn.close()
    
    logger.info(f"Market data table has {market_data_count} symbols")
    logger.info(f"Trading signals table has {signal_count} symbols")
    logger.info("Signal type counts:")
    for signal_type, count in signal_types:
        logger.info(f"  {signal_type}: {count}")
    logger.info("Top 10 symbols by signal count:")
    for symbol, count in top_symbols:
        logger.info(f"  {symbol}: {count}")
    
    return market_data_count, signal_count

def ensure_tables_exist():
    """Create required tables if they don't exist."""
    conn = sqlite3.connect('market_data.db')
    cursor = conn.cursor()
    
    # Create trading_signals table if it doesn't exist
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS trading_signals (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol TEXT NOT NULL,
        timestamp DATETIME NOT NULL,
        signal_type TEXT NOT NULL,
        confidence REAL,
        close_price REAL,
        technical_signal TEXT,
        sentiment_signal TEXT,
        reason TEXT,
        notified INTEGER DEFAULT 0,
        verified INTEGER DEFAULT 0,
        outcome TEXT
    )
    """)
    
    # Create technical_analysis table if it doesn't exist
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS technical_analysis (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol TEXT NOT NULL,
        timestamp DATETIME NOT NULL,
        close_price REAL,
        sma_20 REAL,
        sma_50 REAL,
        rsi REAL,
        macd_line REAL,
        signal_line REAL,
        overall_signal TEXT
    )
    """)
    
    conn.commit()
    conn.close()
    logger.info("Database tables verified/created successfully")

def main():
    """Main function to generate signals for all symbols."""
    # Ensure necessary tables exist
    ensure_tables_exist()
    
    if args.single_symbol:
        logger.info(f"Starting relaxed signal generation for symbol: {args.single_symbol}")
        symbols = [args.single_symbol]
    else:
        logger.info("Starting relaxed signal generation for all symbols")
        # Get all symbols
        symbols = get_all_symbols()
    
    # Get initial counts
    market_data_count, initial_signal_count = check_signal_counts()
    
    # Create SignalGenerator with very relaxed max_data_age_hours (30 days)
    signal_generator = SignalGenerator(db_path='market_data.db', confidence_threshold=0.5, max_data_age_hours=720)
    
    # Process symbols in batches
    batch_size = 10
    success_count = 0
    error_count = 0
    no_signal_count = 0
    
    for i in range(0, len(symbols), batch_size):
        batch = symbols[i:i+batch_size]
        logger.info(f"Processing batch of {len(batch)} symbols ({i+1}-{i+len(batch)} of {len(symbols)})")
        
        try:
            signals = signal_generator.generate_signals(batch)
            if signals:
                success_count += len(signals)
                logger.info(f"Generated {len(signals)} signals in batch")
            else:
                no_signal_count += len(batch)
                logger.info("No signals generated in batch")
        except Exception as e:
            error_count += len(batch)
            logger.error(f"Error processing batch: {e}")
    
    # Get final counts
    _, final_signal_count = check_signal_counts()
    
    logger.info("Signal generation complete")
    logger.info(f"Processed {len(symbols)} symbols")
    logger.info(f"Successful signals: {success_count}")
    logger.info(f"No signals generated: {no_signal_count}")
    logger.info(f"Errors: {error_count}")
    logger.info(f"Symbols with signals: {final_signal_count} (was {initial_signal_count})")
    logger.info(f"New symbols with signals: {final_signal_count - initial_signal_count}")

if __name__ == "__main__":
    main()
