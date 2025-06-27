"""
Enhanced Batch Pipeline Integration

This script integrates the batch data collector into the existing processing pipeline.
It collects market data using batch downloads, runs technical analysis, and generates signals.
"""

import pandas as pd
import subprocess
import os
import sys
import logging
import time
import importlib.util
import sqlite3
import traceback
from datetime import datetime

# Configure logging
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)

# Configure file handler
file_handler = logging.FileHandler(os.path.join(log_dir, 'batch_pipeline.log'))
file_handler.setLevel(logging.INFO)
file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(file_formatter)

# Configure console handler
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler.setFormatter(console_formatter)

# Set up logger
logger = logging.getLogger('BatchPipeline')
logger.setLevel(logging.INFO)
logger.addHandler(file_handler)
logger.addHandler(console_handler)
logger.propagate = False  # Prevent propagation to root logger

# Import the batch collector
from batch_data_collector import BatchDataCollector
import symbol_mapping  # Import the symbol mapping module

# Use the correct Python executable for the environment
PYTHON_EXEC = os.path.join(os.getcwd(), 'trading_env', 'Scripts', 'python.exe') if os.path.exists(os.path.join(os.getcwd(), 'trading_env', 'Scripts', 'python.exe')) else 'python'

def import_module_from_file(module_name, file_path):
    """Import a module from file path."""
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    if spec is None:
        logger.error(f"Could not load specification for {module_name} from {file_path}")
        return None
        
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    try:
        spec.loader.exec_module(module)
        return module
    except Exception as e:
        logger.error(f"Error loading module {module_name}: {e}")
        logger.error(traceback.format_exc())
        return None

def load_symbols():
    """Load all symbols from stocks.csv"""
    try:
        # Load all symbols from stocks.csv (semicolon separated)
        stocks_df = pd.read_csv('stocks.csv', sep=';')
        symbols = stocks_df['Symbol'].dropna().unique().tolist()

        # Add DAX and S&P500 indices explicitly
        symbols += ['^GDAXI', '^GSPC']

        # Remove duplicates and sort
        symbols = sorted(set(symbols))
        
        return symbols
    except Exception as e:
        logger.error(f"Error loading symbols: {e}")
        return ['AAPL', 'MSFT', 'AMZN', 'GOOGL', 'META', '^GDAXI', '^GSPC']  # Default symbols

def run_batch_collection(symbols, interval='1h', period='1d', batch_size=40):
    """Run batch collection for all symbols"""
    logger.info(f"Starting batch collection for {len(symbols)} symbols")
    
    # Initialize collector with specified batch size
    collector = BatchDataCollector(max_batch_size=batch_size)
    
    try:
        # Collect data
        success, total_collected = collector.collect_data(symbols, interval, period)
        
        if success:
            logger.info(f"Batch collection successful: {total_collected}/{len(symbols)} symbols")
        else:
            logger.warning(f"Batch collection completed with issues: {total_collected}/{len(symbols)} symbols")
        
        # Close database connection
        collector.close()
        
        # Return symbols that were successfully collected
        return success, total_collected
    except Exception as e:
        logger.error(f"Error in batch collection: {e}")
        logger.error(traceback.format_exc())
        if collector:
            collector.close()
        return False, 0

def run_technical_analysis_for_symbols(symbols):
    """Run technical analysis using subprocess for compatibility"""
    logger.info(f"Running technical analysis for {len(symbols)} symbols")
    
    success_count = 0
    for symbol in symbols:
        logger.info(f"Running technical analysis for {symbol}")
        result = subprocess.run(
            [PYTHON_EXEC, 'technical_analyzer.py', '--symbol', symbol], 
            capture_output=True, 
            text=True
        )
        
        if result.returncode == 0 and 'ERROR' not in result.stdout:
            logger.info(f"Technical analysis completed for {symbol}")
            success_count += 1
        else:
            logger.error(f"Technical analysis failed for {symbol}: {result.stderr}")
    
    logger.info(f"Technical analysis completed for {success_count}/{len(symbols)} symbols")
    return success_count > 0

def run_signal_generation_for_symbols(symbols):
    """Run signal generation using subprocess for compatibility"""
    logger.info(f"Running signal generation for {len(symbols)} symbols")
    
    success_count = 0
    for symbol in symbols:
        logger.info(f"Generating signals for {symbol}")
        result = subprocess.run(
            [PYTHON_EXEC, 'signal_generator.py', '--symbol', symbol], 
            capture_output=True, 
            text=True
        )
        
        if result.returncode == 0 and 'ERROR' not in result.stdout:
            logger.info(f"Signal generation completed for {symbol}")
            success_count += 1
        else:
            logger.error(f"Signal generation failed for {symbol}: {result.stderr}")
    
    logger.info(f"Signal generation completed for {success_count}/{len(symbols)} symbols")
    return success_count > 0

def get_symbols_with_data():
    """Get list of symbols with data in the database"""
    conn = sqlite3.connect('market_data.db')
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT symbol FROM market_data")
    symbols = [row[0] for row in cursor.fetchall()]
    conn.close()
    return symbols

def legacy_pipeline():
    """Run the original pipeline for individual symbols"""
    symbols = load_symbols()
    
    # Track stocks with and without data
    delivered = []
    no_data = []
    total = len(symbols)

    for symbol in symbols:
        print(f"\n=== Processing {symbol} ===")
        # 1. Data collection
        result1 = subprocess.run([PYTHON_EXEC, 'data_collector.py', '--symbol', symbol], capture_output=True, text=True)
        print(result1.stdout)
        if result1.returncode != 0 or 'No data for' in result1.stdout:
            print(f"[ERROR] Data collection failed for {symbol}: {result1.stderr}")
            no_data.append(symbol)
            continue
        delivered.append(symbol)
        # 2. Technical analysis
        result2 = subprocess.run([PYTHON_EXEC, 'technical_analyzer.py', '--symbol', symbol], capture_output=True, text=True)
        print(result2.stdout)
        if result2.returncode != 0:
            print(f"[ERROR] Technical analysis failed for {symbol}: {result2.stderr}")
            continue
        # 3. Signal generation
        result3 = subprocess.run([PYTHON_EXEC, 'signal_generator.py', '--symbol', symbol], capture_output=True, text=True)
        print(result3.stdout)
        if result3.returncode != 0:
            print(f"[ERROR] Signal generation failed for {symbol}: {result3.stderr}")
            continue
        print(f"[SUCCESS] Pipeline completed for {symbol}")

    # Print summary at the end
    print(f"\n---\nData delivered for {len(delivered)} out of {total} symbols.")
    if no_data:
        print("No data for the following symbols:")
        for s in no_data:
            print(s)
    else:
        print("All symbols received data.")

def enhanced_pipeline(mode="all", interval="1h", period="1d", batch_size=40):
    """
    Run the enhanced pipeline with batch data collection
    
    Args:
        mode (str): "all" for processing all symbols, "key" for key symbols only
        interval (str): Data interval (1m, 5m, 15m, 30m, 1h, 1d)
        period (str): Period to download (1d, 5d, 1mo, 3mo)
        batch_size (int): Number of symbols per batch
    """
    start_time = time.time()
    logger.info(f"Starting enhanced batch pipeline with mode={mode}, interval={interval}, period={period}")
    
    # Load symbols based on mode
    all_symbols = load_symbols()
    
    if mode == "key":
        # Key symbols only (important stocks and indices)
        symbols = ['AAPL', 'MSFT', 'AMZN', 'GOOGL', 'META', 'TSLA', 'NVDA', '^GSPC', '^DJI', '^IXIC', '^GDAXI']
        logger.info(f"Running in 'key' mode with {len(symbols)} key symbols")
    else:
        # All symbols
        symbols = all_symbols
        logger.info(f"Running in 'all' mode with {len(symbols)} symbols")
    
    # Step 1: Batch Data Collection
    logger.info("STEP 1: Batch Data Collection")
    collection_success, collected_count = run_batch_collection(symbols, interval, period, batch_size)
    
    if not collection_success:
        logger.error("Batch data collection failed, pipeline stopping")
        return False
    
    # Get list of symbols that have data after collection
    symbols_with_data = get_symbols_with_data()
    symbols_with_data = [s for s in symbols_with_data if s in symbols]
    logger.info(f"Found {len(symbols_with_data)} symbols with data in database")
    
    if len(symbols_with_data) == 0:
        logger.error("No symbols with data, pipeline stopping")
        return False
    
    # Step 2: Technical Analysis
    logger.info("STEP 2: Technical Analysis")
    ta_success = run_technical_analysis_for_symbols(symbols_with_data)
    
    if not ta_success:
        logger.error("Technical analysis failed, pipeline stopping")
        return False
    
    # Step 3: Signal Generation
    logger.info("STEP 3: Signal Generation")
    sg_success = run_signal_generation_for_symbols(symbols_with_data)
    
    # Calculate elapsed time
    elapsed_time = time.time() - start_time
    logger.info(f"Enhanced pipeline execution completed in {elapsed_time:.2f} seconds")
    
    # Print summary
    logger.info(f"Summary: {collected_count}/{len(symbols)} symbols collected")
    logger.info(f"Processing completed for {len(symbols_with_data)} symbols")
    
    return collection_success and ta_success

def main():
    """Main function with command line options"""
    import argparse
    
    # Parse arguments
    parser = argparse.ArgumentParser(description='Run the data processing pipeline.')
    parser.add_argument('--mode', choices=['legacy', 'enhanced'], default='enhanced',
                      help='Pipeline mode: legacy (individual processing) or enhanced (batch processing)')
    parser.add_argument('--symbols', choices=['all', 'key'], default='key',
                      help='Symbols to process: all or key symbols only')
    parser.add_argument('--interval', default='1h', 
                      help='Data interval for batch collection (1m, 5m, 15m, 30m, 1h, 1d)')
    parser.add_argument('--period', default='1d',
                      help='Period to download (1d, 5d, 1mo, 3mo)')
    parser.add_argument('--batch-size', type=int, default=40,
                      help='Number of symbols per batch')
    
    args = parser.parse_args()
    
    if args.mode == 'legacy':
        logger.info("Running legacy pipeline (individual symbol processing)")
        legacy_pipeline()
    else:
        logger.info(f"Running enhanced pipeline with {args.symbols} symbols, interval={args.interval}, period={args.period}")
        enhanced_pipeline(args.symbols, args.interval, args.period, args.batch_size)

if __name__ == "__main__":
    main()
