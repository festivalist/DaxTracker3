"""
Scheduler for batch data collection.
This script schedules batch collection of market data at specified intervals.
"""

import schedule
import time
import logging
import pandas as pd
import os
from pathlib import Path
from batch_data_collector import BatchDataCollector
from datetime import datetime, timedelta

# Configure logging
log_dir = Path("logs")
log_dir.mkdir(exist_ok=True)

# Configure file handler
file_handler = logging.FileHandler(log_dir / 'batch_collector_scheduler.log')
file_handler.setLevel(logging.INFO)
file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(file_formatter)

# Configure console handler
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler.setFormatter(console_formatter)

# Set up logger
logger = logging.getLogger('BatchCollectorScheduler')
logger.setLevel(logging.INFO)
logger.addHandler(file_handler)
logger.addHandler(console_handler)

# Prevent propagation to root logger to avoid duplicate logs
logger.propagate = False

# Configuration
STOCKS_CSV_PATH = 'stocks.csv'
CUSTOM_SYMBOLS = ['AAPL', 'MSFT', 'AMZN', 'GOOGL', 'META', 'TSLA', 'NVDA']
INDEX_SYMBOLS = ['^GSPC', '^DJI', '^IXIC', '^GDAXI']  # S&P 500, Dow Jones, NASDAQ, DAX

# Collection intervals
# Format is (interval, period, frequency_minutes)
COLLECTION_CONFIGS = [
    # High-frequency data for key indices and stocks (5-minute intervals)
    {
        'name': 'high_freq',
        'interval': '5m',
        'period': '1d', 
        'frequency_minutes': 5,
        'symbols': CUSTOM_SYMBOLS + INDEX_SYMBOLS  # Only key symbols
    },
    # Hourly data for all symbols (all 121 stocks)
    {
        'name': 'hourly',
        'interval': '1h',
        'period': '5d',
        'frequency_minutes': 60,
        'symbols': 'all'  # All symbols from CSV
    },
    # Daily data for all symbols (updated once per day)
    {
        'name': 'daily',
        'interval': '1d',
        'period': '1mo',
        'frequency_minutes': 1440,  # Once per day
        'symbols': 'all'  # All symbols from CSV
    },
    # News collection for key stocks (not indices)
    {
        'name': 'news',
        'frequency_minutes': 120,  # Every 2 hours
        'symbols': CUSTOM_SYMBOLS  # Only for stocks
    }
]

# Initialize collector
collector = BatchDataCollector(max_batch_size=40)

def read_symbols_from_csv(csv_path=STOCKS_CSV_PATH):
    """Read symbols from CSV file."""
    try:
        df = pd.read_csv(csv_path, sep=';')
        if 'Symbol' in df.columns:
            symbols = df['Symbol'].tolist()
            # Filter out empty or NA values
            symbols = [s for s in symbols if s and pd.notna(s)]
            return symbols
        else:
            logger.error(f"No 'Symbol' column in {csv_path}")
            return []
    except Exception as e:
        logger.error(f"Error reading CSV file {csv_path}: {e}")
        return []

def get_symbols_for_config(config):
    """Get the symbol list for a configuration."""
    if isinstance(config['symbols'], list):
        return config['symbols']
    elif config['symbols'] == 'all':
        all_symbols = read_symbols_from_csv()
        if not all_symbols:
            logger.warning("Failed to read symbols from CSV, using custom symbols instead")
            all_symbols = CUSTOM_SYMBOLS + INDEX_SYMBOLS
        return all_symbols
    else:
        logger.error(f"Unknown symbols configuration: {config['symbols']}")
        return []

def collect_market_data(config):
    """Collect market data based on configuration."""
    name = config.get('name', 'unnamed')
    interval = config.get('interval')
    period = config.get('period')
    
    if not interval or not period:
        logger.error(f"Missing interval or period in config '{name}'")
        return
    
    symbols = get_symbols_for_config(config)
    if not symbols:
        logger.warning(f"No symbols to collect for config '{name}'")
        return
    
    logger.info(f"Starting market data collection job '{name}' for {len(symbols)} symbols")
    logger.info(f"Interval: {interval}, Period: {period}")
    
    try:
        success, collected = collector.collect_data(symbols, interval, period)
        
        if success:
            logger.info(f"Market data collection '{name}' successful for {collected}/{len(symbols)} symbols")
        else:
            logger.warning(f"Market data collection '{name}' completed with issues. Collected: {collected}/{len(symbols)}")
    except Exception as e:
        logger.error(f"Error in market data collection job '{name}': {e}")

def collect_news(config):
    """Collect news data based on configuration."""
    name = config.get('name', 'unnamed')
    symbols = get_symbols_for_config(config)
    
    if not symbols:
        logger.warning(f"No symbols for news collection '{name}'")
        return
    
    logger.info(f"Starting news collection job '{name}' for {len(symbols)} symbols")
    
    try:
        count = collector.fetch_news(symbols)
        logger.info(f"News collection '{name}' completed for {count}/{len(symbols)} symbols")
    except Exception as e:
        logger.error(f"Error in news collection job '{name}': {e}")

def run_collection_job(config):
    """Run a collection job based on configuration."""
    name = config.get('name', 'unnamed')
    logger.info(f"Running scheduled job '{name}'")
    
    if name == 'news':
        collect_news(config)
    else:
        collect_market_data(config)

def setup_schedules():
    """Set up the collection schedules."""
    for config in COLLECTION_CONFIGS:
        name = config.get('name', 'unnamed')
        frequency = config.get('frequency_minutes', 60)
        
        if frequency >= 1440:  # Daily job
            # Schedule to run at a specific time (e.g., 6:30 AM)
            schedule.every().day.at("06:30").do(run_collection_job, config)
            logger.info(f"Scheduled '{name}' to run daily at 06:30")
        else:
            # Schedule to run at specified frequency
            schedule.every(frequency).minutes.do(run_collection_job, config)
            logger.info(f"Scheduled '{name}' to run every {frequency} minutes")

def run_initial_jobs():
    """Run initial data collection jobs on startup."""
    logger.info("Running initial data collection jobs")
    
    # Run in reverse order - start with daily, then hourly, then high frequency
    for config in reversed(COLLECTION_CONFIGS):
        run_collection_job(config)

def is_trading_hours():
    """Check if current time is within trading hours."""
    now = datetime.now()
    
    # Weekday check (0=Monday, 6=Sunday)
    if now.weekday() >= 5:  # Saturday or Sunday
        return False
    
    # Time check for weekdays (trading hours: 9:00 AM - 5:30 PM)
    trading_start = datetime(now.year, now.month, now.day, 9, 0, 0)
    trading_end = datetime(now.year, now.month, now.day, 17, 30, 0)
    
    return trading_start <= now <= trading_end

def main():
    """Main function to start the scheduler."""
    logger.info("Starting batch collector scheduler")
    
    # Setup schedules
    setup_schedules()
    
    # Initial data collection
    run_initial_jobs()
    
    # Main loop
    logger.info("Entering scheduler main loop")
    while True:
        try:
            # Only run collections during trading hours
            if is_trading_hours():
                schedule.run_pending()
            else:
                next_job = schedule.next_run()
                if next_job:
                    logger.info(f"Outside trading hours. Next collection scheduled at {next_job}")
                
            # Sleep for 10 seconds
            time.sleep(10)
        except Exception as e:
            logger.error(f"Error in scheduler main loop: {str(e)}")
            time.sleep(60)  # Sleep for a minute on error

if __name__ == "__main__":
    main()
