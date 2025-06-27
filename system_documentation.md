# DaxTracker3 System Documentation

## Project Overview

DaxTracker3 is a comprehensive trading signal system that collects market data, performs technical analysis, generates trading signals, and provides ML predictions. The system includes a dashboard for visualization and monitoring capabilities.

## Core Components

### Data Collection

- `data_collector.py`: Core module for fetching market data from Yahoo Finance and Alpha Vantage APIs. Handles storage in SQLite database.
- `run_collector.py`: Scheduler script that runs data collection at specified intervals. Manages symbol lists and collection parameters.
- `batch_data_collector.py`: Enhanced data collector using yfinance's batch download feature for efficient collection of data for large symbol lists (up to 121 stocks) while respecting API rate limits.
- `run_batch_collector.py`: Scheduler for the batch collector with different collection frequencies for different symbol groups and data granularities.
- `data_validator.py`: Validates collected market data to ensure integrity and completeness before further processing.
- `insert_demo_data.py`: Utility script for inserting demo data for testing.
- `insert_demo_data_extended.py`: Extended version of demo data insertion with more comprehensive datasets.
- `symbol_mapping.py`: Maps between different symbol representations across various data sources.
- `test_batch_download.py`: Test script for evaluating batch download capabilities for different intervals and symbol batch sizes.

### Technical Analysis

- `technical_analyzer.py`: Core module for calculating technical indicators (SMA, EMA, RSI, MACD, etc.) from market data.
- `run_technical_analysis.py`: Scheduler script that runs technical analysis at regular intervals for configured symbols.
- `quick_technical_analysis.py`: Lightweight version for quick ad-hoc technical analysis of specified symbols.

### ML Prediction System

- `market_predictor.py`: Core ML module using LSTM networks to predict market movements based on technical indicators.
- `train_ml_model.py`: Script for training ML models for a specific symbol.
- `batch_train_ml_models.py`: Batch processing script to train models for multiple symbols.
- `ml_processor.py`: Manages ML prediction pipeline, handling interruptions and maintaining state.
- `ml_metrics.py`: Calculates and stores performance metrics (accuracy, precision, recall, F1) for ML models.
- `ml_evaluator.py`: Evaluates model performance against historical data.
- `market_regime.py`: Analyzes market regimes to adapt prediction strategies.
- `run_ml_processor.py`: Runs the ML processor as a service with scheduling.
- `train_models.bat`: Batch file for convenient model training on Windows.

### Signal Generation

- `signal_generator.py`: Core module for generating trading signals by combining technical analysis, ML predictions, and sentiment.
- `run_signal_generator.py`: Runs the signal generation process at scheduled intervals.
- `gen_signals_test.py`: Test script for signal generation.
- `gen_all_signals.py`: Generates signals for all configured symbols.
- `generate_all_signals.py`: Alternative implementation for generating all signals.
- `generate_all_signals_relaxed.py`: Variation with relaxed signal generation parameters.
- `check_signals_status.py`: Utility to verify the status of generated signals.

### Dashboard and Visualization

- `dashboard.py`: Main Streamlit dashboard application providing visualizations and user interface.
- `tab6_content.py`: ML prediction visualization tab for the dashboard, showing model performance metrics.
- `tab7_content.py`: Additional dashboard tab content.

### Notification System

- `notification_system.py`: Core module for sending notifications through various channels.
- `notification_templates.py`: Templates for different types of notifications.
- `run_notifier.py`: Runs the notification system as a service.
- `telegram_config_setup.py`: Setup script for Telegram notification integration.

### System Monitoring and Maintenance

- `system_monitor.py`: Monitors system health, process status, database integrity, etc.
- `backup_system.py`: Handles database backups and restoration.
- `run_maintenance.py`: Performs routine maintenance tasks.
- `check_db.py`: Validates database structure and contents.
- `check_schema.py`: Checks database schema against expected structure.
- `reset_tables.py`: Utility for resetting database tables.

### Trading Server and API

- `trading_signal_server.py`: API server for accessing trading signals.
- `trading_signal_server_new.py`: Updated version of the trading signal server.
- `test_server.py`: Tests for the trading signal server.
- `test_endpoints.py`: Tests for specific API endpoints.

### Backtesting and Evaluation

- `backtesting.py`: Framework for backtesting trading strategies.
- `test_backtesting.py`: Tests for the backtesting system.

### Pipeline Automation

- `automate_data_pipeline.py`: Automates the entire data processing pipeline.
- `run_batch_pipeline.py`: Runs the entire pipeline as a batch process.

### Sentiment Analysis

- `sentiment_analyzer.py`: Analyzes news sentiment using the FinBERT model.

## Data Flow

1. **Data Collection**: `run_collector.py` → `data_collector.py` → Market data stored in SQLite DB
2. **Technical Analysis**: `run_technical_analysis.py` → `technical_analyzer.py` → Technical indicators stored in DB
3. **ML Processing**: `run_ml_processor.py` → `ml_processor.py` → `market_predictor.py` → Predictions stored in DB
4. **Signal Generation**: `run_signal_generator.py` → `signal_generator.py` → Trading signals stored in DB
5. **Visualization**: `dashboard.py` reads from DB to display current state
6. **Notification**: `run_notifier.py` → `notification_system.py` sends alerts for important signals

## Key Files Without Clear Purpose

- None identified. All files appear to have specific roles in the system architecture.

## Notes

- The system uses SQLite for data storage, which is appropriate for a single-user system but may need upgrading to a more robust database for production use with multiple users.
- The data collection functionality (`run_collector.py`) should be run before other components to ensure fresh market data is available.
- ML models are stored in the `checkpoints/` directory with naming pattern `market_lstm_SYMBOL.pth`.
- Technical analysis must be run before ML prediction to ensure the necessary indicators are available.

### Batch Data Collection

The system includes an enhanced data collection method using yfinance's batch download capability:

1. **Efficiency**: The batch collector can retrieve data for multiple symbols in a single API call, significantly reducing API usage and increasing collection speed.

2. **Tiered Collection Strategy**: 
   - High-frequency data (5-minute intervals): Collected for key stocks and indices only
   - Hourly data: Can be collected for all 121 stocks
   - Daily data: Collected once daily for all symbols

3. **API Rate Limit Management**: 
   - Symbols are collected in batches (typically 40-50 symbols per batch)
   - Implements adaptive sleep times and exponential backoff
   - Collection statistics are stored to monitor performance
   - Trading hours detection prevents unnecessary API calls

4. **Usage**:
   - For regular operation: run `python run_batch_collector.py`
   - For one-off collection: use `batch_data_collector.py`
   - For testing: use `test_batch_download.py`
   - For monitoring: use the "Batch Monitoring" tab in the dashboard

5. **Configuration**: Adjust batch sizes, collection frequencies, and symbol groups in `run_batch_collector.py`

6. **Dashboard Integration**:
   - A dedicated "Batch Monitoring" tab in the dashboard shows collection statistics
   - View performance metrics like success rates, collection times, and error rates
   - Run batch collection jobs directly from the dashboard
   - Track trends and optimize batch sizes based on performance data

## Console Output Configuration

Most scripts in the project use Python's logging module for output, which is typically configured to write to log files rather than the console. To enable console output in addition to file logging, modify the logger configuration in each script using one of these approaches:

1. **Global approach**: Update the logging configuration in each script to include both file and console handlers:

    ```python
    import logging

    # Existing file handler setup
    file_handler = logging.FileHandler('script_name.log')
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))

    # Add console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))

    # Configure logger
    logger = logging.getLogger('script_name')
    logger.setLevel(logging.INFO)  # Or DEBUG for more detailed output
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)  # Add this line to enable console output
    ```

2. **Quick approach**: Add direct print statements for important operations:

    ```python
    logger.info("Operation completed")  # Logged to file
    print("Operation completed")  # Also displayed in console
    ```

3. **For batch files**: Ensure standard output isn't redirected by removing any redirection operators (`>` or `>>`) that send output to files instead of the console.

## Key Scripts with Logging

The following scripts use logging and should be updated to show console output:

- `system_monitor.py` - Monitor logs in `monitor.log`
- `data_collector.py` - Collection logs in `collector_scheduler.log`
- `ml_processor.py` - ML processing logs
- `signal_generator.py` - Signal generation logs
- `run_technical_analysis.py` - Technical analysis logs
- `batch_train_ml_models.py` - Training logs in `ml_training.log`

## Real-Time Signal Alerts

The dashboard includes a real-time alert system that notifies users when new trading signals are generated:

1. **Dashboard Notifications**: 
   - Automatically displays new trading signals at the top of the dashboard
   - Visual indicators differentiate between BUY (green), SELL (red), and other signal types (yellow)
   - Configurable to show signals from the last 5-120 minutes

2. **Auto-Refresh Features**:
   - Automatically refreshes the dashboard at configurable intervals (10-300 seconds)
   - Checks for new signals on each refresh
   - Option to play a sound alert when new signals are detected

3. **Alert Settings**:
   - Enable/disable auto-refresh
   - Adjust refresh frequency
   - Set the time window for displaying recent alerts
   - Toggle sound notifications
   - Test alert functionality to verify the system

4. **Multiple Notification Channels**:
   - Dashboard visual alerts (real-time during trading sessions)
   - Sound alerts (optional)
   - Telegram messages (configured separately)

This multi-channel approach ensures that users are promptly notified of new trading signals whether they're actively monitoring the dashboard or away from their computer.
