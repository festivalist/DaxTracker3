# DaxTracker3: Advanced Market Signal Tracking System

DaxTracker3 is a comprehensive market analysis and trading signal system that combines technical analysis, machine learning prediction, and signal generation for multiple market symbols.

## Features

- **Multi-Symbol Support**: Analyzes and generates signals for 120+ market symbols including DAX, international indices, and stocks
- **Robust Signal Generation**: Generates BUY, SELL, and NO_SIGNAL indicators based on combined technical analysis
- **Machine Learning Integration**: Uses LSTM neural networks to predict market movements
- **Interactive Dashboard**: Streamlit-based visualization of signals, market data, and ML predictions
- **System Monitoring**: Automated health checks and database backups for continuous operation
- **Batch Processing**: Support for batch processing of all symbols

## System Components

- **Data Collection**: Gathers market data from Yahoo Finance
- **Technical Analysis**: Calculates technical indicators (RSI, MACD, etc.)
- **Signal Generation**: Generates trading signals based on technical analysis
- **ML Prediction**: Predicts market movements using LSTM neural networks
- **Dashboard**: Visualizes market data, signals, and predictions
- **System Monitor**: Ensures system health and creates regular backups

## Getting Started

### Prerequisites

- Python 3.8+
- Required Python packages (see `requirements.txt`)

### Installation

1. Clone the repository
2. Install dependencies:
   ```
   pip install -r requirements.txt
   ```

### Running the System

#### Data Pipeline

1. Collect market data:
   ```
   python run_collector.py
   ```

2. Run technical analysis:
   ```
   python run_technical_analysis.py
   ```

3. Generate signals:
   ```
   python generate_all_signals.py
   ```

4. Process ML predictions:
   ```
   python run_ml_processor.py
   ```

#### Dashboard

Start the interactive dashboard:
```
run_dashboard.bat
```
or directly with:
```
streamlit run dashboard.py
```

#### System Monitoring

Start the system monitoring and automatic backup service:
```
start_system_monitor.bat
```

### ML Training

Train the ML model for a specific symbol:
```
python train_ml_model.py --symbol DAX --epochs 50
```

Train models for all available symbols:
```
python train_ml_model.py --epochs 50
```

## Development Roadmap

1. ✅ Multi-symbol support
2. ✅ Enhanced dashboard with signal distribution visualization
3. ✅ ML prediction integration
4. ✅ System monitoring and automated backups
5. ⬜ Portfolio optimization features
6. ⬜ Advanced sentiment analysis integration

## Documentation

For more detailed information, see the project documentation in the `docs` folder.

## License

This project is proprietary and confidential. All rights reserved.
