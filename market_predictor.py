"""
Market Predictor Module

This module uses LSTM networks to predict market trends based on historical data.
It combines technical indicators with deep learning for short-term price predictions.
"""

import os
import torch
import torch.nn as nn
import numpy as np
import pandas as pd
from sklearn.preprocessing import MinMaxScaler
import sqlite3
import logging
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] MarketPredictor: %(message)s',
    handlers=[
        logging.FileHandler('market_predictor.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class MarketLSTM(nn.Module):
    def __init__(self, input_size, hidden_size, num_layers, output_size):
        super(MarketLSTM, self).__init__()
        self.hidden_size = hidden_size
        self.num_layers = num_layers
        
        self.lstm = nn.LSTM(input_size, hidden_size, num_layers, batch_first=True)
        self.fc = nn.Linear(hidden_size, output_size)
    
    def forward(self, x):
        h0 = torch.zeros(self.num_layers, x.size(0), self.hidden_size).to(x.device)
        c0 = torch.zeros(self.num_layers, x.size(0), self.hidden_size).to(x.device)
        
        out, _ = self.lstm(x, (h0, c0))
        out = self.fc(out[:, -1, :])
        return out

class MarketPredictor:
    """Predicts market movements using LSTM networks."""
    def __init__(self, db_path, checkpoint_dir='checkpoints', sequence_length=60):
        """
        Initialize the market predictor.
        
        Args:
            db_path: Path to SQLite database with market data
            checkpoint_dir: Directory for model checkpoints
            sequence_length: Number of time steps to use for prediction
        """
        self.db_path = db_path
        self.checkpoint_dir = checkpoint_dir
        self.sequence_length = sequence_length
        self.scaler = MinMaxScaler()
        
        # Create checkpoints directory if it doesn't exist
        os.makedirs(self.checkpoint_dir, exist_ok=True)
          # Model parameters
        self.input_size = 7  # OHLCV + 2 technical indicators
        self.hidden_size = 64
        self.num_layers = 2
        self.output_size = 3  # Up, Down, Sideways
        
        # Initialize model
        self.device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
        self.model = MarketLSTM(self.input_size, self.hidden_size, 
                              self.num_layers, self.output_size).to(self.device)
          # Setup logging
        self.logger = logging.getLogger('MarketPredictor')
        
        # Load model if exists
        self._load_model()
    
    def _load_model(self, symbol=None):
        """Load trained model from checkpoint.
        
        Args:
            symbol: Symbol to load model for. If None, loads the default model.
            
        Returns:
            bool: True if model was successfully loaded, False otherwise
        """
        if symbol:
            model_path = f"{self.checkpoint_dir}/market_lstm_{symbol}.pth"
            scaler_path = f"{self.checkpoint_dir}/market_scaler_{symbol}.npy"
        else:
            model_path = f"{self.checkpoint_dir}/market_lstm.pth" 
            scaler_path = f"{self.checkpoint_dir}/market_scaler.npy"
        
        try:
            model_loaded = False
            
            if os.path.exists(model_path):
                self.model.load_state_dict(torch.load(model_path))
                self.model.eval()
                self.logger.info(f"Loaded market prediction model for {symbol if symbol else 'default'}")
                model_loaded = True
            else:
                # If symbol-specific model doesn't exist but default does, load that
                if symbol and os.path.exists(f"{self.checkpoint_dir}/market_lstm.pth"):
                    self.model.load_state_dict(torch.load(f"{self.checkpoint_dir}/market_lstm.pth"))
                    self.model.eval()
                    self.logger.info(f"Symbol-specific model for {symbol} not found, using default model")
                    model_loaded = True
            
            if os.path.exists(scaler_path):
                self.scaler = MinMaxScaler()
                self.scaler.min_, self.scaler.scale_ = np.load(scaler_path)
                self.logger.info(f"Loaded scaler parameters for {symbol if symbol else 'default'}")
            elif symbol and os.path.exists(f"{self.checkpoint_dir}/market_scaler.npy"):
                self.scaler = MinMaxScaler()
                self.scaler.min_, self.scaler.scale_ = np.load(f"{self.checkpoint_dir}/market_scaler.npy")
                
            return model_loaded
        
        except Exception as e:
            self.logger.error(f"Error loading model for {symbol if symbol else 'default'}: {e}")
            return False
    def _save_model(self, symbol=None):
        """Save model and scaler to checkpoint.
        
        Args:
            symbol: Symbol to save model for. If None, saves as default model.
        """
        try:
            if not os.path.exists(self.checkpoint_dir):
                os.makedirs(self.checkpoint_dir)
            
            if symbol:
                model_path = f"{self.checkpoint_dir}/market_lstm_{symbol}.pth"
                scaler_path = f"{self.checkpoint_dir}/market_scaler_{symbol}.npy"
            else:
                model_path = f"{self.checkpoint_dir}/market_lstm.pth"
                scaler_path = f"{self.checkpoint_dir}/market_scaler.npy"
            
            torch.save(self.model.state_dict(), model_path)
            np.save(scaler_path, [self.scaler.min_, self.scaler.scale_])
            
            self.logger.info(f"Saved market prediction model checkpoint for {symbol if symbol else 'default'}")
        
        except Exception as e:
            self.logger.error(f"Error saving model for {symbol if symbol else 'default'}: {e}")
    def _get_training_data(self, symbol=None, days=365):
        """
        Get historical market data for training.
        
        Args:
            symbol: Symbol to get data for. If None, gets data for all symbols.
            days: Number of days of historical data to use
        
        Returns:
            DataFrame with market data and technical indicators
        """
        from_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        
        # Build query based on whether we want a specific symbol
        if symbol:
            query = """
            SELECT md.timestamp, md.symbol, md.open, md.high, md.low, md.close, md.volume,
                   ta.rsi, ta.macd_line as macd
            FROM market_data md
            LEFT JOIN technical_analysis ta ON md.timestamp = ta.timestamp AND md.symbol = ta.symbol
            WHERE md.timestamp >= ? AND md.symbol = ?
            ORDER BY md.timestamp ASC
            """
            params = (from_date, symbol)
        else:
            query = """
            SELECT md.timestamp, md.symbol, md.open, md.high, md.low, md.close, md.volume,
                   ta.rsi, ta.macd_line as macd
            FROM market_data md
            LEFT JOIN technical_analysis ta ON md.timestamp = ta.timestamp AND md.symbol = ta.symbol
            WHERE md.timestamp >= ?
            ORDER BY md.symbol, md.timestamp ASC
            """
            params = (from_date,)
        
        try:
            conn = sqlite3.connect(self.db_path)
            df = pd.read_sql_query(query, conn, params=params)
            conn.close()
            
            if df.empty:
                self.logger.error(f"No training data found for {symbol if symbol else 'any symbol'}")
                return None
                
            # Calculate returns and movement labels
            # Group by symbol if we have multiple symbols
            if symbol:
                df['returns'] = df['close'].pct_change()
            else:
                df['returns'] = df.groupby('symbol')['close'].pct_change()
                
            df['movement'] = pd.cut(df['returns'], 
                                  bins=[-np.inf, -0.001, 0.001, np.inf],
                                  labels=[0, 1, 2])  # Down, Sideways, Up
            
            return df.dropna()
        
        except Exception as e:
            self.logger.error(f"Error fetching training data for {symbol if symbol else 'all symbols'}: {e}")
            return None
    
    def _prepare_sequences(self, data):
        """Prepare sequences for LSTM training."""
        features = ['open', 'high', 'low', 'close', 'volume', 'rsi', 'macd']
        
        # Scale features
        X = self.scaler.fit_transform(data[features])
        y = data['movement'].values
        
        # Create sequences
        X_seq, y_seq = [], []
        for i in range(len(X) - self.sequence_length):
            X_seq.append(X[i:i + self.sequence_length])
            y_seq.append(y[i + self.sequence_length])
        
        return (torch.FloatTensor(X_seq).to(self.device), 
                torch.LongTensor(y_seq).to(self.device))
    def train(self, symbol=None, epochs=50, batch_size=32, validation_split=0.2):
        """Train the model on historical data.
        
        Args:
            symbol: Symbol to train on. If None, trains on all symbols.
            epochs: Number of training epochs
            batch_size: Batch size for training
            validation_split: Proportion of data to use for validation
            
        Returns:
            dict with training metrics or False if training failed
        """
        # Get training data
        data = self._get_training_data(symbol)
        if data is None:
            return False
        
        # Load the corresponding model first (or use default if new symbol)
        self._load_model(symbol)
        
        # Prepare sequences
        X, y = self._prepare_sequences(data)
        
        # Split into training and validation sets
        val_size = int(len(X) * validation_split)
        train_size = len(X) - val_size
        
        if train_size == 0 or val_size == 0:
            self.logger.error(f"Not enough data to split into train/val sets. Total samples: {len(X)}")
            return False
            
        X_train, X_val = X[:train_size], X[train_size:]
        y_train, y_val = y[:train_size], y[train_size:]
        
        # Setup training
        criterion = nn.CrossEntropyLoss()
        optimizer = torch.optim.Adam(self.model.parameters())
        
        # Train the model
        self.model.train()
        best_val_loss = float('inf')
        training_history = {'train_loss': [], 'val_loss': [], 'val_accuracy': []}
        
        for epoch in range(epochs):
            # Training
            self.model.train()
            total_loss = 0
            for i in range(0, len(X_train), batch_size):
                X_batch = X_train[i:i + batch_size]
                y_batch = y_train[i:i + batch_size]
                
                optimizer.zero_grad()
                outputs = self.model(X_batch)
                loss = criterion(outputs, y_batch)
                loss.backward()
                optimizer.step()
                
                total_loss += loss.item()
            
            train_loss = total_loss / (len(X_train) / batch_size)
            
            # Validation
            self.model.eval()
            with torch.no_grad():
                val_outputs = self.model(X_val)
                val_loss = criterion(val_outputs, y_val).item()
                
                # Calculate accuracy
                _, predicted = torch.max(val_outputs, 1)
                val_accuracy = (predicted == y_val).sum().item() / len(y_val)
            
            # Save history
            training_history['train_loss'].append(train_loss)
            training_history['val_loss'].append(val_loss)
            training_history['val_accuracy'].append(val_accuracy)
            
            self.logger.info(
                f"Epoch {epoch + 1}/{epochs} - "
                f"Train Loss: {train_loss:.4f}, "
                f"Val Loss: {val_loss:.4f}, "
                f"Val Accuracy: {val_accuracy:.4f}"
            )
            
            # Save best model
            if val_loss < best_val_loss:
                best_val_loss = val_loss
                self._save_model(symbol)
                self.logger.info(f"New best model saved with val_loss: {val_loss:.4f}")
        
        return {
            'final_train_loss': training_history['train_loss'][-1],
            'final_val_loss': training_history['val_loss'][-1],
            'final_val_accuracy': training_history['val_accuracy'][-1],
            'best_val_loss': best_val_loss,
            'history': training_history
        }
    
    def predict(self, latest_data=None):
        """
        Predict market movement based on latest data.
        
        Args:
            latest_data: Optional DataFrame with latest market data
                        If None, fetches latest data from database
        
        Returns:
            dict with prediction probabilities and confidence
        """
        self.model.eval()
        
        try:
            if latest_data is None:
                # Get latest sequence_length records
                query = """
                SELECT md.timestamp, md.open, md.high, md.low, md.close, md.volume,
                       ta.rsi as rsi, ta.macd_line as macd
                FROM market_data md
                LEFT JOIN technical_analysis ta ON md.timestamp = ta.timestamp AND md.symbol = ta.symbol
                ORDER BY md.timestamp DESC
                LIMIT ?
                """
                
                conn = sqlite3.connect(self.db_path)
                latest_data = pd.read_sql_query(query, conn, 
                                              params=(self.sequence_length,))
                conn.close()
                
                latest_data = latest_data.iloc[::-1]  # Reverse to ascending order
            
            features = ['open', 'high', 'low', 'close', 'volume', 'rsi', 'macd']
            X = self.scaler.transform(latest_data[features])
            X = torch.FloatTensor(X).unsqueeze(0).to(self.device)
            
            with torch.no_grad():
                outputs = self.model(X)
                probabilities = torch.softmax(outputs, dim=1).cpu().numpy()[0]
                prediction = int(torch.argmax(outputs, dim=1).cpu().numpy()[0])
                confidence = float(probabilities[prediction])
                
                return {
                    'prediction': ['down', 'sideways', 'up'][prediction],
                    'confidence': confidence,
                    'probabilities': {
                        'down': float(probabilities[0]),
                        'sideways': float(probabilities[1]),
                        'up': float(probabilities[2])
                    }                }
        
        except Exception as e:
            self.logger.error(f"Error making prediction: {e}")
            return None
            
    def predict(self, symbol):
        """
        Predict market movement for a specific symbol.
        
        Args:
            symbol: The symbol to make predictions for
        
        Returns:
            dict with prediction probabilities and confidence
        """
        self.model.eval()
        
        try:
            # Get latest sequence_length records for this symbol
            query = """
            SELECT md.timestamp, md.open, md.high, md.low, md.close, md.volume,
                   ta.rsi as rsi, ta.macd_line as macd
            FROM market_data md
            LEFT JOIN technical_analysis ta ON md.timestamp = ta.timestamp AND md.symbol = ta.symbol
            WHERE md.symbol = ?
            ORDER BY md.timestamp DESC
            LIMIT ?
            """
            
            conn = sqlite3.connect(self.db_path)
            latest_data = pd.read_sql_query(query, conn, 
                                          params=(symbol, self.sequence_length))
            conn.close()
            
            if latest_data.empty:
                self.logger.warning(f"No data found for symbol: {symbol}")
                return {
                    'prediction': 'no_data',
                    'confidence': 0.0,
                    'probabilities': {
                        'up': 0.0,
                        'down': 0.0,
                        'sideways': 0.0
                    }
                }
            
            # If we don't have enough data for a full sequence, handle appropriately
            if len(latest_data) < self.sequence_length:
                self.logger.warning(f"Insufficient data for {symbol}: {len(latest_data)} records, need {self.sequence_length}")
                
                # If we have at least 5 data points, dynamically adjust sequence length
                if len(latest_data) >= 5:
                    self.logger.info(f"Adjusting sequence length from {self.sequence_length} to {len(latest_data)-1}")
                    old_seq_length = self.sequence_length
                    self.sequence_length = len(latest_data) - 1  # Leave 1 point for testing
                else:  # If really insufficient data
                    return {
                        'prediction': 'insufficient_data',
                        'confidence': 0.0,
                        'symbol': symbol,
                        'data_points': len(latest_data),
                        'probabilities': {
                            'up': 0.33,
                            'down': 0.33,
                            'sideways': 0.34
                        }
                    }
            
            latest_data = latest_data.iloc[::-1]  # Reverse to ascending order
            
            # Dynamically use available features - handle missing columns
            features = ['open', 'high', 'low', 'close', 'volume']
            if 'rsi' in latest_data.columns:
                features.append('rsi')
            if 'macd' in latest_data.columns:
                features.append('macd')
                
            # Fill in any missing values
            latest_data = latest_data[features].fillna(method='ffill').fillna(method='bfill').fillna(0)
                
            # Handle the case where we have actual data, but not enough for a full sequence
            missing_count = max(0, self.sequence_length - len(latest_data))
            if missing_count > 0:
                # Duplicate the first row as necessary to pad the sequence
                padding = pd.concat([latest_data.iloc[[0]]] * missing_count)
                latest_data = pd.concat([padding, latest_data]).reset_index(drop=True)
            
            # Scale the data
            X = self.scaler.fit_transform(latest_data[features])
            X = torch.FloatTensor(X).unsqueeze(0).to(self.device)
            
            with torch.no_grad():
                outputs = self.model(X)
                probabilities = torch.softmax(outputs, dim=1).cpu().numpy()[0]
                prediction = int(torch.argmax(outputs, dim=1).cpu().numpy()[0])
                confidence = float(probabilities[prediction])
                
                self.logger.info(f"Prediction for {symbol}: {['down', 'sideways', 'up'][prediction]} with {confidence:.2f} confidence")
                
                return {
                    'prediction': ['down', 'sideways', 'up'][prediction],
                    'confidence': confidence,
                    'symbol': symbol,
                    'probabilities': {
                        'down': float(probabilities[0]),
                        'sideways': float(probabilities[1]),
                        'up': float(probabilities[2])
                    }
                }
        
        except Exception as e:
            self.logger.error(f"Error making prediction for {symbol}: {e}")
            return {
                'prediction': 'error',
                'confidence': 0.0,
                'symbol': symbol,
                'error': str(e),
                'probabilities': {
                    'up': 0.0,
                    'down': 0.0,
                    'sideways': 0.0
                }
            }
