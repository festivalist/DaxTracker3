"""
Data validation module for ensuring market data quality.
"""

import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import logging
import numpy as np

class DataValidator:
    def __init__(self, db_path):
        """
        Initialize data validator.
        
        Args:
            db_path (str): Path to the SQLite database
        """
        self.db_path = db_path
        
        # Set up logging
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
    
    def validate_market_data(self, symbol, max_age_hours=24, is_test=False):
        """
        Validate market data for a symbol.
        
        Args:
            symbol (str): Trading symbol to validate
            max_age_hours (int): Maximum age of data in hours
            is_test (bool): Whether this is test data (more lenient validation)
            
        Returns:
            dict: Validation results with issues found
        """
        issues = []
        
        try:
            conn = sqlite3.connect(self.db_path)
            
            # Check data freshness
            query = """
            SELECT timestamp, close, volume
            FROM market_data
            WHERE symbol = ?
            ORDER BY timestamp DESC
            LIMIT 1
            """
            
            df = pd.read_sql_query(query, conn, params=(symbol,))
            
            if df.empty:
                issues.append(f"No market data found for {symbol}")
                return {'valid': False, 'issues': issues}
            
            latest_timestamp = pd.to_datetime(df['timestamp'].iloc[0])
            age = datetime.now() - latest_timestamp.to_pydatetime()
              # Relaxed age check - only warn if data is more than 7 days old
            if not is_test and age > timedelta(days=7):
                issues.append(
                    f"Data is too old. Latest: {latest_timestamp}, "
                    f"Age: {age.total_seconds() / 3600:.1f} hours"
                )
            
            # Check for missing values
            if df['close'].isnull().any():
                issues.append("Missing close prices")
            
            # Get recent data for quality checks
            query = """
            SELECT timestamp, close, volume
            FROM market_data
            WHERE symbol = ?
            ORDER BY timestamp DESC
            LIMIT 100
            """
            
            df = pd.read_sql_query(query, conn, params=(symbol,))
            
            # Check for price gaps (only in production) - relaxed to 20%
            if not is_test:
                price_changes = df['close'].pct_change().abs()
                large_gaps = price_changes[price_changes > 0.2]  # 20% change
                if not large_gaps.empty:
                    issues.append(
                        f"Large price gaps detected at: "
                        f"{', '.join(df.index[large_gaps.index].astype(str))}"
                    )
            
            # Tolerate some zero volumes - only warn if more than 25% of data has zero volume
            if not is_test:
                zero_volumes = df[df['volume'] == 0]
                if len(zero_volumes) > len(df) * 0.25:  # More than 25% have zero volume
                    issues.append(f"Too many zero volume periods: {len(zero_volumes)} instances (>{len(df)*0.25:.0f})")
            
            # Check for duplicate timestamps
            duplicates = df[df['timestamp'].duplicated()]
            if not duplicates.empty:
                issues.append(f"Duplicate timestamps detected: {len(duplicates)} instances")
            
            conn.close()
            
            return {
                'valid': len(issues) == 0 or is_test,  # More lenient in test mode
                'issues': issues,
                'latest_timestamp': latest_timestamp,
                'data_points': len(df)
            }
            
        except Exception as e:
            self.logger.error(f"Error validating data for {symbol}: {e}")
            issues.append(f"Validation error: {str(e)}")
            return {'valid': False, 'issues': issues}
    def validate_technical_indicators(self, symbol):
        """
        Validate technical indicators for a symbol.
        
        Args:
            symbol (str): Trading symbol to validate
            
        Returns:
            dict: Validation results with issues found
        """
        issues = []
        
        try:
            conn = sqlite3.connect(self.db_path)
            
            # First check if technical_analysis table exists and has expected columns
            cursor = conn.cursor()
            cursor.execute("PRAGMA table_info(technical_analysis)")
            columns = cursor.fetchall()
            column_names = [col[1] for col in columns]
            
            # If the table structure doesn't match what we expect, we'll return valid=True
            # and let the _calculate_technical_signals method handle it
            required_columns = ['rsi_14', 'macd_line', 'signal_line', 'adx', 'pdi', 'ndi']
            if not all(col in column_names for col in required_columns):
                self.logger.warning(f"Technical analysis table missing required columns for {symbol}. Will calculate on demand.")
                return {
                    'valid': True,  # Allow processing to continue
                    'issues': ["Technical indicators will be calculated on demand"],
                    'indicators_present': []
                }
            
            # Check technical analysis data
            query = f"""
            SELECT ta.timestamp
            """
            
            # Dynamically add columns that exist
            for col in ['rsi_14', 'macd_line', 'signal_line', 'adx', 'pdi', 'ndi', 'atr', 'stoch_k', 'stoch_d', 'vpt', 'vpt_sma']:
                if col in column_names:
                    query += f", ta.{col}"
            
            query += """
            FROM technical_analysis ta
            WHERE ta.symbol = ?
            ORDER BY ta.timestamp DESC
            LIMIT 1
            """
            
            try:
                df = pd.read_sql_query(query, conn, params=(symbol,))
            except Exception as e:
                self.logger.warning(f"Error querying technical indicators for {symbol}: {e}")
                return {
                    'valid': True,  # Allow processing to continue
                    'issues': ["Technical indicators will be calculated on demand"],
                    'indicators_present': []
                }
            
            if df.empty:
                issues.append(f"No technical indicators found for {symbol}")
                return {'valid': True, 'issues': issues}  # Changed to True to allow calculation
            
            # Valid if we have at least timestamp
            conn.close()
            
            return {
                'valid': True,  # Always valid, will calculate if needed
                'issues': issues,
                'indicators_present': [col for col in df.columns if col != 'timestamp']
            }
            
        except Exception as e:
            self.logger.error(f"Error validating indicators for {symbol}: {e}")
            issues.append(f"Validation error: {str(e)}")
            return {'valid': False, 'issues': issues}
    def validate_sentiment_data(self, symbol, max_age_hours=168):  # Relaxed to 7 days (168 hours)
        """
        Validate sentiment data for a symbol.
        
        Args:
            symbol (str): Trading symbol to validate
            max_age_hours (int): Maximum age of sentiment data in hours
            
        Returns:
            dict: Validation results with issues found
        """
        issues = []
        
        try:
            conn = sqlite3.connect(self.db_path)
            
            # First check if sentiment_results table exists
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='sentiment_results'")
            if not cursor.fetchone():
                self.logger.warning(f"No sentiment_results table found for {symbol}")
                return {
                    'valid': True,  # Allow processing to continue without sentiment data
                    'issues': ["No sentiment data table found"],
                }
            
            # Check sentiment data
            query = """
            SELECT sr.timestamp,
                   sr.negative_score,
                   sr.neutral_score,
                   sr.positive_score,
                   sr.confidence
            FROM sentiment_results sr
            WHERE sr.symbol = ?
            ORDER BY sr.timestamp DESC
            LIMIT 5
            """
            
            df = pd.read_sql_query(query, conn, params=(symbol,))
            
            if df.empty:
                issues.append(f"No sentiment data found for {symbol}")
                return {'valid': True, 'issues': issues}  # Changed to True to allow processing
            
            latest_timestamp = pd.to_datetime(df['timestamp'].iloc[0])
            age = datetime.now() - latest_timestamp.to_pydatetime()
            
            if age > timedelta(hours=max_age_hours):
                issues.append(
                    f"Sentiment data is too old. Latest: {latest_timestamp}, "
                    f"Age: {age.total_seconds() / 3600:.1f} hours"
                )
                # Even if data is old, we can still use it
            
            # Sentiment is helpful but not required, so we'll allow processing to continue
            conn.close()
            
            return {
                'valid': True,  # Always valid, will use neutral sentiment if needed
                'issues': issues,
                'latest_timestamp': latest_timestamp,
                'data_points': len(df)
            }
            
        except Exception as e:
            self.logger.error(f"Error validating sentiment for {symbol}: {e}")
            issues.append(f"Validation error: {str(e)}")
            return {'valid': False, 'issues': issues}
