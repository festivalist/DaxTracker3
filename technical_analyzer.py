"""
Technical Analysis Module

This module implements technical analysis functionalities for market data.
It calculates various technical indicators and generates analysis results
that can be used for trading signal generation.

The module uses pandas for data manipulation and numpy for calculations.
It reads market data from a SQLite database and computes indicators like:
- Simple Moving Averages (SMA)
- Exponential Moving Averages (EMA)
- Relative Strength Index (RSI)
- Moving Average Convergence Divergence (MACD)
- Stochastic Oscillator
- Average Directional Index (ADX)
- Volume Price Trend (VPT)
- EMA Ribbon
"""

import pandas as pd
import numpy as np
import sqlite3
import datetime
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class TechnicalAnalyzer:
    """
    A class that performs technical analysis on market data.
    
    This class provides methods to calculate various technical indicators
    and generate analysis results that can be used for trading decisions.
    
    Attributes:
        db_path (str): Path to the SQLite database containing market data
    """
    
    def __init__(self, db_path):
        """
        Initializes the TechnicalAnalyzer with a database path.
        
        Args:
            db_path (str): Path to the SQLite database file
        """
        self.db_path = db_path
    
    def _validate_market_data(self, df):
        """
        Validates market data for edge cases and applies necessary corrections.
        
        Args:
            df (pandas.DataFrame): Market data DataFrame
            
        Returns:
            pandas.DataFrame: Validated and corrected DataFrame
            None: If data is invalid or uncorrectable
        """
        if df is None or df.empty:
            logger.error("Empty or None DataFrame provided")
            return None
            
        try:
            # Replace infinite values with NaN
            df = df.replace([np.inf, -np.inf], np.nan)
            
            # Handle zero volumes
            if 'volume' in df.columns:
                # Replace zero volumes with the mean of non-zero volumes
                non_zero_mean = df[df['volume'] > 0]['volume'].mean()
                df.loc[df['volume'] == 0, 'volume'] = non_zero_mean
                logger.info(f"Replaced {len(df[df['volume'] == 0])} zero volume entries")
            
            # Check for extreme price movements (more than 20% in one period)
            price_changes = df['close'].pct_change().abs()
            extreme_moves = price_changes > 0.20
            if extreme_moves.any():
                logger.warning(f"Detected {extreme_moves.sum()} extreme price movements")
                
            # Interpolate missing values
            df = df.interpolate(method='linear', limit=5)
            
            # Drop remaining NaN values if any
            initial_rows = len(df)
            df = df.dropna()
            dropped_rows = initial_rows - len(df)
            if dropped_rows > 0:
                logger.warning(f"Dropped {dropped_rows} rows with NaN values")
            
            return df
            
        except Exception as e:
            logger.error(f"Error in data validation: {str(e)}")
            return None

    def _get_market_data(self, symbol, days=30):
        """
        Retrieves market data for a symbol from the database.
        
        This internal method fetches OHLCV data for the specified
        number of days up to the current date.
        
        Args:
            symbol (str): Trading symbol to fetch data for
            days (int, optional): Number of days of historical data. Defaults to 30.
            
        Returns:
            pandas.DataFrame: DataFrame with columns:
                - timestamp
                - open
                - high
                - low
                - close
                - volume
            Returns None if no data is found.
        """
        try:
            conn = sqlite3.connect(self.db_path)
            end_date = datetime.datetime.now()
            start_date = end_date - datetime.timedelta(days=days)
            
            query = f"""
            SELECT timestamp, open, high, low, close, volume
            FROM market_data
            WHERE symbol = '{symbol}'
            AND timestamp >= '{start_date.strftime('%Y-%m-%d')}'
            ORDER BY timestamp
            """
            
            df = pd.read_sql_query(query, conn)
            conn.close()
            
            if df.empty:
                logger.warning(f"No market data found for {symbol}")
                return None
            
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df.set_index('timestamp', inplace=True)
            
            # Add validation step
            df = self._validate_market_data(df)
            if df is None:
                return None
                
            return df
        except Exception as e:
            logger.error(f"Error fetching market data: {str(e)}")
            return None
    
    def calculate_sma(self, df, window):
        """
        Calculates the Simple Moving Average (SMA) for the closing prices.
        
        Args:
            df (pandas.DataFrame): DataFrame containing market data with a 'close' column
            window (int): Window size for the moving average
            
        Returns:
            pandas.Series: Series containing the SMA values
        """
        return df['close'].rolling(window=window).mean()
    
    def calculate_ema(self, df, window):
        """
        Calculates the Exponential Moving Average (EMA) for the closing prices.
        
        Args:
            df (pandas.DataFrame): DataFrame containing market data with a 'close' column
            window (int): Window size for the moving average
            
        Returns:
            pandas.Series: Series containing the EMA values
        """
        return df['close'].ewm(span=window, adjust=False).mean()
    
    def calculate_rsi(self, df, window=14):
        """
        Calculates the Relative Strength Index (RSI) for the closing prices.
        
        Args:
            df (pandas.DataFrame): DataFrame containing market data with a 'close' column
            window (int, optional): Window size for the RSI calculation. Defaults to 14.
            
        Returns:
            pandas.Series: Series containing the RSI values
        """
        delta = df['close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=window).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=window).mean()
        
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        return rsi
    
    def calculate_macd(self, df, fast=12, slow=26, signal=9):
        """
        Calculates the Moving Average Convergence Divergence (MACD) for the closing prices.
        
        Args:
            df (pandas.DataFrame): DataFrame containing market data with a 'close' column
            fast (int, optional): Window size for the fast EMA. Defaults to 12.
            slow (int, optional): Window size for the slow EMA. Defaults to 26.
            signal (int, optional): Window size for the signal line EMA. Defaults to 9.
            
        Returns:
            dict: Dictionary containing Series for 'macd_line', 'signal_line', and 'histogram'
        """
        ema_fast = df['close'].ewm(span=fast, adjust=False).mean()
        ema_slow = df['close'].ewm(span=slow, adjust=False).mean()
        macd_line = ema_fast - ema_slow
        signal_line = macd_line.ewm(span=signal, adjust=False).mean()
        histogram = macd_line - signal_line
        
        return {
            'macd_line': macd_line,
            'signal_line': signal_line,
            'histogram': histogram
        }
    
    def calculate_bollinger_bands(self, df, window=20, num_std=2):
        """
        Calculates the Bollinger Bands for the closing prices.
        
        Args:
            df (pandas.DataFrame): DataFrame containing market data with a 'close' column
            window (int, optional): Window size for the SMA used in Bollinger Bands. Defaults to 20.
            num_std (int, optional): Number of standard deviations for the bands. Defaults to 2.
            
        Returns:
            dict: Dictionary containing Series for 'middle_band', 'upper_band', and 'lower_band'
        """
        sma = self.calculate_sma(df, window)
        std = df['close'].rolling(window=window).std()
        upper_band = sma + (std * num_std)
        lower_band = sma - (std * num_std)
        
        return {
            'middle_band': sma,
            'upper_band': upper_band,
            'lower_band': lower_band
        }
    
    def calculate_stochastic(self, df, k_period=14, d_period=3):
        """
        Calculates the Stochastic Oscillator for price data.
        
        The Stochastic Oscillator is a momentum indicator comparing a particular closing price
        of a security to a range of its prices over time. Values above 80 indicate overbought
        conditions, while values below 20 indicate oversold conditions.
        
        Args:
            df (pandas.DataFrame): DataFrame with OHLC data
            k_period (int): The look-back period for %K. Defaults to 14
            d_period (int): The period for %D (the SMA of %K). Defaults to 3
            
        Returns:
            dict: Dictionary containing 'k_line' and 'd_line' Series
        """
        try:
            # Calculate %K
            lowest_low = df['low'].rolling(window=k_period).min()
            highest_high = df['high'].rolling(window=k_period).max()
            k_line = 100 * ((df['close'] - lowest_low) / (highest_high - lowest_low))
            
            # Calculate %D
            d_line = k_line.rolling(window=d_period).mean()
            
            return {
                'k_line': k_line,
                'd_line': d_line
            }
        except Exception as e:
            logger.error(f"Error calculating Stochastic: {str(e)}")
            return None
            
    def calculate_adx(self, df, period=14):
        """
        Calculates the Average Directional Index (ADX) for trend strength.
        
        ADX is used to quantify trend strength. Values above 25 suggest a strong trend,
        while values below 20 suggest a weak or non-trending market.
        
        Args:
            df (pandas.DataFrame): DataFrame with OHLC data
            period (int): The period for calculations. Defaults to 14
            
        Returns:
            dict: Dictionary containing 'adx', 'di_plus', and 'di_minus' Series
        """
        try:
            # Calculate True Range
            df['high_low'] = df['high'] - df['low']
            df['high_close'] = abs(df['high'] - df['close'].shift())
            df['low_close'] = abs(df['low'] - df['close'].shift())
            df['tr'] = df[['high_low', 'high_close', 'low_close']].max(axis=1)
            
            # Calculate +DM and -DM
            df['up_move'] = df['high'] - df['high'].shift()
            df['down_move'] = df['low'].shift() - df['low']
            
            df['plus_dm'] = np.where(
                (df['up_move'] > df['down_move']) & (df['up_move'] > 0),
                df['up_move'],
                0
            )
            
            df['minus_dm'] = np.where(
                (df['down_move'] > df['up_move']) & (df['down_move'] > 0),
                df['down_move'],
                0
            )
            
            # Calculate smoothed averages
            atr = df['tr'].ewm(span=period, adjust=False).mean()
            plus_di = 100 * (df['plus_dm'].ewm(span=period, adjust=False).mean() / atr)
            minus_di = 100 * (df['minus_dm'].ewm(span=period, adjust=False).mean() / atr)
            
            # Calculate ADX
            dx = 100 * abs(plus_di - minus_di) / (plus_di + minus_di)
            adx = dx.ewm(span=period, adjust=False).mean()
            
            return {
                'adx': adx,
                'di_plus': plus_di,
                'di_minus': minus_di
            }
        except Exception as e:
            logger.error(f"Error calculating ADX: {str(e)}")
            return None
            
    def calculate_vpt(self, df):
        """
        Calculates the Volume Price Trend (VPT) indicator.
        
        VPT is a volume-based indicator that shows whether a trend is supported
        by volume. Strong volume in the trend direction confirms the trend.
        
        Args:
            df (pandas.DataFrame): DataFrame with OHLC and volume data
            
        Returns:
            pandas.Series: The VPT values
        """
        try:
            close = df['close']
            volume = df['volume']
            
            # Calculate percentage price change
            price_change = close.pct_change()
            
            # Calculate VPT
            vpt = volume * price_change
            vpt = vpt.cumsum()
            
            return vpt
        except Exception as e:
            logger.error(f"Error calculating VPT: {str(e)}")
            return None
            
    def calculate_ema_ribbon(self, df, periods=[5, 10, 21, 50, 100, 200]):
        """
        Calculates a series of EMAs to form a price ribbon.
        
        The EMA ribbon shows the overall trend structure and potential
        support/resistance levels. When shorter EMAs cross above longer ones,
        it's bullish, and vice versa.
        
        Args:
            df (pandas.DataFrame): DataFrame with OHLC data
            periods (list): List of periods for the EMAs
            
        Returns:
            dict: Dictionary containing EMA Series for each period
        """
        try:
            emas = {}
            for period in periods:
                emas[f'ema_{period}'] = df['close'].ewm(span=period, adjust=False).mean()
            return emas
        except Exception as e:
            logger.error(f"Error calculating EMA ribbon: {str(e)}")
            return None
    
    def analyze_symbol(self, symbol):
        """
        Analyzes a given symbol and generates technical analysis results.
        
        This method fetches market data for the symbol, calculates various
        technical indicators, and generates buy/sell signals based on a
        comprehensive analysis of multiple indicators.
        
        Args:
            symbol (str): Trading symbol to analyze
            
        Returns:
            dict: Dictionary containing analysis results, including:
                - symbol
                - latest_close
                - indicators (technical indicator values)
                - signals (individual indicator signals)
                - overall_signal (BUY/SELL/NEUTRAL)
                - signal_strength (strength of the signal)
                - timestamp (analysis timestamp)
            Returns None if not enough data is available.
        """
        df = self._get_market_data(symbol)
        if df is None or len(df) < 30:
            logger.warning(f"Insufficient data for {symbol}")
            return None
        
        try:
            # Calculate all technical indicators
            sma_20 = self.calculate_sma(df, 20)
            sma_50 = self.calculate_sma(df, 50)
            ema_12 = self.calculate_ema(df, 12)
            ema_26 = self.calculate_ema(df, 26)
            rsi = self.calculate_rsi(df)
            macd = self.calculate_macd(df)
            bollinger = self.calculate_bollinger_bands(df)
            stoch = self.calculate_stochastic(df)
            adx = self.calculate_adx(df)
            vpt = self.calculate_vpt(df)
            ema_ribbon = self.calculate_ema_ribbon(df)
            
            # Generate signals
            signals = {}
            
            # 1. Trend Signals
            
            # SMA trend with momentum
            sma_trend = 'BUY' if sma_20.iloc[-1] > sma_50.iloc[-1] else 'SELL'
            sma_direction = sma_20.diff().iloc[-1]
            signals['sma_crossover'] = sma_trend if (sma_direction > 0) == (sma_trend == 'BUY') else 'NEUTRAL'
            
            # EMA Crossover with volume
            ema_trend = 'BUY' if ema_12.iloc[-1] > ema_26.iloc[-1] else 'SELL'
            volume_confirm = df['volume'].iloc[-1] > df['volume'].rolling(window=5).mean().iloc[-1]
            signals['ema_crossover'] = ema_trend if volume_confirm else 'NEUTRAL'
            
            # EMA Ribbon
            short_emas = [ema_ribbon[f'ema_{p}'].iloc[-1] for p in [5, 10, 21]]
            long_emas = [ema_ribbon[f'ema_{p}'].iloc[-1] for p in [50, 100, 200]]
            ribbon_alignment = all(s > l for s in short_emas for l in long_emas)
            signals['ema_ribbon'] = 'BUY' if ribbon_alignment else 'SELL'
            
            # 2. Momentum Signals
            
            # RSI with trend alignment
            if rsi.iloc[-1] < 30 and sma_trend == 'BUY':
                signals['rsi'] = 'BUY'  # Oversold with uptrend
            elif rsi.iloc[-1] > 70 and sma_trend == 'SELL':
                signals['rsi'] = 'SELL'  # Overbought with downtrend
            else:
                signals['rsi'] = 'NEUTRAL'
            
            # Stochastic
            if (stoch['k_line'].iloc[-1] < 20 and stoch['k_line'].iloc[-1] > stoch['d_line'].iloc[-1]):
                signals['stochastic'] = 'BUY'  # Oversold with bullish crossover
            elif (stoch['k_line'].iloc[-1] > 80 and stoch['k_line'].iloc[-1] < stoch['d_line'].iloc[-1]):
                signals['stochastic'] = 'SELL'  # Overbought with bearish crossover
            else:
                signals['stochastic'] = 'NEUTRAL'
            
            # 3. Trend Confirmation Signals
            
            # MACD
            signals['macd'] = 'BUY' if macd['macd_line'].iloc[-1] > macd['signal_line'].iloc[-1] else 'SELL'
            
            # ADX Trend Strength
            adx_value = adx['adx'].iloc[-1]
            if adx_value > 25:  # Strong trend
                if adx['di_plus'].iloc[-1] > adx['di_minus'].iloc[-1]:
                    signals['adx'] = 'BUY'
                else:
                    signals['adx'] = 'SELL'
            else:
                signals['adx'] = 'NEUTRAL'  # Weak trend
            
            # Volume Price Trend
            vpt_signal = vpt.diff().iloc[-1]
            signals['vpt'] = 'BUY' if vpt_signal > 0 else 'SELL'
            
            # Weighted Signal Calculation
            signal_weights = {
                'sma_crossover': 0.15,    # Long-term trend
                'ema_crossover': 0.15,    # Short-term trend with volume
                'ema_ribbon': 0.15,       # Overall trend structure
                'rsi': 0.10,             # Momentum
                'stochastic': 0.10,      # Additional momentum confirmation
                'macd': 0.15,            # Trend/Momentum combination
                'adx': 0.10,             # Trend strength
                'vpt': 0.10              # Volume confirmation
            }
            
            # Calculate weighted signals with trend strength modifier
            adx_modifier = min(adx['adx'].iloc[-1] / 25.0, 1.5)  # Stronger signals in strong trends
            
            weighted_buy = sum(signal_weights[k] for k, v in signals.items() if v == 'BUY') * adx_modifier
            weighted_sell = sum(signal_weights[k] for k, v in signals.items() if v == 'SELL') * adx_modifier
            
            # Determine overall signal with enhanced confidence calculation
            if weighted_buy > weighted_sell and weighted_buy > 0.4:  # Minimum threshold
                overall_signal = 'BUY'
                signal_strength = min(weighted_buy, 1.0)  # Cap at 1.0
            elif weighted_sell > weighted_buy and weighted_sell > 0.4:
                overall_signal = 'SELL'
                signal_strength = min(weighted_sell, 1.0)
            else:
                overall_signal = 'NEUTRAL'
                signal_strength = max(weighted_buy, weighted_sell)
            
            # Compile results
            results = {
                'symbol': symbol,
                'latest_close': df['close'].iloc[-1],
                'indicators': {
                    'sma_20': sma_20.iloc[-1],
                    'sma_50': sma_50.iloc[-1],
                    'rsi': rsi.iloc[-1],
                    'macd_line': macd['macd_line'].iloc[-1],
                    'signal_line': macd['signal_line'].iloc[-1],
                    'stoch_k': stoch['k_line'].iloc[-1],
                    'stoch_d': stoch['d_line'].iloc[-1],
                    'adx': adx['adx'].iloc[-1],
                    'di_plus': adx['di_plus'].iloc[-1],
                    'di_minus': adx['di_minus'].iloc[-1],
                    'vpt': vpt.iloc[-1],
                    'vpt_momentum': vpt_signal,
                    'volume': df['volume'].iloc[-1],
                    'volume_sma': df['volume'].rolling(window=5).mean().iloc[-1]
                },
                'signals': signals,
                'overall_signal': overall_signal,
                'signal_strength': signal_strength,
                'timestamp': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
            logger.info(f"Analysis complete for {symbol}: {overall_signal} ({signal_strength:.2f})")
            
            # Save results to the database
            self.save_results(results)
            
            return results
            
        except Exception as e:
            logger.error(f"Error analyzing {symbol}: {str(e)}")
            return None
    
    def save_results(self, results):
        """
        Saves technical analysis results to the database.
        
        Args:
            results (dict): Analysis results from analyze_symbol method
            
        Returns:
            bool: True if saved successfully, False otherwise
        """
        if not results:
            return False
            
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Check if record exists for this symbol and timestamp
            cursor.execute('''
            SELECT id FROM technical_analysis 
            WHERE symbol = ? AND timestamp = ?
            ''', (results['symbol'], results['timestamp']))
            
            existing_id = cursor.fetchone()
            
            if existing_id:
                # Update existing record
                cursor.execute('''
                UPDATE technical_analysis SET 
                close_price = ?,
                sma_20 = ?,
                sma_50 = ?,
                rsi = ?,
                macd_line = ?,
                signal_line = ?,
                overall_signal = ?
                WHERE id = ?
                ''', (
                    results['latest_close'],
                    results['indicators']['sma_20'],
                    results['indicators']['sma_50'],
                    results['indicators']['rsi'],
                    results['indicators']['macd_line'],
                    results['indicators']['signal_line'],
                    results['overall_signal'],
                    existing_id[0]
                ))
            else:
                # Insert new record
                cursor.execute('''
                INSERT INTO technical_analysis
                (symbol, timestamp, close_price, sma_20, sma_50, rsi, macd_line, signal_line, overall_signal)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    results['symbol'],
                    results['timestamp'],
                    results['latest_close'],
                    results['indicators']['sma_20'],
                    results['indicators']['sma_50'],
                    results['indicators']['rsi'],
                    results['indicators']['macd_line'],
                    results['indicators']['signal_line'],
                    results['overall_signal']
                ))
            
            conn.commit()
            conn.close()
            return True
            
        except Exception as e:
            logger.error(f"Error saving analysis results: {str(e)}")
            return False
            
    def _validate_numeric_data(self, data, column_name):
        """
        Validates numeric data and handles edge cases.
        
        Args:
            data (pd.Series): Data series to validate
            column_name (str): Name of the column for logging
            
        Returns:
            pd.Series: Cleaned and validated data
        """
        try:
            # Convert to numeric, coerce errors to NaN
            cleaned_data = pd.to_numeric(data, errors='coerce')
            
            # Log information about data quality
            nan_count = cleaned_data.isna().sum()
            if nan_count > 0:
                logger.warning(f"{nan_count} NaN values found in {column_name}")
            
            # Replace infinite values with NaN
            inf_mask = np.isinf(cleaned_data)
            if inf_mask.any():
                logger.warning(f"{inf_mask.sum()} infinite values found in {column_name}")
                cleaned_data[inf_mask] = np.nan
            
            # Handle zero values for division operations
            if column_name in ['volume', 'close']:
                zero_mask = (cleaned_data == 0)
                if zero_mask.any():
                    logger.warning(f"{zero_mask.sum()} zero values found in {column_name}")
                    # Replace zeros with previous non-zero value
                    cleaned_data[zero_mask] = cleaned_data.replace(0, method='ffill')
            
            return cleaned_data
            
        except Exception as e:
            logger.error(f"Error validating {column_name}: {str(e)}")
            raise ValueError(f"Data validation failed for {column_name}")

    def _validate_timestamp_data(self, data):
        """
        Validates timestamp data for consistency and gaps.
        
        Args:
            data (pd.DataFrame): Data to validate timestamps for
            
        Returns:
            pd.DataFrame: Data with validated timestamps
        """
        try:
            # Convert to datetime if needed
            if not pd.api.types.is_datetime64_any_dtype(data.index):
                data.index = pd.to_datetime(data.index)
            
            # Check for duplicates
            duplicates = data.index.duplicated()
            if duplicates.any():
                logger.warning(f"Found {duplicates.sum()} duplicate timestamps")
                # Keep last occurrence of duplicates
                data = data[~duplicates]
            
            # Check for gaps
            time_diff = data.index.to_series().diff()
            expected_diff = pd.Timedelta(minutes=1)  # Adjust based on your data frequency
            gaps = time_diff > expected_diff
            if gaps.any():
                gap_count = gaps.sum()
                logger.warning(f"Found {gap_count} gaps in time series data")
            
            return data
            
        except Exception as e:
            logger.error(f"Error validating timestamps: {str(e)}")
            raise ValueError("Timestamp validation failed")
    
    def calculate_indicator(self, df, indicator_func, **kwargs):
        """
        Safely calculates technical indicators with error handling.
        
        Args:
            df (pd.DataFrame): Input data
            indicator_func (callable): Function to calculate the indicator
            **kwargs: Additional arguments for the indicator function
            
        Returns:
            pd.Series: Calculated indicator values
        """
        try:
            # Validate input data
            for col in ['close', 'high', 'low', 'volume']:
                if col in df.columns:
                    df[col] = self._validate_numeric_data(df[col], col)
            
            # Validate timestamps
            df = self._validate_timestamp_data(df)
            
            # Calculate indicator with validated data
            result = indicator_func(df, **kwargs)
            
            # Post-calculation validation
            if isinstance(result, pd.Series):
                result = self._validate_numeric_data(result, indicator_func.__name__)
            elif isinstance(result, pd.DataFrame):
                for col in result.columns:
                    result[col] = self._validate_numeric_data(result[col], f"{indicator_func.__name__}_{col}")
            
            return result
            
        except Exception as e:
            logger.error(f"Error calculating {indicator_func.__name__}: {str(e)}")
            raise ValueError(f"Indicator calculation failed: {indicator_func.__name__}")
            
    def _handle_data_gap(self, data, method='ffill'):
        """
        Handles missing data points in time series.
        
        Args:
            data (pd.Series): Data series with gaps
            method (str): Fill method ('ffill', 'bfill', or 'interpolate')
            
        Returns:
            pd.Series: Data with gaps handled
        """
        if method == 'interpolate':
            return data.interpolate(method='time')
        return data.fillna(method=method)
