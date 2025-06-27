"""
Market Regime Detection Module

This module implements advanced market regime detection using various
technical and statistical measures to classify market conditions and
adapt trading strategies accordingly.
"""

import pandas as pd
import numpy as np
from scipy import stats
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class MarketRegimeDetector:
    """
    Detects and classifies market regimes using multiple indicators
    and statistical measures.
    """
    
    def __init__(self, lookback_period=20):
        """
        Initialize the market regime detector.
        
        Args:
            lookback_period (int): Period for calculating regime metrics
        """
        self.lookback_period = lookback_period
        
    def calculate_volatility_regime(self, prices):
        """
        Determines the volatility regime using ATR and historical volatility.
        
        Args:
            prices (pd.DataFrame): DataFrame with OHLC data
            
        Returns:
            str: Volatility regime classification
        """
        try:
            # Calculate daily returns
            returns = prices['close'].pct_change()
            
            # Calculate historical volatility
            hist_vol = returns.rolling(window=self.lookback_period).std() * np.sqrt(252)
            
            # Calculate ATR
            high_low = prices['high'] - prices['low']
            high_close = abs(prices['high'] - prices['close'].shift())
            low_close = abs(prices['low'] - prices['close'].shift())
            
            ranges = pd.concat([high_low, high_close, low_close], axis=1)
            true_range = ranges.max(axis=1)
            atr = true_range.rolling(window=self.lookback_period).mean()
            
            # Classify regime based on both metrics
            vol_percentile = hist_vol.rank(pct=True)
            atr_percentile = atr.rank(pct=True)
            
            if vol_percentile.iloc[-1] > 0.8 or atr_percentile.iloc[-1] > 0.8:
                return 'high_volatility'
            elif vol_percentile.iloc[-1] < 0.2 or atr_percentile.iloc[-1] < 0.2:
                return 'low_volatility'
            else:
                return 'normal_volatility'
                
        except Exception as e:
            logger.error(f"Error calculating volatility regime: {str(e)}")
            return 'normal_volatility'
            
    def detect_trend_regime(self, prices, short_period=20, long_period=50):
        """
        Detects the trend regime using multiple indicators.
        
        Args:
            prices (pd.DataFrame): DataFrame with OHLC data
            short_period (int): Short-term moving average period
            long_period (int): Long-term moving average period
            
        Returns:
            str: Trend regime classification
        """
        try:
            close = prices['close']
            
            # Calculate moving averages
            sma_short = close.rolling(window=short_period).mean()
            sma_long = close.rolling(window=long_period).mean()
            
            # Calculate ADX for trend strength
            high = prices['high']
            low = prices['low']
            
            # Calculate +DM and -DM
            high_diff = high - high.shift(1)
            low_diff = low.shift(1) - low
            
            plus_dm = pd.Series(0, index=high_diff.index)
            plus_dm[(high_diff > low_diff) & (high_diff > 0)] = high_diff
            
            minus_dm = pd.Series(0, index=low_diff.index)
            minus_dm[(low_diff > high_diff) & (low_diff > 0)] = low_diff
            
            # Calculate ATR
            tr = pd.concat([
                high - low,
                abs(high - close.shift(1)),
                abs(low - close.shift(1))
            ], axis=1).max(axis=1)
            
            atr = tr.rolling(window=14).mean()
            
            # Calculate +DI and -DI
            plus_di = 100 * (plus_dm.rolling(window=14).mean() / atr)
            minus_di = 100 * (minus_dm.rolling(window=14).mean() / atr)
            
            # Calculate ADX
            dx = 100 * abs(plus_di - minus_di) / (plus_di + minus_di)
            adx = dx.rolling(window=14).mean()
            
            # Determine trend regime
            if adx.iloc[-1] > 25:
                if sma_short.iloc[-1] > sma_long.iloc[-1]:
                    return 'strong_uptrend'
                else:
                    return 'strong_downtrend'
            elif adx.iloc[-1] < 20:
                return 'ranging'
            else:
                return 'weak_trend'
                
        except Exception as e:
            logger.error(f"Error detecting trend regime: {str(e)}")
            return 'unknown'
            
    def detect_momentum_regime(self, prices):
        """
        Detects the momentum regime using RSI and price momentum.
        
        Args:
            prices (pd.DataFrame): DataFrame with OHLC data
            
        Returns:
            str: Momentum regime classification
        """
        try:
            # Calculate returns
            returns = prices['close'].pct_change()
            
            # Calculate RSI
            delta = returns
            gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
            rs = gain / loss
            rsi = 100 - (100 / (1 + rs))
            
            # Calculate momentum
            momentum = (prices['close'] / prices['close'].shift(self.lookback_period) - 1) * 100
            
            # Classify momentum regime
            if rsi.iloc[-1] > 70 and momentum.iloc[-1] > 0:
                return 'strong_momentum'
            elif rsi.iloc[-1] < 30 and momentum.iloc[-1] < 0:
                return 'oversold'
            elif 40 <= rsi.iloc[-1] <= 60:
                return 'neutral'
            else:
                return 'mixed'
                
        except Exception as e:
            logger.error(f"Error detecting momentum regime: {str(e)}")
            return 'unknown'
            
    def get_market_regime(self, prices):
        """
        Determines the overall market regime combining all metrics.
        
        Args:
            prices (pd.DataFrame): DataFrame with OHLC data
            
        Returns:
            dict: Market regime classification and confidence
        """
        try:
            volatility = self.calculate_volatility_regime(prices)
            trend = self.detect_trend_regime(prices)
            momentum = self.detect_momentum_regime(prices)
            
            # Combine regime information
            regime_info = {
                'volatility_regime': volatility,
                'trend_regime': trend,
                'momentum_regime': momentum,
                'timestamp': pd.Timestamp.now()
            }
            
            # Add overall market condition
            if trend in ['strong_uptrend', 'strong_downtrend']:
                regime_info['market_condition'] = 'trending'
            elif volatility == 'high_volatility':
                regime_info['market_condition'] = 'volatile'
            elif trend == 'ranging' and volatility != 'high_volatility':
                regime_info['market_condition'] = 'ranging'
            else:
                regime_info['market_condition'] = 'mixed'
            
            return regime_info
            
        except Exception as e:
            logger.error(f"Error determining market regime: {str(e)}")
            return {
                'market_condition': 'unknown',
                'error': str(e),
                'timestamp': pd.Timestamp.now()
            }
