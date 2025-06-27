"""
Test script for the enhanced SignalGenerator.

Tests the following features:
- Market condition detection
- Adaptive weights
- Technical indicator integration
- Signal generation with market context
- Sentiment analysis integration
- ML prediction integration
- Error handling and data validation
"""

import logging
import datetime
import unittest
import sqlite3
import json
from signal_generator import SignalGenerator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

class TestSignalGenerator(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Set up test environment before all tests."""
        cls.db_path = "market_data.db"
        cls.generator = SignalGenerator(cls.db_path)
        cls.test_symbol = "^GDAXI"  # DAX index
        
    def setUp(self):
        """Set up test environment before each test."""
        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()

    def tearDown(self):
        """Clean up after each test."""
        self.conn.close()

    def test_market_condition_detection(self):
        """Test market condition detection logic."""
        conditions = ['trending', 'ranging', 'volatile']
        condition = self.generator._detect_market_condition(self.test_symbol)
        
        # Test basic functionality
        self.assertIn(condition, conditions)
        logger.info(f"Detected market condition: {condition}")
        
        # Test with different lookback periods
        condition_short = self.generator._detect_market_condition(self.test_symbol, lookback_periods=10)
        condition_long = self.generator._detect_market_condition(self.test_symbol, lookback_periods=30)
        self.assertIn(condition_short, conditions)
        self.assertIn(condition_long, conditions)

    def test_adaptive_weights(self):
        """Test weight adaptation based on market conditions."""
        # Test weight changes for different conditions
        for condition in ['trending', 'ranging', 'volatile']:
            weights = self.generator.market_weights[condition]
            self.assertEqual(sum(weights.values()), 1.0)  # Weights should sum to 1
            self.assertGreater(weights['technical'], 0)
            self.assertGreater(weights['ml'], 0)
            self.assertGreater(weights['sentiment'], 0)

    def test_technical_analysis_integration(self):
        """Test technical analysis signal generation."""
        tech_signals = self.generator._get_technical_signals(self.test_symbol)
        
        self.assertIsNotNone(tech_signals)
        if tech_signals:
            # Verify signal structure
            self.assertIn('signals', tech_signals)
            self.assertIn('confidence', tech_signals)
            
            # Test indicator weights
            condition = self.generator._detect_market_condition(self.test_symbol)
            weights = self.generator.indicator_weights[condition]
            self.assertEqual(sum(weights.values()), 1.0)

    def test_sentiment_analysis_integration(self):
        """Test sentiment analysis integration."""
        sentiment = self.generator._get_latest_sentiment(self.test_symbol)
        
        if sentiment:
            self.assertIn('dominant_sentiment', sentiment)
            self.assertIn('confidence', sentiment)
            self.assertGreaterEqual(sentiment['confidence'], 0)
            self.assertLessEqual(sentiment['confidence'], 1)

    def test_signal_generation(self):
        """Test full signal generation pipeline."""
        signals = self.generator.generate_signals([self.test_symbol])
        
        self.assertIsNotNone(signals)
        if signals:
            signal = signals[0]
            # Verify signal structure
            required_fields = ['symbol', 'signal_type', 'confidence', 'close_price', 'reason']
            for field in required_fields:
                self.assertIn(field, signal)
            
            # Verify confidence score
            self.assertGreaterEqual(signal['confidence'], 0)
            self.assertLessEqual(signal['confidence'], 1)
            
            # Verify reason contains market context
            self.assertIn('market condition', signal['reason'].lower())

    def test_error_handling(self):
        """Test error handling and data validation."""
        # Test with invalid symbol
        signals = self.generator.generate_signals(['INVALID'])
        self.assertEqual(len(signals), 0)
        
        # Test with empty symbol list
        signals = self.generator.generate_signals([])
        self.assertEqual(len(signals), 0)
        
        # Test with None symbol
        signals = self.generator.generate_signals([None])
        self.assertEqual(len(signals), 0)

    def test_performance_tracking(self):
        """Test signal performance tracking."""
        # Get latest signal
        signals = self.generator.generate_signals([self.test_symbol])
        
        if signals:
            signal = signals[0]
            # Verify signal is tracked in database
            self.cursor.execute("""
                SELECT * FROM signals 
                WHERE symbol = ? 
                ORDER BY timestamp DESC 
                LIMIT 1
            """, (self.test_symbol,))
            
            tracked_signal = self.cursor.fetchone()
            self.assertIsNotNone(tracked_signal)

if __name__ == '__main__':
    unittest.main(verbosity=2)
