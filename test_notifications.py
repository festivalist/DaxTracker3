"""
Test script for the enhanced notification system.
Tests all notification types and features.
"""

import time
from notification_system import TelegramNotifier
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_notifications():
    print("Starting notification system test...")
    
    # Initialize notifier
    notifier = TelegramNotifier()
    
    # Test 1: Basic Signal
    print("\n1. Testing basic trading signal...")
    basic_signal = {
        "symbol": "^GDAXI",
        "signal_type": "BUY",
        "confidence": 0.85,
        "close_price": 23487.50,
        "timestamp": "2025-06-17 16:45:00",
        "reason": "Strong bullish pattern detected with multiple confirming indicators",
        "technical_analysis": {
            "rsi": 58.5,
            "macd_line": 1.234,
            "signal_line": 0.567,
            "adx": 28.5
        }
    }
    success = notifier.send_signal(basic_signal)
    print(f"Basic signal sent: {'✅' if success else '❌'}")
    
    # Test 2: Sell Signal
    print("\n2. Testing sell signal...")
    sell_signal = {
        "symbol": "^GDAXI",
        "signal_type": "SELL",
        "confidence": 0.92,
        "close_price": 23495.75,
        "timestamp": "2025-06-17 16:46:00",
        "reason": "Bearish reversal pattern with high volume and overbought conditions",
        "technical_analysis": {
            "rsi": 72.3,
            "macd_line": -2.123,
            "signal_line": -1.234,
            "adx": 32.1
        }
    }
    success = notifier.send_signal(sell_signal)
    print(f"Sell signal sent: {'✅' if success else '❌'}")
    
    # Test 3: Error Notification
    print("\n3. Testing error notification...")
    success = notifier.send_error(
        "Database Connection Error",
        "Failed to connect to market_data.db: timeout after 30 seconds"
    )
    print(f"Error notification sent: {'✅' if success else '❌'}")
    
    # Test 4: Low Confidence Signal (should be filtered)
    print("\n4. Testing low confidence signal (should be filtered)...")
    low_conf_signal = {
        "symbol": "^GDAXI",
        "signal_type": "BUY",
        "confidence": 0.45,
        "close_price": 23490.25,
        "timestamp": "2025-06-17 16:47:00",
        "reason": "Weak bullish pattern with mixed indicators",
        "technical_analysis": {
            "rsi": 55.2,
            "macd_line": 0.123,
            "signal_line": 0.234,
            "adx": 15.7
        }
    }
    success = notifier.send_signal(low_conf_signal)
    print(f"Low confidence signal handled: {'✅' if success else '❌'}")
    
    # Test 5: Queue Processing
    print("\n5. Testing queue processing...")
    queued_messages = notifier.process_queue()
    print(f"Processed {queued_messages} queued messages")
    
    # Test 6: Daily Summary
    print("\n6. Testing daily summary...")
    success = notifier.send_daily_summary()
    print(f"Daily summary sent: {'✅' if success else '❌'}")
    
    print("\nNotification system test completed!")

if __name__ == "__main__":
    test_notifications()
