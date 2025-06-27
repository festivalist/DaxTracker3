"""
Script to send a test signal via Telegram, bypassing quiet hours and weekend checks
"""

import asyncio
from notification_system import TelegramNotifier
import datetime
import sqlite3
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def send_test_signal():
    print("Sending test signal via Telegram...")
    
    # Initialize notifier
    notifier = TelegramNotifier()
    
    # Temporarily disable quiet hours and weekend checks
    original_quiet_hours = notifier.config['quiet_hours']['enabled']
    original_weekends = notifier.config['weekends']['enabled']
    
    try:
        # Disable restrictions
        notifier.config['quiet_hours']['enabled'] = False
        notifier.config['weekends']['enabled'] = False
        
        # Create test signal
        test_signal = {
            "symbol": "TEST",
            "signal_type": "BUY",
            "confidence": 0.95,
            "close_price": 500.25,
            "timestamp": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "reason": "This is a test signal from DaxTracker3",
            "technical_analysis": {
                "rsi": 60.5,
                "macd_line": 2.345,
                "signal_line": 1.234,
                "adx": 30.2
            }
        }
        
        # Send the signal directly
        print("Sending signal to Telegram...")
        success = notifier.send_signal(test_signal)
        
        # Also insert into the database so it shows in the dashboard
        conn = sqlite3.connect('market_data.db')
        cursor = conn.cursor()
        cursor.execute("""
        INSERT INTO trading_signals 
        (symbol, timestamp, signal_type, confidence, close_price, technical_signal, reason, notified)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, ('TEST', test_signal['timestamp'], 'BUY', 0.95, 500.25, 'BUY', 'This is a test signal from DaxTracker3', 1))
        conn.commit()
        conn.close()
        
        return success
    finally:
        # Restore original settings
        notifier.config['quiet_hours']['enabled'] = original_quiet_hours
        notifier.config['weekends']['enabled'] = original_weekends

if __name__ == "__main__":
    success = send_test_signal()
    if success:
        print("✅ Test signal sent successfully via Telegram!")
    else:
        print("❌ Failed to send test signal.")
