"""
Notification System Module

This module handles the delivery of trading signals and alerts through various
notification channels. Currently, it implements Telegram as the primary
notification method.

The module supports features like:
- Quiet hours configuration
- Weekend handling
- Message                    asyncio.run(self.bot.send_message(
                        chat_id=self.chat_id,
                        text=items['content'],
                        parse_mode=ParseMode.MARKDOWN
                    ))atting with templates
- Rate limiting
- Message queueing
- Notification history tracking
- Error handling and logging

Example:
    notifier = TelegramNotifier(config_file='notification_config.json')
    notifier.send_signal({
        "symbol": "^GDAXI",
        "signal_type": "BUY",
        "confidence": 0.95
    })
"""

import logging
import json
import os
import datetime
import asyncio
from collections import deque
from telegram import Bot
from telegram.constants import ParseMode
from telegram.error import TelegramError
import sqlite3
from notification_templates import TELEGRAM_TEMPLATES
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class NotificationQueue:
    """Queue for managing notifications during quiet hours."""
    
    def __init__(self, max_size=100):
        self.queue = deque(maxlen=max_size)
    
    def add(self, message_data):
        self.queue.append(message_data)
    
    def get_pending(self):
        return list(self.queue)
    
    def clear(self):
        self.queue.clear()

class NotificationHistory:
    """Tracks notification history in SQLite database."""
    
    def __init__(self, db_path='market_data.db'):
        self.db_path = db_path
        self._create_table()
    
    def _create_table(self):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS notification_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            type TEXT NOT NULL,
            symbol TEXT,
            message TEXT NOT NULL,
            success BOOLEAN NOT NULL
        )
        ''')
        conn.commit()
        conn.close()
    
    def add(self, notification_type, symbol, message, success):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('''
        INSERT INTO notification_history (timestamp, type, symbol, message, success)
        VALUES (?, ?, ?, ?, ?)
        ''', (datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
              notification_type, symbol, message, success))
        conn.commit()
        conn.close()
    
    def get_recent(self, hours=24):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        since = (datetime.datetime.now() - datetime.timedelta(hours=hours)).strftime('%Y-%m-%d %H:%M:%S')
        cursor.execute('''
        SELECT * FROM notification_history
        WHERE timestamp > ?
        ORDER BY timestamp DESC
        ''', (since,))
        results = cursor.fetchall()
        conn.close()
        return results

class TelegramNotifier:
    """
    A class that handles sending notifications via Telegram.
    
    This class manages the connection to Telegram's API and handles
    message delivery with features like quiet hours, rate limiting,
    and message queueing.
    
    Attributes:
        token (str): Telegram bot API token
        chat_id (str): Telegram chat ID to send messages to
        config_file (str): Path to the configuration JSON file
        bot (telegram.Bot): Telegram bot instance
        config (dict): Notification configuration settings
        queue (NotificationQueue): Queue for delayed notifications
        history (NotificationHistory): Notification history tracker
    """
    
    def __init__(self, token=None, chat_id=None, config_file='notification_config.json'):
        """
        Initializes the TelegramNotifier with optional token and chat ID.
        
        If token and chat_id are not provided, they will be loaded from environment
        variables or the configuration file.
        
        Args:
            token (str, optional): Telegram bot API token
            chat_id (str, optional): Telegram chat ID
            config_file (str, optional): Path to config file
        """
        self.token = token or os.getenv('TELEGRAM_TOKEN')
        self.chat_id = chat_id or os.getenv('TELEGRAM_CHAT_ID')
        self.config_file = config_file
        self.bot = None
        self.rate_limit_count = 0
        self.rate_limit_reset = datetime.datetime.now()
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        
        if self.token and self.chat_id:
            try:
                self.bot = Bot(token=self.token)
            except Exception as e:
                logging.error(f"Failed to initialize Telegram bot: {e}")
        
        self.config = self._load_config()
        self.queue = NotificationQueue()
        self.history = NotificationHistory()
    
    def _escape_markdown(self, text):
        """Escape special characters in text for Markdown formatting."""
        special_chars = ['_', '*', '[', ']', '(', ')', '~', '`', '>', '#', '+', '-', '=', '|', '{', '}', '.', '!']
        for char in special_chars:
            text = text.replace(char, f'\\{char}')
        return text

    async def _send_message(self, chat_id, text, parse_mode=ParseMode.MARKDOWN):
        """Helper method to send messages asynchronously."""
        return await self.bot.send_message(
            chat_id=chat_id,
            text=text,
            parse_mode=parse_mode
        )
    
    def _load_config(self):
        """Loads notification configuration from JSON file."""
        default_config = {
            'quiet_hours': {
                'enabled': True,
                'start': '22:00',
                'end': '07:30'
            },
            'weekends': {
                'enabled': True,
                'collect_for_monday': True
            },
            'minimum_confidence': 0.7,
            'rate_limit': {
                'max_per_hour': 20,
                'max_per_day': 100
            },
            'notification_history': {
                'enabled': True,
                'max_age_days': 30
            }
        }
        
        if os.path.exists(self.config_file):
            try:
                with open(self.config_file, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logging.error(f"Error loading config: {e}")
        
        with open(self.config_file, 'w') as f:
            json.dump(default_config, f, indent=4)
        return default_config
    
    def _is_quiet_hours(self):
        """Checks if current time is within quiet hours."""
        if not self.config['quiet_hours']['enabled']:
            return False
        
        now = datetime.datetime.now().time()
        start = datetime.datetime.strptime(self.config['quiet_hours']['start'], '%H:%M').time()
        end = datetime.datetime.strptime(self.config['quiet_hours']['end'], '%H:%M').time()
        
        if start <= end:
            return start <= now <= end
        else:  # Handle overnight quiet hours
            return now >= start or now <= end
    
    def _is_weekend(self):
        """Checks if current day is a weekend."""
        if not self.config['weekends']['enabled']:
            return False
        return datetime.datetime.now().weekday() >= 5
    
    def _check_rate_limit(self):
        """Checks if rate limit is exceeded."""
        now = datetime.datetime.now()
        
        # Reset counter if an hour has passed
        if (now - self.rate_limit_reset).total_seconds() > 3600:
            self.rate_limit_count = 0
            self.rate_limit_reset = now
        
        return self.rate_limit_count < self.config['rate_limit']['max_per_hour']
    
    def _format_signal_message(self, signal):
        """Formats the trading signal message using template."""
        emoji_map = {'BUY': '🟢', 'SELL': '🔴', 'NEUTRAL': '⚪️'}
        
        # Prepare template variables
        template_vars = {
            'emoji': emoji_map.get(signal['signal_type'], '⚪️'),
            'symbol': signal['symbol'],
            'signal_type': signal['signal_type'],
            'close_price': signal['close_price'],
            'confidence_pct': int(signal['confidence'] * 100),
            'timestamp': signal['timestamp'],
            'reason': signal['reason'],
            'signal_type_lower': signal['signal_type'].lower(),
            'rsi': signal.get('technical_analysis', {}).get('rsi', 0),
            'macd': signal.get('technical_analysis', {}).get('macd_line', 0),
            'signal_line': signal.get('technical_analysis', {}).get('signal_line', 0),
            'adx': signal.get('technical_analysis', {}).get('adx', 0)
        }
        
        return TELEGRAM_TEMPLATES['signal'].format(**template_vars)
    
    def send_signal(self, signal):
        """
        Sends the trading signal to the configured Telegram chat.
        
        Handles quiet hours, rate limiting, and message queueing.
        
        Args:
            signal (dict): Signal data to be sent
            
        Returns:
            bool: True if message was sent or queued successfully
        """
        if not self.bot:
            logging.warning("Telegram bot not configured")
            return False
        
        # Check confidence threshold
        if signal['confidence'] < self.config['minimum_confidence']:
            logging.info(f"Signal confidence {signal['confidence']} below threshold")
            return True
        
        # Format message
        message = self._format_signal_message(signal)
        
        # Check quiet hours and weekends
        if self._is_quiet_hours() or self._is_weekend():
            self.queue.add({'type': 'signal', 'content': message, 'signal': signal})
            logging.info(f"Queued signal for {signal['symbol']} during quiet hours/weekend")
            return True
        
        # Check rate limit
        if not self._check_rate_limit():
            self.queue.add({'type': 'signal', 'content': message, 'signal': signal})
            logging.warning("Rate limit exceeded, message queued")
            return True
        
        # Send message
        try:
            self.loop.run_until_complete(self._send_message(self.chat_id, message))
            self.rate_limit_count += 1
            self.history.add('signal', signal['symbol'], message, True)
            logging.info(f"Sent signal for {signal['symbol']}")
            return True
            
        except TelegramError as e:
            self.history.add('signal', signal['symbol'], message, False)
            logging.error(f"Failed to send Telegram message: {e}")
            return False
    
    def process_queue(self):
        """
        Processes queued messages outside of quiet hours.
        
        Returns:
            int: Number of messages successfully processed
        """
        if self._is_quiet_hours() or self._is_weekend():
            return 0
        
        processed = 0
        messages = self.queue.get_pending()
        self.queue.clear()
        
        for msg in messages:
            if self._check_rate_limit():
                try:
                    self.loop.run_until_complete(self._send_message(self.chat_id, msg['content']))
                    self.rate_limit_count += 1
                    self.history.add(
                        msg['type'],
                        msg.get('signal', {}).get('symbol'),
                        msg['content'],
                        True
                    )
                    processed += 1
                except TelegramError as e:
                    logging.error(f"Failed to send queued message: {e}")
                    self.history.add(
                        msg['type'],
                        msg.get('signal', {}).get('symbol'),
                        msg['content'],
                        False
                    )
            else:
                # Re-queue remaining messages
                self.queue.add(msg)
                break
        
        return processed

    def send_error(self, error_type, error_message):
        """Sends error notification using error template."""
        if not self.bot:
            return False
            
        message = TELEGRAM_TEMPLATES['error'].format(
            error_type=self._escape_markdown(error_type),
            error_message=self._escape_markdown(error_message),
            timestamp=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        )
        
        try:
            self.loop.run_until_complete(self._send_message(self.chat_id, message))
            self.history.add('error', None, message, True)
            return True
        except TelegramError as e:
            logging.error(f"Failed to send error message: {e}")
            self.history.add('error', None, message, False)
            return False
    
    def send_daily_summary(self):
        """Sends daily trading summary using summary template."""
        if not self.bot:
            return False
            
        # Get last 24 hours of notifications
        notifications = self.history.get_recent(24)
        
        # Calculate statistics
        signals = [n for n in notifications if n[2] == 'signal']
        buy_signals = len([s for s in signals if 'BUY' in s[3]])
        sell_signals = len([s for s in signals if 'SELL' in s[3]])
        neutral_signals = len([s for s in signals if 'NEUTRAL' in s[3]])
        
        success_rate = (len([n for n in notifications if n[5]]) / len(notifications)) * 100 if notifications else 0
        
        message = TELEGRAM_TEMPLATES['summary'].format(
            date=datetime.datetime.now().strftime('%Y-%m-%d'),
            signal_count=len(signals),
            buy_count=buy_signals,
            sell_count=sell_signals,
            neutral_count=neutral_signals,
            avg_confidence=0.0,  # TODO: Calculate from signals
            success_rate=success_rate
        )
        
        try:
            self.loop.run_until_complete(self._send_message(self.chat_id, message))
            self.history.add('summary', None, message, True)
            return True
        except TelegramError as e:
            logging.error(f"Failed to send daily summary: {e}")
            self.history.add('summary', None, message, False)
            return False
