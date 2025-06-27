"""
Telegram Configuration Setup Script

This script helps users set up their Telegram bot and save credentials
securely for the trading signal system.

It guides users through:
1. Creating a Telegram bot
2. Getting the chat ID
3. Testing the connection
4. Saving the configuration
"""

import os
import json
import logging
from telegram import Bot
from telegram.error import TelegramError
import sys

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

def get_bot_token():
    print("\n=== Telegram Bot Setup ===")
    print("1. Open Telegram and search for 'BotFather'")
    print("2. Send /newbot to BotFather")
    print("3. Follow instructions to create a new bot")
    print("4. Copy the API token provided by BotFather")
    
    token = input("\nEnter your Telegram Bot API token: ").strip()
    return token

def get_chat_id():
    print("\n=== Chat ID Setup ===")
    print("1. Open Telegram and search for your bot")
    print("2. Start a chat with your bot (/start)")
    print("3. Send any message to the bot")
    return input("\nEnter your Telegram Chat ID: ").strip()

def test_connection(token, chat_id):
    try:
        bot = Bot(token=token)
        message = "🔄 Testing Telegram bot connection..."
        bot.send_message(
            chat_id=chat_id,
            text=message
        )
        return True
    except TelegramError as e:
        logging.error(f"Connection test failed: {e}")
        return False

def save_config(token, chat_id):
    config = {
        'telegram': {
            'token': token,
            'chat_id': chat_id
        },
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
    
    # Save to notification_config.json
    with open('notification_config.json', 'w') as f:
        json.dump(config, f, indent=4)
    
    # Create .env file or update existing
    env_content = f"""TELEGRAM_TOKEN={token}
TELEGRAM_CHAT_ID={chat_id}
"""
    with open('.env', 'a') as f:
        f.write(env_content)

def main():
    setup_logging()
    print("Welcome to the Telegram Notification Setup!")
    
    # Get configuration
    token = get_bot_token()
    chat_id = get_chat_id()
    
    # Test connection
    print("\nTesting connection...")
    if test_connection(token, chat_id):
        print("✅ Connection test successful!")
        
        # Save configuration
        save_config(token, chat_id)
        print("\n✅ Configuration saved successfully!")
        print("\nYou can now start the trading signal server.")
    else:
        print("❌ Connection test failed. Please check your token and chat ID.")
        sys.exit(1)

if __name__ == "__main__":
    main()
