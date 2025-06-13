import schedule
import time
import logging
import datetime
import sqlite3
from signal_generator import SignalGenerator
from notification_system import TelegramNotifier

# Logger konfigurieren
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename='notification_scheduler.log'
)
logger = logging.getLogger('NotificationScheduler')

# Konfiguration
DB_PATH = 'market_data.db'
TELEGRAM_TOKEN = 'YOUR_TELEGRAM_BOT_TOKEN'  # Ersetzen Sie dies mit Ihrem Token
TELEGRAM_CHAT_ID = 'YOUR_CHAT_ID'           # Ersetzen Sie dies mit Ihrer Chat-ID

# Signal-Generator und Notifier initialisieren
generator = SignalGenerator(DB_PATH)
notifier = TelegramNotifier(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID)

def send_notifications():
    """Sendet Benachrichtigungen für neue Signale"""
    logger.info("Starting notification job")
    
    # Unbenachrichtigte Signale holen
    signals = generator.get_unnotified_signals()
    
    if not signals:
        logger.info("No new signals to notify")
        return
    
    # Signale senden und als benachrichtigt markieren
    for signal in signals:
        if notifier.send_signal(signal):
            generator.mark_as_notified(signal['id'])
    
    logger.info(f"Notification job completed for {len(signals)} signals")

def send_daily_summary():
    """Sendet eine tägliche Zusammenfassung"""
    logger.info("Starting daily summary job")
    
    # Signale des heutigen Tages aus der Datenbank holen
    try:
        today = datetime.datetime.now().strftime('%Y-%m-%d')
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        cursor.execute('''
        SELECT id, symbol, timestamp, signal_type, confidence, close_price, reason
        FROM trading_signals
        WHERE timestamp LIKE ?
        ORDER BY timestamp DESC
        ''', (f'{today}%',))
        
        rows = cursor.fetchall()
        conn.close()
        
        signals = []
        for row in rows:
            signals.append({
                'id': row[0],
                'symbol': row[1],
                'timestamp': row[2],
                'signal_type': row[3],
                'confidence': row[4],
                'close_price': row[5],
                'reason': row[6]
            })
        
        if signals:
            notifier.send_daily_summary(signals)
            logger.info(f"Daily summary sent for {len(signals)} signals")
        else:
            logger.info("No signals for daily summary")
    
    except Exception as e:
        logger.error(f"Error sending daily summary: {str(e)}")

# Zeitplan für Benachrichtigungen definieren
# Alle 5 Minuten während der Handelszeiten prüfen
schedule.every(5).minutes.do(send_notifications)
# Tägliche Zusammenfassung um 18:00 Uhr senden
schedule.every().day.at("18:00").do(send_daily_summary)

# Initiale Benachrichtigung senden
send_notifications()

# Hauptschleife für den Scheduler
logger.info("Starting notification scheduler")
while True:
    try:
        schedule.run_pending()
        time.sleep(1)
    except Exception as e:
        logger.error(f"Error in scheduler: {str(e)}")
        time.sleep(60)  # Bei Fehler 60 Sekunden warten
