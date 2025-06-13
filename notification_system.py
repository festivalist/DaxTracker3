import logging
import json
import os
import datetime
from telegram import Bot, ParseMode
from telegram.error import TelegramError

# Logger konfigurieren
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename='notification.log'
)
logger = logging.getLogger('NotificationSystem')

class TelegramNotifier:
    def __init__(self, token, chat_id, config_file='notification_config.json'):
        """
        Initialisiert den Telegram-Notifier
        
        Args:
            token: Der Bot-Token
            chat_id: Die Chat-ID für Benachrichtigungen
            config_file: Pfad zur Konfigurationsdatei
        """
        self.token = token
        self.chat_id = chat_id
        self.config_file = config_file
        self.bot = Bot(token=token)
        self.config = self._load_config()
        logger.info("TelegramNotifier initialized")
    
    def _load_config(self):
        """Lädt die Konfiguration oder erstellt eine Standardkonfiguration"""
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
            'last_notification': None
        }
        
        if os.path.exists(self.config_file):
            try:
                with open(self.config_file, 'r') as f:
                    config = json.load(f)
                logger.info("Config loaded from file")
                return config
            except Exception as e:
                logger.error(f"Error loading config: {str(e)}")
        
        # Standardkonfiguration speichern
        with open(self.config_file, 'w') as f:
            json.dump(default_config, f, indent=4)
        logger.info("Default config created")
        return default_config
    
    def _save_config(self):
        """Speichert die Konfiguration"""
        try:
            with open(self.config_file, 'w') as f:
                json.dump(self.config, f, indent=4)
            logger.info("Config saved")
        except Exception as e:
            logger.error(f"Error saving config: {str(e)}")
    
    def _is_in_quiet_hours(self):
        """Prüft, ob die aktuelle Zeit in den Ruhezeiten liegt"""
        if not self.config['quiet_hours']['enabled']:
            return False
        
        now = datetime.datetime.now().time()
        start = datetime.datetime.strptime(self.config['quiet_hours']['start'], '%H:%M').time()
        end = datetime.datetime.strptime(self.config['quiet_hours']['end'], '%H:%M').time()
        
        # Wenn start > end, dann geht die Ruhezeit über Mitternacht
        if start > end:
            return now >= start or now <= end
        else:
            return start <= now <= end
    
    def _is_weekend(self):
        """Prüft, ob heute Wochenende ist"""
        if not self.config['weekends']['enabled']:
            return False
        
        weekday = datetime.datetime.now().weekday()
        return weekday >= 5  # 5 = Samstag, 6 = Sonntag
    
    def _format_signal_message(self, signal):
        """
        Formatiert ein Signal als Telegram-Nachricht
        
        Args:
            signal: Das Signal
            
        Returns:
            Formatierte Nachricht
        """
        # Emoji basierend auf Signal-Typ
        if signal['signal_type'] == 'BUY':
            emoji = '🟢'
        elif signal['signal_type'] == 'SELL':
            emoji = '🔴'
        else:
            emoji = '⚪️'
        
        # Konfidenz in Prozent
        confidence_pct = int(signal['confidence'] * 100)
        
        # Nachricht formatieren
        message = f"{emoji} *{signal['symbol']}* - {signal['signal_type']} Signal\n\n"
        message += f"*Kurs:* {signal['close_price']:.2f} $\n"
        message += f"*Konfidenz:* {confidence_pct}%\n"
        message += f"*Zeitpunkt:* {signal['timestamp']}\n\n"
        message += f"*Begründung:*\n{signal['reason']}\n\n"
        message += f"#Signal #{signal['symbol']} #{signal['signal_type'].lower()}"
        
        return message
    
    def send_signal(self, signal):
        """
        Sendet ein Signal als Telegram-Nachricht
        
        Args:
            signal: Das Signal
            
        Returns:
            True, wenn erfolgreich, sonst False
        """
        # Prüfen, ob die Nachricht in den Ruhezeiten oder am Wochenende ist
        if self._is_in_quiet_hours():
            logger.info(f"Signal for {signal['symbol']} not sent due to quiet hours")
            return False
        
        is_weekend = self._is_weekend()
        collect_for_monday = self.config['weekends']['collect_for_monday']
        
        if is_weekend and not collect_for_monday:
            logger.info(f"Signal for {signal['symbol']} not sent due to weekend")
            return False
        
        # Nachricht formatieren und senden
        message = self._format_signal_message(signal)
        try:
            self.bot.send_message(
                chat_id=self.chat_id,
                text=message,
                parse_mode=ParseMode.MARKDOWN
            )
            
            # Letzte Benachrichtigung aktualisieren
            self.config['last_notification'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self._save_config()
            
            logger.info(f"Signal for {signal['symbol']} sent successfully")
            return True
        except TelegramError as e:
            logger.error(f"Error sending Telegram message: {str(e)}")
            return False
    
    def send_daily_summary(self, signals):
        """
        Sendet eine tägliche Zusammenfassung der Signale
        
        Args:
            signals: Liste von Signalen
            
        Returns:
            True, wenn erfolgreich, sonst False
        """
        if not signals:
            return False
        
        # Signale nach Typ gruppieren
        buy_signals = [s for s in signals if s['signal_type'] == 'BUY']
        sell_signals = [s for s in signals if s['signal_type'] == 'SELL']
        neutral_signals = [s for s in signals if s['signal_type'] == 'NEUTRAL']
        
        # Nachricht formatieren
        message = f"*Tägliche Trading-Signal Zusammenfassung*\n\n"
        message += f"📅 *Datum:* {datetime.datetime.now().strftime('%d.%m.%Y')}\n\n"
        
        # Buy-Signale
        if buy_signals:
            message += "🟢 *BUY Signale:*\n"
            for signal in buy_signals:
                message += f"  • {signal['symbol']} (Konfidenz: {int(signal['confidence'] * 100)}%)\n"
            message += "\n"
        
        # Sell-Signale
        if sell_signals:
            message += "🔴 *SELL Signale:*\n"
            for signal in sell_signals:
                message += f"  • {signal['symbol']} (Konfidenz: {int(signal['confidence'] * 100)}%)\n"
            message += "\n"
        
        # Nachricht senden
        try:
            self.bot.send_message(
                chat_id=self.chat_id,
                text=message,
                parse_mode=ParseMode.MARKDOWN
            )
            logger.info("Daily summary sent successfully")
            return True
        except TelegramError as e:
            logger.error(f"Error sending daily summary: {str(e)}")
            return False
