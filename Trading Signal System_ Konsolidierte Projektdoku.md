# Trading Signal System: Konsolidierte Projektdokumentation für MCP Server Integration

## 1. Projektübersicht und Konzept

Das Trading Signal System ist ein automatisiertes Tool zur Generierung von Handelssignalen für Trade Republic-kompatible Derivate, basierend auf Marktdaten und technischer Analyse [^1]. Das System verwendet eine Kombination aus Yahoo Finance API für Datenerfassung, technischer Analyse für Signal-Identifikation und Telegram für Benachrichtigungen [^2]. Die ursprüngliche Implementierung basierte auf einer verteilten Zwei-System-Architektur mit einem Raspberry Pi 5 für 24/7-Datensammlung und einem Windows PC für rechenintensive Machine-Learning-Operationen [^3].

### Kernfunktionalitäten:

- Kontinuierliche Sammlung von Marktdaten über Yahoo Finance API [^1]
- Technische Analyse mit bewährten Indikatoren (SMA, EMA, RSI, MACD, Bollinger Bands) [^4]
- Sentiment-Analyse von Finanznachrichten mit FinBERT (PyTorch) [^5]
- Signalgenerierung mit Konfidenz-Bewertung und Zusammenführung technischer und sentimentaler Signale [^3]
- Telegram-Bot für sofortige Benachrichtigungen über neue Trading-Signale [^6]
- SQLite-Datenbank für persistente Datenspeicherung und Analyse [^7]


## 2. System-Architektur

### 2.1 Aktuelle Zwei-System-Architektur

Die bisherige Implementierung nutzte zwei separate Systeme mit unterschiedlichen Verantwortlichkeiten [^3]:

**Raspberry Pi 5 (24/7-Betrieb):**

- Datensammlung und -speicherung
- Technische Analyse
- Signal-Generierung
- Telegram-Benachrichtigungen

**Windows PC (Tagesbetrieb):**

- PyTorch-basierte Sentiment-Analyse
- FinBERT-Modell-Training und -Inferenz
- ML-Pipeline mit Checkpoint-Management
- Streamlit-Dashboard für Performance-Visualisierung

Diese Aufteilung führte zu erhöhter Komplexität bei der Fehlersuche und Wartung [^8].

### 2.2 Vorgeschlagene MCP-Server-Architektur

Die neue Architektur konsolidiert alle Komponenten in einen einzigen MCP-Server, der in Visual Studio Code integriert wird [^6]:

```json
{
  "inputs": [
    {
      "type": "promptString", 
      "id": "telegram-token",
      "description": "Telegram Bot Token",
      "password": true
    },
    {
      "type": "promptString",
      "id": "telegram-chat-id", 
      "description": "Telegram Chat ID"
    }
  ],
  "servers": {
    "TradingSignals": {
      "type": "stdio",
      "command": "python",
      "args": ["-m", "trading_signal_server"],
      "env": {
        "TELEGRAM_TOKEN": "${input:telegram-token}",
        "TELEGRAM_CHAT_ID": "${input:telegram-chat-id}"
      }
    }
  }
}
```

Diese Architektur nutzt das Model Context Protocol (MCP), ein offener Standard für die Integration von AI-Modellen mit externen Tools und Datenquellen [^9].

## 3. Kernkomponenten und Module

### 3.1 Datenerfassungs-Modul

```python
# data_collector.py
import yfinance as yf
import pandas as pd
import sqlite3
import datetime
import logging

class DataCollector:
    def __init__(self, db_path):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.setup_database()
        
    def setup_database(self):
        # Datenbankstruktur erstellen
        cursor = self.conn.cursor()
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS market_data (
            timestamp TEXT,
            symbol TEXT,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume INTEGER,
            PRIMARY KEY (timestamp, symbol)
        )
        ''')
        
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS news_data (
            timestamp TEXT,
            symbol TEXT,
            title TEXT,
            summary TEXT,
            url TEXT,
            PRIMARY KEY (timestamp, symbol, url)
        )
        ''')
        self.conn.commit()
        
    def fetch_market_data(self, symbol, period="1d", interval="1m"):
        # Wichtiger Fix für die neue yfinance-Version 0.2.63
        try:
            stock = yf.Ticker(symbol)
            data = stock.history(period=period, interval=interval, auto_adjust=True)
            
            # Multi-Index-Struktur behandeln
            if isinstance(data.columns, pd.MultiIndex):
                data.columns = ['_'.join(col).strip() for col in data.columns.values]
            
            # Daten in die Datenbank speichern
            for timestamp, row in data.iterrows():
                cursor = self.conn.cursor()
                cursor.execute('''
                INSERT OR REPLACE INTO market_data 
                (timestamp, symbol, open, high, low, close, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (
                    timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                    symbol,
                    row.get('Open', None),
                    row.get('High', None),
                    row.get('Low', None),
                    row.get('Close', None),
                    row.get('Volume', None)
                ))
            self.conn.commit()
            return True
        except Exception as e:
            return False
    
    def fetch_news(self, symbol):
        # Nachrichtendaten von Yahoo Finance abrufen
        try:
            stock = yf.Ticker(symbol)
            news = stock.news
            
            if news:
                cursor = self.conn.cursor()
                for item in news:
                    timestamp = datetime.datetime.fromtimestamp(item.get('providerPublishTime', 0))
                    cursor.execute('''
                    INSERT OR IGNORE INTO news_data
                    (timestamp, symbol, title, summary, url)
                    VALUES (?, ?, ?, ?, ?)
                    ''', (
                        timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                        symbol,
                        item.get('title', ''),
                        item.get('summary', ''),
                        item.get('link', '')
                    ))
                self.conn.commit()
                return True
            return False
        except Exception as e:
            return False
```

Dieses Modul verwendet yfinance 0.2.63 zur Datensammlung mit einer speziellen Behandlung für die Multi-Index-Struktur der neuesten API-Version [^10].

### 3.2 Technische Analyse

```python
# technical_analyzer.py
import pandas as pd
import numpy as np
import sqlite3
import datetime

class TechnicalAnalyzer:
    def __init__(self, db_path):
        self.db_path = db_path
    
    def _get_market_data(self, symbol, days=30):
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
            return None
        
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df.set_index('timestamp', inplace=True)
        
        return df
    
    def calculate_sma(self, df, window):
        return df['close'].rolling(window=window).mean()
    
    def calculate_ema(self, df, window):
        return df['close'].ewm(span=window, adjust=False).mean()
    
    def calculate_rsi(self, df, window=14):
        delta = df['close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=window).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=window).mean()
        
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        return rsi
    
    def calculate_macd(self, df, fast=12, slow=26, signal=9):
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
        sma = self.calculate_sma(df, window)
        std = df['close'].rolling(window=window).std()
        upper_band = sma + (std * num_std)
        lower_band = sma - (std * num_std)
        
        return {
            'middle_band': sma,
            'upper_band': upper_band,
            'lower_band': lower_band
        }
    
    def analyze_symbol(self, symbol):
        df = self._get_market_data(symbol)
        if df is None or len(df) < 30:
            return None
        
        try:
            # Technische Indikatoren berechnen
            sma_20 = self.calculate_sma(df, 20)
            sma_50 = self.calculate_sma(df, 50)
            ema_12 = self.calculate_ema(df, 12)
            ema_26 = self.calculate_ema(df, 26)
            rsi = self.calculate_rsi(df)
            macd = self.calculate_macd(df)
            bollinger = self.calculate_bollinger_bands(df)
            
            # Signale generieren
            signals = {}
            
            # SMA Crossover Signal
            signals['sma_crossover'] = 'BUY' if sma_20.iloc[-1] > sma_50.iloc[-1] else 'SELL'
            
            # EMA Crossover Signal
            signals['ema_crossover'] = 'BUY' if ema_12.iloc[-1] > ema_26.iloc[-1] else 'SELL'
            
            # RSI Signal
            if rsi.iloc[-1] < 30:
                signals['rsi'] = 'BUY'  # Überverkauft
            elif rsi.iloc[-1] > 70:
                signals['rsi'] = 'SELL'  # Überkauft
            else:
                signals['rsi'] = 'NEUTRAL'
            
            # Gesamtsignal berechnen
            buy_signals = sum(1 for signal in signals.values() if signal == 'BUY')
            sell_signals = sum(1 for signal in signals.values() if signal == 'SELL')
            
            if buy_signals > sell_signals:
                overall_signal = 'BUY'
                signal_strength = buy_signals / len(signals)
            elif sell_signals > buy_signals:
                overall_signal = 'SELL'
                signal_strength = sell_signals / len(signals)
            else:
                overall_signal = 'NEUTRAL'
                signal_strength = 0.5
            
            # Ergebnisse zusammenstellen
            results = {
                'symbol': symbol,
                'latest_close': df['close'].iloc[-1],
                'indicators': {
                    'sma_20': sma_20.iloc[-1],
                    'sma_50': sma_50.iloc[-1],
                    'rsi': rsi.iloc[-1],
                    'macd_line': macd['macd_line'].iloc[-1],
                    'signal_line': macd['signal_line'].iloc[-1],
                },
                'signals': signals,
                'overall_signal': overall_signal,
                'signal_strength': signal_strength,
                'timestamp': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
            return results
        except Exception as e:
            return None
```

Die technische Analyse-Komponente berechnet verschiedene Indikatoren und kombiniert sie zu einem Gesamtsignal mit Konfidenz-Bewertung [^4].

### 3.3 Sentiment-Analyse (PyTorch/FinBERT)

```python
# sentiment_analyzer.py
import torch
from transformers import AutoTokenizer, AutoModelForSequenceClassification
import numpy as np
import os
import json

class FinBERTSentimentAnalyzer:
    def __init__(self, model_path=None, checkpoint_dir='checkpoints'):
        self.checkpoint_dir = checkpoint_dir
        self.checkpoint_file = os.path.join(checkpoint_dir, 'sentiment_checkpoint.json')
        self.current_state = {'last_processed_id': 0}
        
        # Checkpoints-Verzeichnis erstellen, falls es nicht existiert
        if not os.path.exists(checkpoint_dir):
            os.makedirs(checkpoint_dir)
        
        # Checkpoint laden, falls vorhanden
        self._load_checkpoint()
        
        # Modell und Tokenizer laden
        model_name = model_path if model_path else 'yiyanghkust/finbert-tone'
        try:
            self.tokenizer = AutoTokenizer.from_pretrained(model_name)
            self.model = AutoModelForSequenceClassification.from_pretrained(model_name)
            
            # Wenn GPU verfügbar ist, das Modell auf die GPU verschieben
            self.device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
            self.model.to(self.device)
        except Exception as e:
            raise
        
        # Labels definieren
        self.labels = ['negative', 'neutral', 'positive']
    
    def _load_checkpoint(self):
        if os.path.exists(self.checkpoint_file):
            try:
                with open(self.checkpoint_file, 'r') as f:
                    self.current_state = json.load(f)
            except Exception as e:
                pass
    
    def _save_checkpoint(self):
        try:
            with open(self.checkpoint_file, 'w') as f:
                json.dump(self.current_state, f)
        except Exception as e:
            pass
    
    def analyze_text(self, text, max_length=512):
        try:
            # Text tokenisieren
            inputs = self.tokenizer(text, return_tensors="pt", max_length=max_length, 
                                   truncation=True, padding=True)
            inputs = {key: val.to(self.device) for key, val in inputs.items()}
            
            # Modell-Ausgabe berechnen
            with torch.no_grad():
                outputs = self.model(**inputs)
            
            # Softmax anwenden, um Wahrscheinlichkeiten zu erhalten
            scores = torch.nn.functional.softmax(outputs.logits, dim=1).cpu().numpy()[^0]
            
            # Ergebnisse zusammenstellen
            result = {
                'scores': {self.labels[i]: float(scores[i]) for i in range(len(self.labels))},
                'dominant_sentiment': self.labels[np.argmax(scores)],
                'confidence': float(np.max(scores))
            }
            
            return result
        except Exception as e:
            return None
```

Diese Komponente verwendet PyTorch und den FinBERT-Transformer für Sentiment-Analyse von Finanznachrichten [^5][^11].

### 3.4 Signal-Generator

```python
# signal_generator.py
import sqlite3
import datetime

class SignalGenerator:
    def __init__(self, db_path, confidence_threshold=0.7):
        self.db_path = db_path
        self.confidence_threshold = confidence_threshold
    
    def _get_latest_technical_analysis(self, symbol):
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
            SELECT id, symbol, timestamp, close_price, overall_signal, signal_strength
            FROM technical_analysis
            WHERE symbol = ?
            ORDER BY timestamp DESC
            LIMIT 1
            ''', (symbol,))
            
            row = cursor.fetchone()
            conn.close()
            
            if not row:
                return None
            
            return {
                'id': row[^0],
                'symbol': row[^1],
                'timestamp': row[^2],
                'close_price': row[^3],
                'overall_signal': row[^4],
                'signal_strength': row[^5]
            }
        except Exception as e:
            return None
    
    def _get_latest_sentiment(self, symbol):
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
            SELECT sr.news_id, sr.symbol, sr.negative_score, sr.neutral_score, sr.positive_score,
                   sr.dominant_sentiment, sr.confidence, nd.title, nd.summary
            FROM sentiment_results sr
            JOIN news_data nd ON sr.news_id = nd.rowid
            WHERE sr.symbol = ?
            ORDER BY sr.timestamp DESC
            LIMIT 5
            ''', (symbol,))
            
            rows = cursor.fetchall()
            conn.close()
            
            if not rows:
                return None
            
            # Durchschnittliches Sentiment berechnen
            avg_negative = sum(row[^2] for row in rows) / len(rows)
            avg_neutral = sum(row[^3] for row in rows) / len(rows)
            avg_positive = sum(row[^4] for row in rows) / len(rows)
            
            # Dominantes Sentiment bestimmen
            scores = {
                'negative': avg_negative,
                'neutral': avg_neutral,
                'positive': avg_positive
            }
            dominant = max(scores, key=scores.get)
            confidence = scores[dominant]
            
            return {
                'symbol': symbol,
                'avg_negative': avg_negative,
                'avg_neutral': avg_neutral,
                'avg_positive': avg_positive,
                'dominant_sentiment': dominant,
                'confidence': confidence,
                'latest_news_title': rows[^0][^7]
            }
        except Exception as e:
            return None
    
    def _map_sentiment_to_signal(self, sentiment):
        if sentiment == 'positive':
            return 'BUY'
        elif sentiment == 'negative':
            return 'SELL'
        else:
            return 'NEUTRAL'
    
    def generate_signals(self, symbols):
        signals = []
        
        for symbol in symbols:
            try:
                # Technische Analyse und Sentiment holen
                technical = self._get_latest_technical_analysis(symbol)
                sentiment = self._get_latest_sentiment(symbol)
                
                if not technical:
                    continue
                
                # Basis-Signal aus technischer Analyse
                tech_signal = technical['overall_signal']
                tech_strength = technical['signal_strength']
                
                # Sentiment-basiertes Signal (falls verfügbar)
                if sentiment:
                    sent_signal = self._map_sentiment_to_signal(sentiment['dominant_sentiment'])
                    sent_strength = sentiment['confidence']
                else:
                    sent_signal = 'NEUTRAL'
                    sent_strength = 0.5
                
                # Kombiniertes Signal berechnen
                if tech_signal == sent_signal and tech_signal != 'NEUTRAL':
                    # Starkes Signal, wenn beide übereinstimmen
                    combined_signal = tech_signal
                    combined_strength = (tech_strength + sent_strength) / 2
                elif tech_signal != 'NEUTRAL' and sent_signal != 'NEUTRAL' and tech_signal != sent_signal:
                    # Widersprüchliche Signale, Neutralisieren
                    combined_signal = 'NEUTRAL'
                    combined_strength = max(tech_strength, sent_strength)
                elif tech_signal != 'NEUTRAL':
                    # Technisches Signal stärker gewichten
                    combined_signal = tech_signal
                    combined_strength = tech_strength * 0.7 + sent_strength * 0.3
                elif sent_signal != 'NEUTRAL':
                    # Sentiment-Signal stärker gewichten
                    combined_signal = sent_signal
                    combined_strength = sent_strength * 0.6 + tech_strength * 0.4
                else:
                    combined_signal = 'NEUTRAL'
                    combined_strength = (tech_strength + sent_strength) / 2
                
                # Signal nur bei ausreichender Konfidenz generieren
                if combined_strength >= self.confidence_threshold:
                    signal = {
                        'symbol': symbol,
                        'timestamp': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        'signal_type': combined_signal,
                        'confidence': combined_strength,
                        'close_price': technical['close_price'],
                        'technical_signal': tech_signal,
                        'technical_strength': tech_strength,
                        'sentiment_signal': sent_signal,
                        'sentiment_strength': sent_strength,
                        'reason': self._generate_reason(tech_signal, sent_signal, sentiment)
                    }
                    
                    signals.append(signal)
            except Exception as e:
                pass
        
        return signals
```

Der Signal-Generator kombiniert technische und Sentiment-basierte Signale zu einem Gesamtsignal mit Konfidenz-Bewertung [^3][^7].

### 3.5 Telegram-Benachrichtigungssystem

```python
# notification_system.py
import logging
import json
import os
import datetime
from telegram import Bot
from telegram.constants import ParseMode  # WICHTIGER FIX: ParseMode aus telegram.constants importieren
from telegram.error import TelegramError

class TelegramNotifier:
    def __init__(self, token, chat_id, config_file='notification_config.json'):
        self.token = token
        self.chat_id = chat_id
        self.config_file = config_file
        self.bot = Bot(token=token)
        self.config = self._load_config()
    
    def _load_config(self):
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
                return config
            except Exception as e:
                pass
        
        # Standardkonfiguration speichern
        with open(self.config_file, 'w') as f:
            json.dump(default_config, f, indent=4)
        return default_config
    
    def _format_signal_message(self, signal):
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
        # Nachricht formatieren und senden
        message = self._format_signal_message(signal)
        try:
            self.bot.send_message(
                chat_id=self.chat_id,
                text=message,
                parse_mode=ParseMode.MARKDOWN
            )
            return True
        except TelegramError as e:
            return False
```

Das Benachrichtigungssystem verwendet den Telegram Bot API mit einem wichtigen Fix: ParseMode muss aus telegram.constants importiert werden, nicht direkt aus telegram [^6][^12].

## 4. MCP-Server Integration

### 4.1 MCP-Server Setup in VS Code

Um den MCP-Server in VS Code einzurichten, erstellen Sie eine Datei `.vscode/mcp.json` mit folgender Konfiguration [^9]:

```json
{
  "inputs": [
    {
      "type": "promptString", 
      "id": "telegram-token",
      "description": "Telegram Bot Token",
      "password": true
    },
    {
      "type": "promptString",
      "id": "telegram-chat-id", 
      "description": "Telegram Chat ID"
    }
  ],
  "servers": {
    "TradingSignals": {
      "type": "stdio",
      "command": "python",
      "args": ["-m", "trading_signal_server"],
      "env": {
        "TELEGRAM_TOKEN": "${input:telegram-token}",
        "TELEGRAM_CHAT_ID": "${input:telegram-chat-id}"
      }
    }
  }
}
```

Erstellen Sie dann ein Python-Modul `trading_signal_server.py`, das als MCP-Server fungiert und die entsprechenden Tools bereitstellt [^13].

### 4.2 MCP-Server Implementierung

```python
# trading_signal_server.py
import mcp_python as mcp
import json
import os
from data_collector import DataCollector
from technical_analyzer import TechnicalAnalyzer
from signal_generator import SignalGenerator
from notification_system import TelegramNotifier

# Konfiguration aus Umgebungsvariablen
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
DB_PATH = os.environ.get("DB_PATH", "market_data.db")

# Komponenten initialisieren
data_collector = DataCollector(DB_PATH)
technical_analyzer = TechnicalAnalyzer(DB_PATH)
signal_generator = SignalGenerator(DB_PATH)
notifier = TelegramNotifier(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID)

# MCP-Tools definieren
@mcp.tool("market_data_collector")
def collect_market_data(symbol: str, period: str = "1d", interval: str = "1m"):
    """Sammelt Marktdaten für ein Symbol von Yahoo Finance"""
    result = data_collector.fetch_market_data(symbol, period, interval)
    return {"success": result, "symbol": symbol}

@mcp.tool("news_collector")
def collect_news(symbol: str):
    """Sammelt Nachrichtendaten für ein Symbol von Yahoo Finance"""
    result = data_collector.fetch_news(symbol)
    return {"success": result, "symbol": symbol}

@mcp.tool("technical_analysis")
def analyze_symbol(symbol: str):
    """Führt eine technische Analyse für ein Symbol durch"""
    result = technical_analyzer.analyze_symbol(symbol)
    return result if result else {"error": f"Keine Daten für {symbol} gefunden"}

@mcp.tool("generate_signals")
def generate_trading_signals(symbols: list):
    """Generiert Trading-Signale für eine Liste von Symbolen"""
    signals = signal_generator.generate_signals(symbols)
    return {"signals": signals, "count": len(signals)}

@mcp.tool("send_notification")
def send_signal_notification(signal: dict):
    """Sendet eine Signal-Benachrichtigung über Telegram"""
    success = notifier.send_signal(signal)
    return {"success": success, "signal": signal}

# MCP-Server starten
if __name__ == "__main__":
    server = mcp.Server()
    server.register_tools([
        collect_market_data,
        collect_news,
        analyze_symbol,
        generate_trading_signals,
        send_signal_notification
    ])
    server.start()
```

Diese Implementierung verwendet die mcp_python-Bibliothek, um die Trading-System-Komponenten als MCP-Tools zu registrieren [^14].

## 5. Installations- und Einrichtungsanleitung

### 5.1 Abhängigkeiten installieren

```bash
# Virtuelle Umgebung erstellen
python -m venv trading_env
source trading_env/bin/activate  # Unter Windows: trading_env\Scripts\activate

# Abhängigkeiten installieren (mit exakten Versionen)
pip install yfinance==0.2.63
pip install pandas==2.3.0
pip install numpy==1.26.0
pip install scikit-learn==1.7.0
pip install python-telegram-bot==22.1
pip install torch==2.7.1  # Für PyTorch/FinBERT
pip install transformers==4.52.4  # Für FinBERT
pip install mcp-python==0.3.0  # Für MCP-Server
```


### 5.2 Telegram Bot einrichten

1. BotFather auf Telegram öffnen (@BotFather)
2. `/newbot` senden und den Anweisungen folgen
3. Den Bot-Token speichern
4. Eine Nachricht an den Bot senden
5. Chat ID über die API abrufen:

```
https://api.telegram.org/bot{IHR_BOT_TOKEN}/getUpdates
```

6. Die numerische Chat ID aus der JSON-Antwort extrahieren [^6]

### 5.3 MCP-Server in VS Code einrichten

1. VS Code 1.99+ installieren
2. GitHub Copilot Extension installieren
3. MCP-Unterstützung in VS Code aktivieren:
    - Einstellungen öffnen (Ctrl+,)
    - Nach "mcp" suchen
    - "chat.mcp.enabled" aktivieren
4. `.vscode/mcp.json` erstellen (wie oben gezeigt)
5. "MCP: List Servers" aus dem Befehlspalette ausführen
6. Server starten und bei Aufforderung Token und Chat ID eingeben [^9]

## 6. Migration und nächste Schritte

### 6.1 Datenbank-Migration

Um vorhandene Daten vom Zwei-System-Setup zu migrieren:

```python
# migrate_data.py
import sqlite3
import shutil
import os

def migrate_database(source_path, target_path):
    """Migriert eine SQLite-Datenbank von source_path nach target_path"""
    # Sicherungskopie erstellen
    if os.path.exists(target_path):
        backup_path = f"{target_path}.bak"
        shutil.copy2(target_path, backup_path)
    
    # Datenbank kopieren
    shutil.copy2(source_path, target_path)
    
    return True
```


### 6.2 PyTorch/FinBERT-Integration

Die vollständige Integration von PyTorch und FinBERT erfordert zusätzliche Schritte [^5][^11]:

1. Modell herunterladen und speichern:

```python
from transformers import AutoTokenizer, AutoModelForSequenceClassification

# FinBERT-Modell herunterladen
model_name = 'yiyanghkust/finbert-tone'
tokenizer = AutoTokenizer.from_pretrained(model_name)
model = AutoModelForSequenceClassification.from_pretrained(model_name)

# Modell lokal speichern
tokenizer.save_pretrained('./models/finbert-tokenizer')
model.save_pretrained('./models/finbert-model')
```

2. Lokales Modell in MCP-Server laden:

```python
tokenizer = AutoTokenizer.from_pretrained('./models/finbert-tokenizer')
model = AutoModelForSequenceClassification.from_pretrained('./models/finbert-model')
```


### 6.3 Automatisierte Tests

Erstellen Sie Unittest-Skripte zur Validierung der Komponenten:

```python
# test_data_collector.py
import unittest
from data_collector import DataCollector
import os
import sqlite3

class TestDataCollector(unittest.TestCase):
    def setUp(self):
        self.test_db = "test_market_data.db"
        self.collector = DataCollector(self.test_db)
    
    def tearDown(self):
        if os.path.exists(self.test_db):
            os.remove(self.test_db)
    
    def test_fetch_market_data(self):
        result = self.collector.fetch_market_data("AAPL", period="1d", interval="1h")
        self.assertTrue(result)
        
        # Prüfen, ob Daten in der Datenbank sind
        conn = sqlite3.connect(self.test_db)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM market_data WHERE symbol = 'AAPL'")
        count = cursor.fetchone()[^0]
        conn.close()
        
        self.assertGreater(count, 0)

if __name__ == '__main__':
    unittest.main()
```


## 7. Fazit und Ausblick

Die Migration des Trading Signal Systems zu einer MCP-Server-basierten Lösung in VS Code bietet mehrere Vorteile [^15]:

1. **Vereinfachte Architektur**: Alle Komponenten in einem einzigen System
2. **Verbesserte Entwicklungs-Experience**: Integration in VS Code mit Copilot
3. **Einfachere Fehlersuche**: Zentrale Logs und einheitliche Umgebung
4. **Skalierbarkeit**: Einfachere Erweiterung mit zusätzlichen MCP-Tools
5. **Verbesserte Wartbarkeit**: Reduzierte Komplexität durch einheitliche Architektur

Für die zukünftige Entwicklung bieten sich folgende Erweiterungen an:

- Integration weiterer Datenquellen neben Yahoo Finance
- Implementierung fortgeschrittener ML-Modelle
- Erstellung eines integrierten Web-Dashboards
- Automatisierte Performance-Auswertung und Strategieoptimierung

Mit der MCP-Server-Integration ist das Trading Signal System bereit für die nächste Evolutionsstufe und kann effizienter gewartet und erweitert werden [^8][^9].

<div style="text-align: center">⁂</div>

[^1]: https://code.visualstudio.com/docs/copilot/chat/mcp-servers

[^2]: https://apidog.com/blog/vscode-mcp-server/

[^3]: https://www.youtube.com/watch?v=Wp0p7iKH6ho

[^4]: https://charlbotha.com/til/Add-MCP-server-to-VSCode-settings

[^5]: https://en.wikipedia.org/wiki/Model_Context_Protocol

[^6]: https://code.visualstudio.com/api/extension-guides/mcp

[^7]: https://www.philschmid.de/mcp-introduction

[^8]: https://www.youtube.com/watch?v=hAcG8Oey4VE

[^9]: https://tradingstrategy.ai/docs/programming/visual-studio-code.html

[^10]: https://www.youtube.com/watch?v=PIjFDUwgdk4

[^11]: https://www.datacamp.com/blog/how-to-learn-pytorch

[^12]: https://github.com/SanyaB1801/Sentiment-Analysis-of-Financial-News-using-FInBERT

[^13]: https://gaper.io/algorithmic-trading-in-python/

[^14]: https://www.youtube.com/watch?v=pSVvXSsp_Ek

[^15]: https://web.stanford.edu/class/archive/cs/cs224n/cs224n.1234/final-reports/final-report-170049613.pdf

[^16]: https://www.youtube.com/watch?v=WcfKaZL4vpA

[^17]: https://wire.insiderfinance.io/build-a-telegram-bot-with-stock-price-and-candlestick-charts-using-python-0092237f43aa

[^18]: https://www.youtube.com/watch?v=p4L01ZQRPrM

[^19]: https://www.mql5.com/en/book/advanced/sqlite/sqlite_example_ts

[^20]: https://dev.to/shadyshafik/algorithmic-trading-how-to-build-a-trading-bot-with-python-and-sqlite-4h55

[^21]: https://dev.to/shrsv/boost-vs-code-copilot-with-mcp-servers-a-detailed-guide-5fh4

[^22]: https://docs.perplexity.ai/guides/mcp-server

[^23]: https://www.mql5.com/en/forum/474062

[^24]: https://stackoverflow.com/questions/65932303/django-migrations-or-python-or-vscode-problem

[^25]: https://github.com/pmutua/tradingbot

[^26]: https://mayerkrebs.com/create-a-trading-bot-in-python-and-yfinance/

[^27]: https://learn.marketcalls.in/courses/Building-Stock-Market-Based-Telegram-Bots-using-Python-667d47a796f3c041744cf65d-667d47a796f3c041744cf65d

[^28]: https://towardsdatascience.com/how-to-create-a-fully-automated-ai-based-trading-system-with-python-708503c1a907/

