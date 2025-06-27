# trading_signal_server.py
from fastapi import FastAPI
from pydantic import BaseModel
from typing import List, Optional
import json
import os

# Initialize FastAPI app
app = FastAPI()

class MarketDataRequest(BaseModel):
    symbol: str
    period: str = "1d"
    interval: str = "1m"

class NewsRequest(BaseModel):
    symbol: str

class TechnicalAnalysisRequest(BaseModel):
    symbol: str

class SignalGenerationRequest(BaseModel):
    symbols: List[str]

class SignalNotificationRequest(BaseModel):
    signal: dict
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
@mcp.Tool(name="market_data_collector", description="Sammelt Marktdaten für ein Symbol von Yahoo Finance", input_schema={
    "type": "object",
    "properties": {
        "symbol": {"type": "string"},
        "period": {"type": "string", "default": "1d"},
        "interval": {"type": "string", "default": "1m"}
    },
    "required": ["symbol"]
})
def collect_market_data(symbol: str, period: str = "1d", interval: str = "1m"):
    result = data_collector.fetch_market_data(symbol, period, interval)
    return {"success": result, "symbol": symbol}

@mcp.Tool(name="news_collector", description="Sammelt Nachrichtendaten für ein Symbol von Yahoo Finance", input_schema={
    "type": "object",
    "properties": {
        "symbol": {"type": "string"}
    },
    "required": ["symbol"]
})
def collect_news(symbol: str):
    result = data_collector.fetch_news(symbol)
    return {"success": result, "symbol": symbol}

@mcp.Tool(name="technical_analysis", description="Führt eine technische Analyse für ein Symbol durch", input_schema={
    "type": "object",
    "properties": {
        "symbol": {"type": "string"}
    },
    "required": ["symbol"]
})
def analyze_symbol(symbol: str):
    result = technical_analyzer.analyze_symbol(symbol)
    return result if result else {"error": f"Keine Daten für {symbol} gefunden"}

@mcp.Tool(name="generate_signals", description="Generiert Trading-Signale für eine Liste von Symbolen", input_schema={
    "type": "object",
    "properties": {
        "symbols": {"type": "array", "items": {"type": "string"}}
    },
    "required": ["symbols"]
})
def generate_trading_signals(symbols: list):
    signals = signal_generator.generate_signals(symbols)
    return {"signals": signals, "count": len(signals)}

@mcp.Tool(name="send_notification", description="Sendet eine Signal-Benachrichtigung über Telegram", input_schema={
    "type": "object",
    "properties": {
        "signal": {"type": "object"}
    },
    "required": ["signal"]
})
def send_signal_notification(signal: dict):
    success = notifier.send_signal(signal)
    return {"success": success, "signal": signal}

# MCP-Server starten
if __name__ == "__main__":
    app = mcp.App()
    app.add_tools([
        collect_market_data,
        collect_news,
        analyze_symbol,
        generate_trading_signals,
        send_signal_notification
    ])
    app.run()
