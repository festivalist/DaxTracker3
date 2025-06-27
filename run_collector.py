import schedule
import time
import logging
from data_collector import DataCollector
from datetime import datetime

# Logger konfigurieren
# Configure file handler
file_handler = logging.FileHandler('collector_scheduler.log')
file_handler.setLevel(logging.INFO)
file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(file_formatter)

# Configure console handler
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler.setFormatter(console_formatter)

# Set up logger
logger = logging.getLogger('CollectorScheduler')
logger.setLevel(logging.INFO)
logger.addHandler(file_handler)
logger.addHandler(console_handler)

# Prevent propagation to root logger to avoid duplicate logs
logger.propagate = False

# Symbole für die Überwachung definieren (Beispiele)
STOCK_SYMBOLS = ['AAPL', 'MSFT', 'AMZN', 'GOOGL', 'META', 'TSLA', 'NVDA']
INDEX_SYMBOLS = ['^GSPC', '^DJI', '^IXIC', '^GDAXI'] # S&P 500, Dow Jones, NASDAQ, DAX

# DataCollector initialisieren
collector = DataCollector('market_data.db')

SOURCES = ['yahoo']  # Extend to ['yahoo', 'alphavantage'] if API key is set
FETCH_PERIOD = '5d'
INTERVAL = '1d'
SLEEP_BETWEEN_SYMBOLS = 2

def collect_market_data():
    """Sammelt Marktdaten für alle definierten Symbole"""
    logger.info("Starting market data collection job (multi-source)")
    for symbol in STOCK_SYMBOLS + INDEX_SYMBOLS:
        for source in SOURCES:
            logger.info(f"Fetching {symbol} from {source} at {datetime.now()}")
            if source == 'yahoo':
                collector.fetch_market_data(symbol, period=FETCH_PERIOD, interval=INTERVAL)
            elif source == 'alphavantage':
                collector.fetch_alpha_vantage(symbol, interval=INTERVAL)
            time.sleep(SLEEP_BETWEEN_SYMBOLS)
    logger.info("Market data collection job completed (multi-source)")

def collect_news_data():
    """Sammelt Nachrichtendaten für alle definierten Aktien-Symbole"""
    logger.info("Starting news collection job")
    for symbol in STOCK_SYMBOLS:  # Nur für einzelne Aktien, nicht für Indizes
        collector.fetch_news(symbol)
        time.sleep(1)  # Pause, um API-Limits zu respektieren
    logger.info("News collection job completed")

# Zeitplan für die Datensammlung definieren
# Marktdaten alle 5 Minuten während der Handelszeiten sammeln
schedule.every(5).minutes.do(collect_market_data)
# Nachrichten stündlich sammeln
schedule.every(60).minutes.do(collect_news_data)

# Initiale Datensammlung starten
collect_market_data()
collect_news_data()

# Hauptschleife für den Scheduler
logger.info("Starting scheduler main loop")
while True:
    try:
        schedule.run_pending()
        time.sleep(1)
    except Exception as e:
        logger.error(f"Error in scheduler main loop: {str(e)}")
        time.sleep(60)  # Bei Fehler 60 Sekunden warten

# Optionally, add a CLI entry point for one-off collection or ML export
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Automated data collection and ML export.")
    parser.add_argument('--ml-export', action='store_true', help='Export data for ML pipeline (prints DataFrame)')
    parser.add_argument('--symbol', help='Symbol for ML export')
    parser.add_argument('--start', help='Start date for ML export')
    parser.add_argument('--end', help='End date for ML export')
    args = parser.parse_args()
    if args.ml_export and args.symbol:
        df = collector.export_for_ml(args.symbol, args.start, args.end)
        print(df.head())
    else:
        collect_market_data()
        collect_news_data()
