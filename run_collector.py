import schedule
import time
import logging
from data_collector import DataCollector

# Logger konfigurieren
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename='collector_scheduler.log'
)
logger = logging.getLogger('CollectorScheduler')

# Symbole für die Überwachung definieren (Beispiele)
STOCK_SYMBOLS = ['AAPL', 'MSFT', 'AMZN', 'GOOGL', 'META', 'TSLA', 'NVDA']
INDEX_SYMBOLS = ['^GSPC', '^DJI', '^IXIC', '^GDAXI'] # S&P 500, Dow Jones, NASDAQ, DAX

# DataCollector initialisieren
collector = DataCollector('market_data.db')

def collect_market_data():
    """Sammelt Marktdaten für alle definierten Symbole"""
    logger.info("Starting market data collection job")
    for symbol in STOCK_SYMBOLS + INDEX_SYMBOLS:
        collector.fetch_market_data(symbol, period="1d", interval="1m")
        time.sleep(1)  # Pause, um API-Limits zu respektieren
    logger.info("Market data collection job completed")

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
