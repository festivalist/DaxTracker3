import schedule
import time
import logging
from technical_analyzer import TechnicalAnalyzer

# Logger konfigurieren
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename='technical_scheduler.log'
)
logger = logging.getLogger('TechnicalScheduler')

# Symbole für die Analyse definieren
STOCK_SYMBOLS = ['AAPL', 'MSFT', 'AMZN', 'GOOGL', 'META', 'TSLA', 'NVDA']
INDEX_SYMBOLS = ['^GSPC', '^DJI', '^IXIC', '^GDAXI']  # S&P 500, Dow Jones, NASDAQ, DAX

# Analyzer initialisieren
analyzer = TechnicalAnalyzer('market_data.db')

def run_analysis():
    """Führt die technische Analyse für alle Symbole durch"""
    logger.info("Starting technical analysis job")
    for symbol in STOCK_SYMBOLS + INDEX_SYMBOLS:
        results = analyzer.analyze_symbol(symbol)
        if results:
            analyzer.save_analysis_results(results)
        time.sleep(1)  # Kurze Pause zwischen Analysen
    logger.info("Technical analysis job completed")

# Zeitplan für die Analyse definieren
# Alle 15 Minuten während der Handelszeiten ausführen
schedule.every(15).minutes.do(run_analysis)

# Initiale Analyse starten
run_analysis()

# Hauptschleife für den Scheduler
logger.info("Starting technical analysis scheduler")
while True:
    try:
        schedule.run_pending()
        time.sleep(1)
    except Exception as e:
        logger.error(f"Error in scheduler: {str(e)}")
        time.sleep(60)  # Bei Fehler 60 Sekunden warten
