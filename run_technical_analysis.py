import schedule
import time
import logging
import sys
import argparse
from technical_analyzer import TechnicalAnalyzer

# Logger konfigurieren
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename='technical_scheduler.log'
)
logger = logging.getLogger('TechnicalScheduler')

# Add console handler for terminal output
console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger.addHandler(console_handler)

# Symbole für die Analyse definieren
STOCK_SYMBOLS = ['AAPL', 'MSFT', 'AMZN', 'GOOGL', 'META', 'TSLA', 'NVDA']
INDEX_SYMBOLS = ['^GSPC', '^DJI', '^IXIC', '^GDAXI']  # S&P 500, Dow Jones, NASDAQ, DAX

# Analyzer initialisieren
analyzer = TechnicalAnalyzer('market_data.db')

def run_analysis():
    """Führt die technische Analyse für alle Symbole durch"""
    logger.info("Starting technical analysis job")
    
    # If symbol is specified in command line arguments, only analyze that symbol
    if len(sys.argv) > 2 and sys.argv[1] == '--symbol':
        symbols = [sys.argv[2]]
    else:
        symbols = STOCK_SYMBOLS + INDEX_SYMBOLS
    
    for symbol in symbols:
        # Analyze and save (saving is now handled inside analyze_symbol method)
        results = analyzer.analyze_symbol(symbol)
        logger.info(f"Analysis results for {symbol}: {results}")
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
