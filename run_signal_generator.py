import schedule
import time
import logging
from signal_generator import SignalGenerator

# Logger konfigurieren
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename='signal_scheduler.log'
)
logger = logging.getLogger('SignalScheduler')

# Symbole für die Analyse definieren
STOCK_SYMBOLS = ['AAPL', 'MSFT', 'AMZN', 'GOOGL', 'META', 'TSLA', 'NVDA']
INDEX_SYMBOLS = ['^GSPC', '^DJI', '^IXIC', '^GDAXI']  # S&P 500, Dow Jones, NASDAQ, DAX

# Generator initialisieren
generator = SignalGenerator('market_data.db', confidence_threshold=0.7)

def generate_signals():
    """Generiert Trading-Signale für alle Symbole"""
    logger.info("Starting signal generation job")
    signals = generator.generate_signals(STOCK_SYMBOLS + INDEX_SYMBOLS)
    generator.save_signals(signals)
    logger.info(f"Signal generation job completed with {len(signals)} signals")

# Zeitplan für die Signalgenerierung definieren
# Während der Handelszeiten alle 30 Minuten ausführen
schedule.every(30).minutes.do(generate_signals)

# Initiale Signalgenerierung starten
generate_signals()

# Hauptschleife für den Scheduler
logger.info("Starting signal generator scheduler")
while True:
    try:
        schedule.run_pending()
        time.sleep(1)
    except Exception as e:
        logger.error(f"Error in scheduler: {str(e)}")
        time.sleep(60)  # Bei Fehler 60 Sekunden warten
