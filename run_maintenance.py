import schedule
import time
import logging
import argparse
from backup_system import BackupSystem
from system_monitor import SystemMonitor

# Logger konfigurieren
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename='maintenance.log'
)
logger = logging.getLogger('Maintenance')

def main():
    # Argumente parsen
    parser = argparse.ArgumentParser(description='Trading Signal System Maintenance')
    parser.add_argument('--email', required=True, help='pCloud Email')
    parser.add_argument('--password', required=True, help='pCloud Password')
    parser.add_argument('--db-path', default='market_data.db', help='Path to SQLite database')
    parser.add_argument('--scripts-dir', default='.', help='Directory containing Python scripts')
    parser.add_argument('--backup-dir', default='backups', help='Directory for local backups')
    args = parser.parse_args()
    
    # Backup-System und Monitor initialisieren
    backup_system = BackupSystem(args.email, args.password, args.backup_dir, args.db_path)
    system_monitor = SystemMonitor(args.db_path, args.scripts_dir)
    
    # Funktionen für Schedule definieren
    def run_backup():
        logger.info("Running scheduled backup")
        backup_system.perform_backup()
    
    def run_monitoring():
        logger.info("Running scheduled monitoring")
        system_monitor.run_monitoring()
    
    # Zeitplan definieren
    # Backup wöchentlich am Sonntag um 03:00 Uhr
    schedule.every().sunday.at("03:00").do(run_backup)
    # Monitoring alle 15 Minuten
    schedule.every(15).minutes.do(run_monitoring)
    
    # Initiales Monitoring durchführen
    run_monitoring()
    
    # Hauptschleife
    logger.info("Starting maintenance scheduler")
    while True:
        try:
            schedule.run_pending()
            time.sleep(60)
        except Exception as e:
            logger.error(f"Error in maintenance scheduler: {str(e)}")
            time.sleep(300)  # Bei Fehler 5 Minuten warten

if __name__ == "__main__":
    main()
