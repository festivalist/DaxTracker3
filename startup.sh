#!/bin/bash

# Verzeichnis des Skripts
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $DIR

# Log-Datei
LOG_FILE="$DIR/startup.log"

# Funktion für Logging
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a $LOG_FILE
}

# Virtuelle Umgebung aktivieren
log "Aktiviere Python-Umgebung"
source trading_env/bin/activate

# Starten der Komponenten
log "Starte Data Collector"
python run_collector.py > logs/collector.log 2>&1 &

# Kurz warten, um sicherzustellen, dass der Collector gestartet ist
sleep 5

log "Starte Technical Analyzer"
python run_technical_analysis.py > logs/technical.log 2>&1 &

sleep 3

log "Starte Signal Generator"
python run_signal_generator.py > logs/signal.log 2>&1 &

sleep 3

log "Starte Notifier"
python run_notifier.py > logs/notifier.log 2>&1 &

sleep 3

log "Starte System-Monitor"
python run_maintenance.py --email "IHRE_PCLOUD_EMAIL" --password "IHR_PCLOUD_PASSWORT" > logs/maintenance.log 2>&1 &

log "Alle Komponenten gestartet"
