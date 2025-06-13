import os
import sys
import time
from ml_processor import InterruptibleMLProcessor

def main():
    print("Starting ML Processor")
    
    # Pfade konfigurieren
    # Achtung: Pfad zur gemeinsamen Datenbank anpassen!
    db_path = "\\\\RaspberryPi\\shared\\market_data.db"  # Netzwerkpfad zur Datenbank
    checkpoint_dir = "checkpoints"
    
    # Prozessor initialisieren
    processor = InterruptibleMLProcessor(db_path, checkpoint_dir)
    
    try:
        # Verarbeitung starten
        processor.process(batch_size=10)
    except KeyboardInterrupt:
        print("Keyboard interrupt received")
    finally:
        # Sauberes Herunterfahren sicherstellen
        processor.shutdown()
        print("ML Processor shutdown complete")

if __name__ == "__main__":
    main()
