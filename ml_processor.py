import threading
import signal
import time
import logging
import sqlite3
import json
import os
from sentiment_analyzer import FinBERTSentimentAnalyzer

# Logger konfigurieren
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename='ml_processor.log'
)
logger = logging.getLogger('MLProcessor')

class InterruptibleMLProcessor:
    def __init__(self, db_path, checkpoint_dir='checkpoints'):
        """
        Initialisiert den unterbrechbaren ML-Prozessor
        
        Args:
            db_path: Pfad zur SQLite-Datenbank
            checkpoint_dir: Verzeichnis für Checkpoints
        """
        self.db_path = db_path
        self.checkpoint_dir = checkpoint_dir
        self.checkpoint_file = os.path.join(checkpoint_dir, 'processor_checkpoint.json')
        self.sentiment_analyzer = FinBERTSentimentAnalyzer(checkpoint_dir=checkpoint_dir)
        
        # Pause-Event für unterbrechbare Verarbeitung
        self.pause_event = threading.Event()
        self.sentiment_analyzer.set_interruptible(self.pause_event)
        
        # Flag für Beendigung
        self.shutdown_flag = threading.Event()
        
        # Checkpoint-Status
        self.current_state = self._load_checkpoint()
        
        # Signal-Handlers einrichten
        signal.signal(signal.SIGINT, self._handle_interrupt)
        signal.signal(signal.SIGTERM, self._handle_terminate)
    
    def _load_checkpoint(self):
        """Lädt den letzten Checkpoint, falls vorhanden"""
        default_state = {
            'last_news_id': 0,
            'last_run': None
        }
        
        if os.path.exists(self.checkpoint_file):
            try:
                with open(self.checkpoint_file, 'r') as f:
                    state = json.load(f)
                logger.info(f"Loaded checkpoint: {state}")
                return state
            except Exception as e:
                logger.error(f"Error loading checkpoint: {str(e)}")
        
        logger.info("No checkpoint found, starting fresh")
        return default_state
    
    def _save_checkpoint(self):
        """Speichert den aktuellen Zustand als Checkpoint"""
        try:
            with open(self.checkpoint_file, 'w') as f:
                json.dump(self.current_state, f)
            logger.info(f"Saved checkpoint: {self.current_state}")
        except Exception as e:
            logger.error(f"Error saving checkpoint: {str(e)}")
    
    def _handle_interrupt(self, signum, frame):
        """Behandelt SIGINT (Strg+C) - pausiert die Verarbeitung"""
        logger.info("Received interrupt signal, pausing processing")
        self.pause_event.set()
    
    def _handle_terminate(self, signum, frame):
        """Behandelt SIGTERM - beendet die Verarbeitung sauber"""
        logger.info("Received terminate signal, shutting down")
        self.pause_event.set()
        self.shutdown_flag.set()
    
    def fetch_unprocessed_news(self):
        """Holt unverarbeitete Nachrichten aus der Datenbank"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Nachrichten abrufen, die nach der letzten verarbeiteten ID kommen
            cursor.execute('''
            SELECT rowid, timestamp, symbol, title, summary, url 
            FROM news_data 
            WHERE rowid > ? 
            ORDER BY rowid
            ''', (self.current_state['last_news_id'],))
            
            results = []
            for row in cursor.fetchall():
                results.append({
                    'id': row[0],
                    'timestamp': row[1],
                    'symbol': row[2],
                    'title': row[3],
                    'summary': row[4],
                    'url': row[5]
                })
            
            conn.close()
            logger.info(f"Fetched {len(results)} unprocessed news items")
            return results
        except Exception as e:
            logger.error(f"Error fetching unprocessed news: {str(e)}")
            return []
    
    def save_sentiment_results(self, results):
        """Speichert die Sentiment-Analyseergebnisse in der Datenbank"""
        if not results:
            return
        
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Tabelle erstellen, falls sie nicht existiert
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS sentiment_results (
                news_id INTEGER PRIMARY KEY,
                symbol TEXT,
                negative_score REAL,
                neutral_score REAL,
                positive_score REAL,
                dominant_sentiment TEXT,
                confidence REAL,
                timestamp TEXT
            )
            ''')
            
            # Ergebnisse speichern
            for result in results:
                sentiment = result['sentiment']
                scores = sentiment['scores']
                
                cursor.execute('''
                INSERT OR REPLACE INTO sentiment_results
                (news_id, symbol, negative_score, neutral_score, positive_score, 
                dominant_sentiment, confidence, timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now'))
                ''', (
                    result['id'],
                    result['symbol'],
                    scores.get('negative', 0.0),
                    scores.get('neutral', 0.0),
                    scores.get('positive', 0.0),
                    sentiment['dominant_sentiment'],
                    sentiment['confidence']
                ))
            
            conn.commit()
            conn.close()
            
            # Letzten Status aktualisieren
            if results:
                self.current_state['last_news_id'] = max(r['id'] for r in results)
                self.current_state['last_run'] = time.strftime('%Y-%m-%d %H:%M:%S')
                self._save_checkpoint()
            
            logger.info(f"Saved {len(results)} sentiment results to database")
        except Exception as e:
            logger.error(f"Error saving sentiment results: {str(e)}")
    
    def process(self, batch_size=10):
        """
        Hauptverarbeitungsschleife
        
        Args:
            batch_size: Anzahl der Nachrichten pro Batch
        """
        logger.info("Starting ML processing")
        
        while not self.shutdown_flag.is_set():
            try:
                # Prüfen, ob Pause aktiv ist
                if self.pause_event.is_set():
                    logger.info("Processing paused, waiting for resume")
                    time.sleep(5)
                    continue
                
                # Unverarbeitete Nachrichten holen
                news_items = self.fetch_unprocessed_news()
                
                if not news_items:
                    logger.info("No new items to process, sleeping for 60 seconds")
                    time.sleep(60)
                    continue
                
                # Sentiment-Analyse durchführen
                results = self.sentiment_analyzer.process_news_batch(news_items, batch_size)
                
                # Ergebnisse speichern
                self.save_sentiment_results(results)
                
                # Kurze Pause zwischen Batches
                time.sleep(1)
            
            except Exception as e:
                logger.error(f"Error in processing loop: {str(e)}")
                time.sleep(60)  # Bei Fehler 60 Sekunden warten
        
        logger.info("Processing loop terminated")
    
    def resume(self):
        """Setzt die Verarbeitung fort"""
        logger.info("Resuming processing")
        self.pause_event.clear()
    
    def pause(self):
        """Pausiert die Verarbeitung"""
        logger.info("Pausing processing")
        self.pause_event.set()
    
    def shutdown(self):
        """Beendet die Verarbeitung sauber"""
        logger.info("Shutting down")
        self.pause_event.set()
        self.shutdown_flag.set()
        self._save_checkpoint()
