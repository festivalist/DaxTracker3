import torch
from transformers import AutoTokenizer, AutoModelForSequenceClassification
import numpy as np
import logging
import json
import os

# Logger konfigurieren
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename='sentiment.log'
)
logger = logging.getLogger('SentimentAnalyzer')

class FinBERTSentimentAnalyzer:
    def __init__(self, model_path=None, checkpoint_dir='checkpoints'):
        """
        Initialisiert den FinBERT-basierten Sentiment-Analyzer
        
        Args:
            model_path: Pfad zum vortrainierten Modell, wenn None wird 'yiyanghkust/finbert-tone' verwendet
            checkpoint_dir: Verzeichnis für Checkpoints
        """
        self.checkpoint_dir = checkpoint_dir
        self.checkpoint_file = os.path.join(checkpoint_dir, 'sentiment_checkpoint.json')
        self.current_state = {'last_processed_id': 0}
        
        # Checkpoints-Verzeichnis erstellen, falls es nicht existiert
        if not os.path.exists(checkpoint_dir):
            os.makedirs(checkpoint_dir)
        
        # Checkpoint laden, falls vorhanden
        self._load_checkpoint()
        
        # Modell und Tokenizer laden
        logger.info("Loading FinBERT model and tokenizer")
        model_name = model_path if model_path else 'yiyanghkust/finbert-tone'
        try:
            self.tokenizer = AutoTokenizer.from_pretrained(model_name)
            self.model = AutoModelForSequenceClassification.from_pretrained(model_name)
            
            # Wenn GPU verfügbar ist, das Modell auf die GPU verschieben
            self.device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
            self.model.to(self.device)
            logger.info(f"Model loaded and moved to {self.device}")
        except Exception as e:
            logger.error(f"Error loading model: {str(e)}")
            raise
        
        # Labels definieren
        self.labels = ['negative', 'neutral', 'positive']
    
    def _load_checkpoint(self):
        """Lädt den letzten Checkpoint, falls vorhanden"""
        if os.path.exists(self.checkpoint_file):
            try:
                with open(self.checkpoint_file, 'r') as f:
                    self.current_state = json.load(f)
                logger.info(f"Loaded checkpoint: {self.current_state}")
            except Exception as e:
                logger.error(f"Error loading checkpoint: {str(e)}")
        else:
            logger.info("No checkpoint found, starting fresh")
    
    def _save_checkpoint(self):
        """Speichert den aktuellen Zustand als Checkpoint"""
        try:
            with open(self.checkpoint_file, 'w') as f:
                json.dump(self.current_state, f)
            logger.info(f"Saved checkpoint: {self.current_state}")
        except Exception as e:
            logger.error(f"Error saving checkpoint: {str(e)}")
    
    def analyze_text(self, text, max_length=512):
        """
        Analysiert einen Text mit FinBERT und gibt die Sentiment-Scores zurück
        
        Args:
            text: Der zu analysierende Text
            max_length: Maximale Tokenanzahl (512 für BERT)
            
        Returns:
            Ein Dictionary mit den Sentiment-Scores und dem dominierenden Sentiment
        """
        try:
            # Text tokenisieren
            inputs = self.tokenizer(text, return_tensors="pt", max_length=max_length, 
                                   truncation=True, padding=True)
            inputs = {key: val.to(self.device) for key, val in inputs.items()}
            
            # Modell-Ausgabe berechnen
            with torch.no_grad():
                outputs = self.model(**inputs)
            
            # Softmax anwenden, um Wahrscheinlichkeiten zu erhalten
            scores = torch.nn.functional.softmax(outputs.logits, dim=1).cpu().numpy()[0]
            
            # Ergebnisse zusammenstellen
            result = {
                'scores': {self.labels[i]: float(scores[i]) for i in range(len(self.labels))},
                'dominant_sentiment': self.labels[np.argmax(scores)],
                'confidence': float(np.max(scores))
            }
            
            return result
        except Exception as e:
            logger.error(f"Error analyzing text: {str(e)}")
            return None
    
    def analyze_long_text(self, text, chunk_size=512):
        """
        Analysiert einen langen Text, indem er in Chunks aufgeteilt wird
        
        Args:
            text: Der lange Text
            chunk_size: Größe der Textchunks
            
        Returns:
            Ein gemitteltes Sentiment-Ergebnis
        """
        try:
            # Text in Chunks aufteilen
            tokenized = self.tokenizer.encode(text)
            chunks = [tokenized[i:i + chunk_size] for i in range(0, len(tokenized), chunk_size)]
            
            # Für jeden Chunk das Sentiment analysieren
            chunk_results = []
            for chunk in chunks:
                chunk_text = self.tokenizer.decode(chunk)
                result = self.analyze_text(chunk_text)
                if result:
                    chunk_results.append(result)
            
            if not chunk_results:
                return None
            
            # Mittelwerte berechnen
            avg_scores = {label: 0.0 for label in self.labels}
            for result in chunk_results:
                for label, score in result['scores'].items():
                    avg_scores[label] += score / len(chunk_results)
            
            # Dominantes Sentiment bestimmen
            dominant_label = max(avg_scores, key=avg_scores.get)
            
            return {
                'scores': avg_scores,
                'dominant_sentiment': dominant_label,
                'confidence': avg_scores[dominant_label]
            }
        except Exception as e:
            logger.error(f"Error analyzing long text: {str(e)}")
            return None
    
    def set_interruptible(self, pause_event):
        """
        Setzt das Pause-Event für unterbrechbare Verarbeitung
        
        Args:
            pause_event: Ein threading.Event-Objekt zur Steuerung der Pausierung
        """
        self.pause_event = pause_event
        logger.info("Interruptible processing enabled")
    
    def process_news_batch(self, news_items, batch_size=10):
        """
        Verarbeitet einen Batch von Nachrichtenartikeln und speichert den Fortschritt
        
        Args:
            news_items: Liste von Nachrichtenartikeln (dict mit 'id', 'title', 'summary')
            batch_size: Anzahl der Artikel pro Batch
            
        Returns:
            Liste von Ergebnissen mit Sentiment-Analyse
        """
        results = []
        
        for i in range(0, len(news_items), batch_size):
            # Prüfen, ob Pause angefordert wurde
            if hasattr(self, 'pause_event') and self.pause_event.is_set():
                logger.info("Processing paused, saving checkpoint")
                self._save_checkpoint()
                return results
            
            batch = news_items[i:i + batch_size]
            for item in batch:
                # Nur verarbeiten, wenn es eine neue ID ist
                if item['id'] > self.current_state['last_processed_id']:
                    # Kombination aus Titel und Zusammenfassung analysieren
                    full_text = f"{item['title']} {item['summary']}"
                    sentiment = self.analyze_text(full_text)
                    
                    if sentiment:
                        result = {
                            'id': item['id'],
                            'symbol': item['symbol'],
                            'sentiment': sentiment
                        }
                        results.append(result)
                        
                        # Letzte verarbeitete ID aktualisieren
                        self.current_state['last_processed_id'] = item['id']
            
            # Checkpoint nach jedem Batch speichern
            self._save_checkpoint()
        
        return results
