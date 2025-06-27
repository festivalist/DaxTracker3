# sentiment_analyzer.py
import torch
from transformers import AutoTokenizer, AutoModelForSequenceClassification
import numpy as np
import os
import json

class FinBERTSentimentAnalyzer:
    def __init__(self, model_path=None, checkpoint_dir='checkpoints'):
        self.checkpoint_dir = checkpoint_dir
        self.checkpoint_file = os.path.join(checkpoint_dir, 'sentiment_checkpoint.json')
        self.current_state = {'last_processed_id': 0}
        
        # Checkpoints-Verzeichnis erstellen, falls es nicht existiert
        if not os.path.exists(checkpoint_dir):
            os.makedirs(checkpoint_dir)
        
        # Checkpoint laden, falls vorhanden
        self._load_checkpoint()
        
        # Modell und Tokenizer laden
        model_name = model_path if model_path else 'yiyanghkust/finbert-tone'
        try:
            self.tokenizer = AutoTokenizer.from_pretrained(model_name)
            self.model = AutoModelForSequenceClassification.from_pretrained(model_name)
            
            # Wenn GPU verfügbar ist, das Modell auf die GPU verschieben
            self.device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
            self.model.to(self.device)
        except Exception as e:
            raise
        
        # Labels definieren
        self.labels = ['negative', 'neutral', 'positive']
    
    def _load_checkpoint(self):
        if os.path.exists(self.checkpoint_file):
            try:
                with open(self.checkpoint_file, 'r') as f:
                    self.current_state = json.load(f)
            except Exception as e:
                pass
    
    def _save_checkpoint(self):
        try:
            with open(self.checkpoint_file, 'w') as f:
                json.dump(self.current_state, f)
        except Exception as e:
            pass
    
    def analyze_text(self, text, max_length=512):
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
            return None
