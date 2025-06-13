import sqlite3
import logging
import datetime
import json

# Logger konfigurieren
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename='signal_generator.log'
)
logger = logging.getLogger('SignalGenerator')

class SignalGenerator:
    def __init__(self, db_path, confidence_threshold=0.7):
        """
        Initialisiert den Signal-Generator
        
        Args:
            db_path: Pfad zur SQLite-Datenbank
            confidence_threshold: Schwellenwert für die Konfidenz eines Signals
        """
        self.db_path = db_path
        self.confidence_threshold = confidence_threshold
        logger.info(f"SignalGenerator initialized with database at {db_path} and threshold {confidence_threshold}")
    
    def _get_latest_technical_analysis(self, symbol):
        """
        Holt die neueste technische Analyse für ein Symbol
        
        Args:
            symbol: Das Aktiensymbol
            
        Returns:
            Die neueste technische Analyse oder None
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
            SELECT id, symbol, timestamp, close_price, overall_signal, signal_strength
            FROM technical_analysis
            WHERE symbol = ?
            ORDER BY timestamp DESC
            LIMIT 1
            ''', (symbol,))
            
            row = cursor.fetchone()
            conn.close()
            
            if not row:
                return None
            
            return {
                'id': row[0],
                'symbol': row[1],
                'timestamp': row[2],
                'close_price': row[3],
                'overall_signal': row[4],
                'signal_strength': row[5]
            }
        except Exception as e:
            logger.error(f"Error getting technical analysis for {symbol}: {str(e)}")
            return None
    
    def _get_latest_sentiment(self, symbol):
        """
        Holt die neueste Sentiment-Analyse für ein Symbol
        
        Args:
            symbol: Das Aktiensymbol
            
        Returns:
            Die neueste Sentiment-Analyse oder None
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
            SELECT sr.news_id, sr.symbol, sr.negative_score, sr.neutral_score, sr.positive_score,
                   sr.dominant_sentiment, sr.confidence, nd.title, nd.summary
            FROM sentiment_results sr
            JOIN news_data nd ON sr.news_id = nd.rowid
            WHERE sr.symbol = ?
            ORDER BY sr.timestamp DESC
            LIMIT 5
            ''', (symbol,))
            
            rows = cursor.fetchall()
            conn.close()
            
            if not rows:
                return None
            
            # Durchschnittliches Sentiment aus den letzten 5 Nachrichtenartikeln berechnen
            avg_negative = sum(row[2] for row in rows) / len(rows)
            avg_neutral = sum(row[3] for row in rows) / len(rows)
            avg_positive = sum(row[4] for row in rows) / len(rows)
            
            # Dominantes Sentiment bestimmen
            scores = {
                'negative': avg_negative,
                'neutral': avg_neutral,
                'positive': avg_positive
            }
            dominant = max(scores, key=scores.get)
            confidence = scores[dominant]
            
            # Die neueste Nachricht für Referenz speichern
            latest = rows[0]
            
            return {
                'symbol': symbol,
                'avg_negative': avg_negative,
                'avg_neutral': avg_neutral,
                'avg_positive': avg_positive,
                'dominant_sentiment': dominant,
                'confidence': confidence,
                'latest_news_id': latest[0],
                'latest_news_title': latest[7],
                'latest_news_summary': latest[8]
            }
        except Exception as e:
            logger.error(f"Error getting sentiment for {symbol}: {str(e)}")
            return None
    
    def _map_sentiment_to_signal(self, sentiment):
        """
        Wandelt ein Sentiment in ein Trading-Signal um
        
        Args:
            sentiment: Das dominante Sentiment
            
        Returns:
            Das entsprechende Trading-Signal
        """
        if sentiment == 'positive':
            return 'BUY'
        elif sentiment == 'negative':
            return 'SELL'
        else:
            return 'NEUTRAL'
    
    def generate_signals(self, symbols):
        """
        Generiert Trading-Signale für eine Liste von Symbolen
        
        Args:
            symbols: Liste von Aktiensymbolen
            
        Returns:
            Liste von generierten Signalen
        """
        signals = []
        
        for symbol in symbols:
            try:
                # Technische Analyse und Sentiment holen
                technical = self._get_latest_technical_analysis(symbol)
                sentiment = self._get_latest_sentiment(symbol)
                
                if not technical:
                    logger.warning(f"No technical analysis available for {symbol}")
                    continue
                
                # Basis-Signal aus technischer Analyse
                tech_signal = technical['overall_signal']
                tech_strength = technical['signal_strength']
                
                # Sentiment-basiertes Signal (falls verfügbar)
                if sentiment:
                    sent_signal = self._map_sentiment_to_signal(sentiment['dominant_sentiment'])
                    sent_strength = sentiment['confidence']
                else:
                    sent_signal = 'NEUTRAL'
                    sent_strength = 0.5
                
                # Kombiniertes Signal berechnen
                if tech_signal == sent_signal and tech_signal != 'NEUTRAL':
                    # Starkes Signal, wenn beide übereinstimmen
                    combined_signal = tech_signal
                    combined_strength = (tech_strength + sent_strength) / 2
                elif tech_signal != 'NEUTRAL' and sent_signal != 'NEUTRAL' and tech_signal != sent_signal:
                    # Widersprüchliche Signale, Neutralisieren
                    combined_signal = 'NEUTRAL'
                    combined_strength = max(tech_strength, sent_strength)
                elif tech_signal != 'NEUTRAL':
                    # Technisches Signal stärker gewichten
                    combined_signal = tech_signal
                    combined_strength = tech_strength * 0.7 + sent_strength * 0.3
                elif sent_signal != 'NEUTRAL':
                    # Sentiment-Signal stärker gewichten
                    combined_signal = sent_signal
                    combined_strength = sent_strength * 0.6 + tech_strength * 0.4
                else:
                    combined_signal = 'NEUTRAL'
                    combined_strength = (tech_strength + sent_strength) / 2
                
                # Signal nur bei ausreichender Konfidenz generieren
                if combined_strength >= self.confidence_threshold:
                    # Signal-Metadaten zusammenstellen
                    signal = {
                        'symbol': symbol,
                        'timestamp': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        'signal_type': combined_signal,
                        'confidence': combined_strength,
                        'close_price': technical['close_price'],
                        'technical_signal': tech_signal,
                        'technical_strength': tech_strength,
                        'sentiment_signal': sent_signal,
                        'sentiment_strength': sent_strength,
                        'reason': self._generate_reason(tech_signal, sent_signal, sentiment)
                    }
                    
                    signals.append(signal)
                    logger.info(f"Generated {combined_signal} signal for {symbol} with confidence {combined_strength:.2f}")
            except Exception as e:
                logger.error(f"Error generating signal for {symbol}: {str(e)}")
        
        return signals
    
    def _generate_reason(self, tech_signal, sent_signal, sentiment):
        """
        Generiert eine Begründung für das Signal
        
        Args:
            tech_signal: Das technische Signal
            sent_signal: Das Sentiment-Signal
            sentiment: Die Sentiment-Daten
            
        Returns:
            Eine Begründung als String
        """
        reasons = []
        
        if tech_signal == 'BUY':
            reasons.append("Technische Indikatoren zeigen einen Aufwärtstrend")
        elif tech_signal == 'SELL':
            reasons.append("Technische Indikatoren zeigen einen Abwärtstrend")
        
        if sentiment:
            if sent_signal == 'BUY':
                reasons.append(f"Positive Nachrichten: {sentiment['latest_news_title']}")
            elif sent_signal == 'SELL':
                reasons.append(f"Negative Nachrichten: {sentiment['latest_news_title']}")
        
        return " und ".join(reasons)
    
    def save_signals(self, signals):
        """
        Speichert die generierten Signale in der Datenbank
        
        Args:
            signals: Die generierten Signale
        """
        if not signals:
            return
        
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Tabelle erstellen, falls sie nicht existiert
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS trading_signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT,
                timestamp TEXT,
                signal_type TEXT,
                confidence REAL,
                close_price REAL,
                technical_signal TEXT,
                sentiment_signal TEXT,
                reason TEXT,
                notified INTEGER DEFAULT 0,
                verified INTEGER DEFAULT 0,
                outcome TEXT DEFAULT NULL
            )
            ''')
            
            # Signale speichern
            for signal in signals:
                cursor.execute('''
                INSERT INTO trading_signals
                (symbol, timestamp, signal_type, confidence, close_price, 
                technical_signal, sentiment_signal, reason)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    signal['symbol'],
                    signal['timestamp'],
                    signal['signal_type'],
                    signal['confidence'],
                    signal['close_price'],
                    signal['technical_signal'],
                    signal['sentiment_signal'],
                    signal['reason']
                ))
            
            conn.commit()
            conn.close()
            logger.info(f"Saved {len(signals)} signals to database")
        except Exception as e:
            logger.error(f"Error saving signals: {str(e)}")
    
    def get_unnotified_signals(self):
        """
        Holt unbenachrichtigte Signale aus der Datenbank
        
        Returns:
            Liste von unbenachrichtigten Signalen
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
            SELECT id, symbol, timestamp, signal_type, confidence, close_price, reason
            FROM trading_signals
            WHERE notified = 0
            ORDER BY timestamp DESC
            ''')
            
            rows = cursor.fetchall()
            conn.close()
            
            signals = []
            for row in rows:
                signals.append({
                    'id': row[0],
                    'symbol': row[1],
                    'timestamp': row[2],
                    'signal_type': row[3],
                    'confidence': row[4],
                    'close_price': row[5],
                    'reason': row[6]
                })
            
            logger.info(f"Retrieved {len(signals)} unnotified signals")
            return signals
        except Exception as e:
            logger.error(f"Error getting unnotified signals: {str(e)}")
            return []
    
    def mark_as_notified(self, signal_id):
        """
        Markiert ein Signal als benachrichtigt
        
        Args:
            signal_id: Die ID des Signals
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
            UPDATE trading_signals
            SET notified = 1
            WHERE id = ?
            ''', (signal_id,))
            
            conn.commit()
            conn.close()
            logger.info(f"Marked signal {signal_id} as notified")
            return True
        except Exception as e:
            logger.error(f"Error marking signal as notified: {str(e)}")
            return False
    
    def verify_signal(self, signal_id, outcome):
        """
        Verifiziert ein Signal mit dem tatsächlichen Outcome
        
        Args:
            signal_id: Die ID des Signals
            outcome: Der tatsächliche Outcome (SUCCESS oder FAILURE)
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
            UPDATE trading_signals
            SET verified = 1, outcome = ?
            WHERE id = ?
            ''', (outcome, signal_id))
            
            conn.commit()
            conn.close()
            logger.info(f"Verified signal {signal_id} with outcome {outcome}")
            return True
        except Exception as e:
            logger.error(f"Error verifying signal: {str(e)}")
            return False
