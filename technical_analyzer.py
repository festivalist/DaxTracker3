import pandas as pd
import numpy as np
import sqlite3
import logging
import datetime

# Logger konfigurieren
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename='technical.log'
)
logger = logging.getLogger('TechnicalAnalyzer')

class TechnicalAnalyzer:
    def __init__(self, db_path):
        """
        Initialisiert den technischen Analyzer
        
        Args:
            db_path: Pfad zur SQLite-Datenbank
        """
        self.db_path = db_path
        logger.info(f"TechnicalAnalyzer initialized with database at {db_path}")
    
    def _get_market_data(self, symbol, days=30):
        """
        Holt Marktdaten für ein Symbol aus der Datenbank
        
        Args:
            symbol: Das Aktiensymbol
            days: Anzahl der Tage in die Vergangenheit
            
        Returns:
            Ein Pandas DataFrame mit den Marktdaten
        """
        try:
            conn = sqlite3.connect(self.db_path)
            
            # Zeitpunkt berechnen, ab dem Daten geholt werden sollen
            end_date = datetime.datetime.now()
            start_date = end_date - datetime.timedelta(days=days)
            
            # Daten aus der Datenbank abrufen
            query = f"""
            SELECT timestamp, open, high, low, close, volume
            FROM market_data
            WHERE symbol = '{symbol}'
            AND timestamp >= '{start_date.strftime('%Y-%m-%d')}'
            ORDER BY timestamp
            """
            
            df = pd.read_sql_query(query, conn)
            conn.close()
            
            if df.empty:
                logger.warning(f"No market data found for {symbol}")
                return None
            
            # Timestamp als Index setzen
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df.set_index('timestamp', inplace=True)
            
            logger.info(f"Retrieved {len(df)} market data points for {symbol}")
            return df
        except Exception as e:
            logger.error(f"Error getting market data for {symbol}: {str(e)}")
            return None
    
    def calculate_sma(self, df, window):
        """Berechnet Simple Moving Average"""
        return df['close'].rolling(window=window).mean()
    
    def calculate_ema(self, df, window):
        """Berechnet Exponential Moving Average"""
        return df['close'].ewm(span=window, adjust=False).mean()
    
    def calculate_rsi(self, df, window=14):
        """Berechnet Relative Strength Index"""
        delta = df['close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=window).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=window).mean()
        
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        return rsi
    
    def calculate_macd(self, df, fast=12, slow=26, signal=9):
        """Berechnet MACD (Moving Average Convergence Divergence)"""
        ema_fast = df['close'].ewm(span=fast, adjust=False).mean()
        ema_slow = df['close'].ewm(span=slow, adjust=False).mean()
        macd_line = ema_fast - ema_slow
        signal_line = macd_line.ewm(span=signal, adjust=False).mean()
        histogram = macd_line - signal_line
        
        return {
            'macd_line': macd_line,
            'signal_line': signal_line,
            'histogram': histogram
        }
    
    def calculate_bollinger_bands(self, df, window=20, num_std=2):
        """Berechnet Bollinger Bands"""
        sma = self.calculate_sma(df, window)
        std = df['close'].rolling(window=window).std()
        upper_band = sma + (std * num_std)
        lower_band = sma - (std * num_std)
        
        return {
            'middle_band': sma,
            'upper_band': upper_band,
            'lower_band': lower_band
        }
    
    def analyze_symbol(self, symbol):
        """
        Führt eine technische Analyse für ein Symbol durch
        
        Args:
            symbol: Das Aktiensymbol
            
        Returns:
            Ein Dictionary mit technischen Indikatoren und Signalen
        """
        df = self._get_market_data(symbol)
        if df is None or len(df) < 30:
            logger.warning(f"Insufficient data for technical analysis of {symbol}")
            return None
        
        try:
            # Technische Indikatoren berechnen
            sma_20 = self.calculate_sma(df, 20)
            sma_50 = self.calculate_sma(df, 50)
            ema_12 = self.calculate_ema(df, 12)
            ema_26 = self.calculate_ema(df, 26)
            rsi = self.calculate_rsi(df)
            macd = self.calculate_macd(df)
            bollinger = self.calculate_bollinger_bands(df)
            
            # Die neuesten Werte extrahieren
            latest_close = df['close'].iloc[-1]
            latest_sma_20 = sma_20.iloc[-1]
            latest_sma_50 = sma_50.iloc[-1]
            latest_ema_12 = ema_12.iloc[-1]
            latest_ema_26 = ema_26.iloc[-1]
            latest_rsi = rsi.iloc[-1]
            latest_macd_line = macd['macd_line'].iloc[-1]
            latest_signal_line = macd['signal_line'].iloc[-1]
            latest_upper_band = bollinger['upper_band'].iloc[-1]
            latest_lower_band = bollinger['lower_band'].iloc[-1]
            
            # Signale generieren
            signals = {}
            
            # SMA Crossover Signal
            signals['sma_crossover'] = 'BUY' if latest_sma_20 > latest_sma_50 else 'SELL'
            
            # EMA Crossover Signal
            signals['ema_crossover'] = 'BUY' if latest_ema_12 > latest_ema_26 else 'SELL'
            
            # RSI Signal
            if latest_rsi < 30:
                signals['rsi'] = 'BUY'  # Überverkauft
            elif latest_rsi > 70:
                signals['rsi'] = 'SELL'  # Überkauft
            else:
                signals['rsi'] = 'NEUTRAL'
            
            # MACD Signal
            signals['macd'] = 'BUY' if latest_macd_line > latest_signal_line else 'SELL'
            
            # Bollinger Bands Signal
            if latest_close > latest_upper_band:
                signals['bollinger'] = 'SELL'  # Preis über oberem Band, potenziell überkauft
            elif latest_close < latest_lower_band:
                signals['bollinger'] = 'BUY'   # Preis unter unterem Band, potenziell überverkauft
            else:
                signals['bollinger'] = 'NEUTRAL'
            
            # Gesamtsignal berechnen
            buy_signals = sum(1 for signal in signals.values() if signal == 'BUY')
            sell_signals = sum(1 for signal in signals.values() if signal == 'SELL')
            
            if buy_signals > sell_signals:
                overall_signal = 'BUY'
                signal_strength = buy_signals / len(signals)
            elif sell_signals > buy_signals:
                overall_signal = 'SELL'
                signal_strength = sell_signals / len(signals)
            else:
                overall_signal = 'NEUTRAL'
                signal_strength = 0.5
            
            # Ergebnisse zusammenstellen
            results = {
                'symbol': symbol,
                'latest_close': latest_close,
                'indicators': {
                    'sma_20': latest_sma_20,
                    'sma_50': latest_sma_50,
                    'ema_12': latest_ema_12,
                    'ema_26': latest_ema_26,
                    'rsi': latest_rsi,
                    'macd_line': latest_macd_line,
                    'signal_line': latest_signal_line,
                    'upper_band': latest_upper_band,
                    'lower_band': latest_lower_band
                },
                'signals': signals,
                'overall_signal': overall_signal,
                'signal_strength': signal_strength,
                'timestamp': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
            logger.info(f"Completed technical analysis for {symbol} with overall signal {overall_signal}")
            return results
        except Exception as e:
            logger.error(f"Error analyzing {symbol}: {str(e)}")
            return None
    
    def save_analysis_results(self, results):
        """
        Speichert die Analyseergebnisse in der Datenbank
        
        Args:
            results: Die Analyseergebnisse
        """
        if not results:
            return
        
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Tabelle erstellen, falls sie nicht existiert
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS technical_analysis (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT,
                timestamp TEXT,
                close_price REAL,
                sma_20 REAL,
                sma_50 REAL,
                rsi REAL,
                macd_line REAL,
                signal_line REAL,
                overall_signal TEXT,
                signal_strength REAL
            )
            ''')
            
            # Ergebnisse speichern
            cursor.execute('''
            INSERT INTO technical_analysis
            (symbol, timestamp, close_price, sma_20, sma_50, rsi, macd_line, 
            signal_line, overall_signal, signal_strength)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                results['symbol'],
                results['timestamp'],
                results['latest_close'],
                results['indicators']['sma_20'],
                results['indicators']['sma_50'],
                results['indicators']['rsi'],
                results['indicators']['macd_line'],
                results['indicators']['signal_line'],
                results['overall_signal'],
                results['signal_strength']
            ))
            
            conn.commit()
            conn.close()
            logger.info(f"Saved technical analysis results for {results['symbol']}")
        except Exception as e:
            logger.error(f"Error saving analysis results: {str(e)}")
