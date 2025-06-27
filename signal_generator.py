# signal_generator.py
import sqlite3
import datetime
import pandas as pd
import logging
from market_predictor import MarketPredictor
from sentiment_analyzer import FinBERTSentimentAnalyzer
from data_validator import DataValidator
import json

class SignalGenerator:
    def __init__(self, db_path, confidence_threshold=0.7, max_data_age_hours=24):
        """
        Initializes the SignalGenerator with all required components.
        
        Args:
            db_path (str): Path to the SQLite database file
            confidence_threshold (float, optional): Minimum confidence score (0.0 to 1.0)
                for generating signals. Defaults to 0.7.
            max_data_age_hours (int, optional): Maximum age of data in hours to 
                consider valid for signal generation. Defaults to 24.
        """
        # Set up logging
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        if not self.logger.handlers:
            # Console handler
            console_handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
            console_handler.setFormatter(formatter)
            self.logger.addHandler(console_handler)
            
            # File handler
            file_handler = logging.FileHandler('signal_generator.log')
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)
        
        self.db_path = db_path
        self.confidence_threshold = confidence_threshold
        self.max_data_age_hours = max_data_age_hours
        
        # Initialize components
        self.market_predictor = MarketPredictor(db_path)
        self.sentiment_analyzer = FinBERTSentimentAnalyzer()
        self.data_validator = DataValidator(db_path)
        
        # Define weights for different market conditions
        self.market_weights = {
            'trending': {
                'technical': 0.5,    # Technical analysis more reliable in trends
                'ml': 0.3,          # ML predictions valuable in trends
                'sentiment': 0.2     # News impact moderate in trends
            },
            'ranging': {
                'technical': 0.6,    # Technical analysis most important in ranges
                'ml': 0.2,          # ML less reliable in ranges
                'sentiment': 0.2     # News can break the range
            },
            'volatile': {
                'technical': 0.3,    # Technical less reliable in volatility
                'ml': 0.4,          # ML can help detect patterns
                'sentiment': 0.3     # News more important in volatile markets
            }
        }
        
        # Initialize with default trending weights
        self.weights = self.market_weights['trending']
        
        # Technical indicator weights for different conditions
        self.indicator_weights = {
            'trending': {
                'adx': 0.2,          # ADX important for trend strength
                'macd': 0.15,        # MACD for trend confirmation
                'ema_ribbon': 0.15,  # EMA ribbon for trend direction
                'vpt': 0.15,         # Volume Price Trend
                'rsi': 0.1,          # RSI less important in trends
                'stochastic': 0.1,   # Stochastic for overbought/oversold
                'sma': 0.15          # Moving averages for trend
            },
            'ranging': {
                'rsi': 0.2,          # RSI more important in ranges
                'stochastic': 0.2,   # Stochastic more important in ranges
                'macd': 0.15,        # MACD for range breakouts
                'adx': 0.1,          # ADX to confirm ranging
                'vpt': 0.15,         # Volume confirmation
                'ema_ribbon': 0.1,   # Less important in ranges
                'sma': 0.1           # Less important in ranges
            },
            'volatile': {
                'vpt': 0.2,          # Volume Price Trend crucial
                'adx': 0.15,         # ADX for volatility confirmation
                'macd': 0.15,        # MACD for direction
                'rsi': 0.15,         # Quick reversals
                'stochastic': 0.15,  # Quick reversals
                'ema_ribbon': 0.1,   # Less reliable in volatility
                'sma': 0.1           # Less reliable in volatility
            }
        }
        
        # Initialize database tables if they don't exist
        self._init_database()
        
        # Initialize caching
        self._initialize_cache()
        
    def _init_database(self):
        """Initialize required database tables if they don't exist."""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Create technical_analysis table if it doesn't exist
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS technical_analysis (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    timestamp DATETIME NOT NULL,
                    close_price REAL,
                    rsi_14 REAL,
                    macd_line REAL,
                    signal_line REAL,
                    adx REAL,
                    pdi REAL,
                    ndi REAL,
                    atr REAL,
                    stoch_k REAL,
                    stoch_d REAL,
                    vpt REAL,
                    vpt_sma REAL,
                    overall_signal TEXT,
                    signal_strength REAL,
                    indicators TEXT,
                    signals TEXT
                )
            """)
            
            # Create signals table for tracking if it doesn't exist
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS signals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    timestamp DATETIME NOT NULL,
                    signal_type TEXT NOT NULL,
                    confidence REAL,
                    price REAL,
                    market_condition TEXT,
                    reason TEXT,
                    success BOOLEAN,
                    profit_loss REAL
                )
            """)
            
            conn.commit()
            conn.close()
            self.logger.info("Database tables initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Error initializing database: {e}")
            raise
    
    def _get_latest_technical_analysis(self, symbol):
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
            return None
    
    def _get_latest_sentiment(self, symbol):
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
            
            # Durchschnittliches Sentiment berechnen
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
            
            return {
                'symbol': symbol,
                'avg_negative': avg_negative,
                'avg_neutral': avg_neutral,
                'avg_positive': avg_positive,
                'dominant_sentiment': dominant,
                'confidence': confidence,
                'latest_news_title': rows[0][7]
            }
        except Exception as e:
            return None
    
    def _map_sentiment_to_signal(self, sentiment):
        if sentiment == 'positive':
            return 'BUY'
        elif sentiment == 'negative':
            return 'SELL'
        else:
            return 'NEUTRAL'
    
    def _detect_market_condition(self, symbol, lookback_periods=20):
        """
        Detects the current market condition (trending, ranging, or volatile).
        
        Args:
            symbol (str): Trading symbol to analyze
            lookback_periods (int): Number of periods to analyze
            
        Returns:
            str: Market condition ('trending', 'ranging', or 'volatile')
        """
        try:
            conn = sqlite3.connect(self.db_path)
            
            # Get recent technical data
            query = """
            SELECT ta.timestamp, ta.close_price, 
                   json_extract(indicators, '$.adx') as adx,
                   json_extract(indicators, '$.atr') as atr,
                   json_extract(indicators, '$.high') as high,
                   json_extract(indicators, '$.low') as low
            FROM technical_analysis ta
            WHERE ta.symbol = ?
            ORDER BY ta.timestamp DESC
            LIMIT ?
            """
            
            df = pd.read_sql_query(query, conn, params=(symbol, lookback_periods))
            conn.close()
            
            if df.empty:
                return 'trending'  # Default if no data
                
            # Convert indicator strings to float
            for col in ['adx', 'atr', 'high', 'low']:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Calculate metrics
            latest_adx = df['adx'].iloc[0]
            avg_price = df['close_price'].mean()
            volatility = (df['atr'] / df['close_price']).mean() * 100
            price_range = ((df['high'].max() - df['low'].min()) / df['low'].min()) * 100;
            
            # Determine market condition
            if latest_adx > 25:  # Strong trend
                return 'trending'
            elif volatility > 2.0 or price_range > 2.5:  # High volatility
                return 'volatile'
            else:
                return 'ranging'
                
        except Exception as e:
            return 'trending'
    
    def _get_technical_signals(self, symbol, indicator_weights=None):
        """
        Get technical analysis signals with weighted indicators based on market condition.
        
        Args:
            symbol (str): Trading symbol to analyze
            indicator_weights (dict, optional): Weights for different indicators
            
        Returns:
            dict: Technical analysis results with signals and confidence
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # First try to get data from technical_analysis table
            cursor.execute("""
                SELECT ta.timestamp, ta.close_price,
                       ta.rsi_14, ta.macd_line,
                       ta.signal_line, ta.adx,
                       ta.pdi, ta.ndi, ta.atr,
                       ta.stoch_k, ta.stoch_d,
                       ta.vpt, ta.vpt_sma
                FROM technical_analysis ta
                WHERE ta.symbol = ?
                ORDER BY ta.timestamp DESC
                LIMIT 1
            """, (symbol,))
            
            row = cursor.fetchone()
            
            # If no data in technical_analysis, try market_data table
            if not row:
                cursor.execute("""
                    SELECT timestamp, close, volume
                    FROM market_data
                    WHERE symbol = ?
                    ORDER BY timestamp DESC
                    LIMIT 60
                """, (symbol,))
                
                data = cursor.fetchall()
                
                if not data:
                    self.logger.warning(f"No market data found for {symbol}")
                    return None
                
                # Calculate basic technical indicators from market data
                df = pd.DataFrame(data, columns=['timestamp', 'close', 'volume'])
                row = self._calculate_basic_indicators(df)
            
            conn.close()
            
            # Use default weights if none provided
            if not indicator_weights:
                indicator_weights = self.indicator_weights['trending']
            
            # Initialize signal components
            signals = {
                'overall': 'NEUTRAL',
                'rsi': 'NEUTRAL',
                'macd': 'NEUTRAL',
                'adx': 'NEUTRAL',
                'stochastic': 'NEUTRAL',
                'vpt': 'NEUTRAL'
            }
            
            signal_scores = {
                'BUY': 0.0,
                'SELL': 0.0,
                'NEUTRAL': 0.0
            }
            
            # Process RSI
            rsi = float(row[2]) if row[2] is not None else 50
            if rsi < 30:
                signals['rsi'] = 'BUY'
                signal_scores['BUY'] += indicator_weights['rsi']
            elif rsi > 70:
                signals['rsi'] = 'SELL'
                signal_scores['SELL'] += indicator_weights['rsi']
            else:
                signal_scores['NEUTRAL'] += indicator_weights['rsi']
            
            # Process MACD
            macd = float(row[3]) if row[3] is not None else 0
            signal_line = float(row[4]) if row[4] is not None else 0
            if macd > signal_line:
                signals['macd'] = 'BUY'
                signal_scores['BUY'] += indicator_weights['macd']
            elif macd < signal_line:
                signals['macd'] = 'SELL'
                signal_scores['SELL'] += indicator_weights['macd']
            else:
                signal_scores['NEUTRAL'] += indicator_weights['macd']
            
            # Process ADX
            adx = float(row[5]) if row[5] is not None else 0
            pdi = float(row[6]) if row[6] is not None else 0
            ndi = float(row[7]) if row[7] is not None else 0
            
            if adx > 25:
                if pdi > ndi:
                    signals['adx'] = 'BUY'
                    signal_scores['BUY'] += indicator_weights['adx']
                else:
                    signals['adx'] = 'SELL'
                    signal_scores['SELL'] += indicator_weights['adx']
            else:
                signal_scores['NEUTRAL'] += indicator_weights['adx']
            
            # Process Stochastic
            stoch_k = float(row[9]) if row[9] is not None else 50
            stoch_d = float(row[10]) if row[10] is not None else 50
            
            if stoch_k < 20 and stoch_d < 20:
                signals['stochastic'] = 'BUY'
                signal_scores['BUY'] += indicator_weights['stochastic']
            elif stoch_k > 80 and stoch_d > 80:
                signals['stochastic'] = 'SELL'
                signal_scores['SELL'] += indicator_weights['stochastic']
            else:
                signal_scores['NEUTRAL'] += indicator_weights['stochastic']
            
            # Process VPT
            vpt = float(row[11]) if row[11] is not None else 0
            vpt_sma = float(row[12]) if row[12] is not None else 0
            
            if vpt > vpt_sma:
                signals['vpt'] = 'BUY'
                signal_scores['BUY'] += indicator_weights['vpt']
            elif vpt < vpt_sma:
                signals['vpt'] = 'SELL'
                signal_scores['SELL'] += indicator_weights['vpt']
            else:
                signal_scores['NEUTRAL'] += indicator_weights['vpt']
            
            # Determine overall signal
            max_score = max(signal_scores.values())
            for signal_type, score in signal_scores.items():
                if score == max_score:
                    signals['overall'] = signal_type
                    break
            
            return {
                'signals': signals,
                'confidence': max_score,
                'indicators': {
                    'rsi': rsi,
                    'macd': macd,
                    'signal_line': signal_line,
                    'adx': adx,
                    'pdi': pdi,
                    'ndi': ndi,
                    'stoch_k': stoch_k,
                    'stoch_d': stoch_d,
                    'vpt': vpt,
                    'vpt_sma': vpt_sma
                }
            }
            
        except Exception as e:
            self.logger.error(f"Error generating technical signals for {symbol}: {e}")
            return None
    
    def _calculate_basic_indicators(self, df):
        """Calculate basic technical indicators from market data."""
        try:
            # Calculate RSI
            delta = df['close'].diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
            rs = gain / loss
            rsi = 100 - (100 / (1 + rs)).iloc[-1]
            
            # Calculate MACD
            exp1 = df['close'].ewm(span=12, adjust=False).mean()
            exp2 = df['close'].ewm(span=26, adjust=False).mean()
            macd = (exp1 - exp2).iloc[-1]
            signal_line = (exp1 - exp2).ewm(span=9, adjust=False).mean().iloc[-1]
            
            # Calculate simple momentum
            momentum = (df['close'] / df['close'].shift(14) - 1) * 100
            
            return [
                df['timestamp'].iloc[-1],  # timestamp
                df['close'].iloc[-1],      # close_price
                rsi,                       # rsi_14
                macd,                      # macd_line
                signal_line,               # signal_line
                None,                      # adx (requires high/low)
                None,                      # pdi
                None,                      # ndi
                None,                      # atr
                None,                      # stoch_k
                None,                      # stoch_d
                momentum,                  # vpt (simplified as momentum)
                0                          # vpt_sma
            ]
            
        except Exception as e:
            self.logger.error(f"Error calculating basic indicators: {e}")
            return None
    
    def validate_data(self, symbol):
        """
        Validate all required data for signal generation.
        
        Args:
            symbol (str): Trading symbol to validate
            
        Returns:
            bool: True if all data is valid, False otherwise
        """
        # Validate market data
        market_validation = self.data_validator.validate_market_data(
            symbol,
            self.max_data_age_hours
        )
        if not market_validation['valid']:
            self.logger.warning(
                f"Market data validation failed for {symbol}: "
                f"{market_validation['issues']}"
            )
            return False
        
        # Validate technical indicators
        tech_validation = self.data_validator.validate_technical_indicators(symbol)
        if not tech_validation['valid']:
            self.logger.warning(
                f"Technical indicator validation failed for {symbol}: "
                f"{tech_validation['issues']}"
            )
            # Don't return False here, we can calculate indicators if needed
        
        # Validate sentiment data
        sentiment_validation = self.data_validator.validate_sentiment_data(
            symbol,
            self.max_data_age_hours
        )
        if not sentiment_validation['valid']:
            self.logger.warning(
                f"Sentiment data validation failed for {symbol}: "
                f"{sentiment_validation['issues']}"
            )
            # Don't return False, we can still generate signals without sentiment
        
        return True
    
    def generate_signals(self, symbols):
        """
        Generate trading signals for the given symbols.
        
        Args:
            symbols (list): List of trading symbols to analyze
            
        Returns:
            list: List of signal dictionaries containing signal type, confidence, and reasoning
        """
        if not symbols:
            self.logger.warning("No symbols provided for signal generation")
            return []
        
        signals = []
        for symbol in symbols:
            conn = None
            self.logger.info(f"Processing symbol: {symbol}")
            try:
                if not symbol:
                    self.logger.warning("Empty symbol encountered, skipping.")
                    continue
                # Validate data before processing
                self.logger.info(f"Validating data for {symbol}...")
                if not self.validate_data(symbol):
                    self.logger.warning(f"Validation failed for {symbol}, skipping.")
                    continue
                self.logger.info(f"Validation passed for {symbol}.")
                # Detect market condition
                market_condition = self._detect_market_condition(symbol)
                self.logger.info(f"Market condition for {symbol}: {market_condition}")
                self.weights = self.market_weights[market_condition]
                current_indicator_weights = self.indicator_weights[market_condition]
                # Get latest market data
                conn = sqlite3.connect(self.db_path)
                df = pd.read_sql_query("""
                    SELECT timestamp, close, volume
                    FROM market_data
                    WHERE symbol = ?
                    ORDER BY timestamp DESC
                    LIMIT 60
                """, conn, params=(symbol,))
                if df.empty:
                    self.logger.warning(f"No market data found for {symbol}, skipping.")
                    continue
                self.logger.info(f"Market data loaded for {symbol}, {len(df)} rows.")
                # Get technical signals
                tech_signals = self._get_technical_signals(symbol, current_indicator_weights)
                if not tech_signals:
                    self.logger.warning(f"No technical signals for {symbol}, trying to calculate basic indicators.")
                    tech_signals = self._calculate_technical_signals(df)
                if not tech_signals:
                    self.logger.warning(f"Could not generate technical signals for {symbol}, skipping.")
                    continue
                self.logger.info(f"Technical signals for {symbol}: {tech_signals['signals'] if tech_signals else None}")
                # Get ML prediction
                ml_prediction = self.market_predictor.predict(symbol)
                self.logger.info(f"ML prediction for {symbol}: {ml_prediction}")
                # Get sentiment analysis
                sentiment = self._get_latest_sentiment(symbol)
                self.logger.info(f"Sentiment for {symbol}: {sentiment}")
                sentiment_signal = self._map_sentiment_to_signal(
                    sentiment['dominant_sentiment'] if sentiment else 'NEUTRAL'
                )
                # Calculate weighted signal
                signal_weights = {
                    'BUY': 0,
                    'SELL': 0,
                    'NEUTRAL': 0
                }
                # Add technical analysis weight
                signal_weights[tech_signals['signals']['overall']] += (
                    self.weights['technical'] * tech_signals['confidence']
                )
                # Add ML prediction weight
                if ml_prediction and 'signal' in ml_prediction:
                    signal_weights[ml_prediction['signal']] += (
                        self.weights['ml'] * ml_prediction['confidence']
                    )
                # Add sentiment weight
                if sentiment:
                    signal_weights[sentiment_signal] += (
                        self.weights['sentiment'] * sentiment['confidence']
                    )
                self.logger.info(f"Signal weights for {symbol}: {signal_weights}")
                # Determine final signal
                signal_type = max(signal_weights.items(), key=lambda x: x[1])[0]
                confidence = signal_weights[signal_type]
                self.logger.info(f"Final signal for {symbol}: {signal_type} (confidence: {confidence})")
                if confidence >= self.confidence_threshold:
                    # Create detailed signal with market context
                    latest_data = df.iloc[0]
                    signal = {
                        'symbol': symbol,
                        'signal_type': signal_type,
                        'confidence': confidence,
                        'timestamp': latest_data['timestamp'],
                        'close_price': latest_data['close'],
                        'market_condition': market_condition,
                        'reason': self._generate_signal_reason(
                            signal_type,
                            tech_signals,
                            ml_prediction,
                            sentiment,
                            market_condition
                        ),
                        'technical_analysis': tech_signals['indicators']
                    }
                    self.logger.info(f"Tracking signal in database for {symbol}...")
                    # Track signal in database
                    self._track_signal(signal)
                    signals.append(signal)
                else:
                    # Insert a 'no signal' entry into trading_signals
                    latest_data = df.iloc[0]
                    self.logger.info(f"Inserting NO_SIGNAL entry for {symbol} at {latest_data['timestamp']}...")
                    try:
                        cursor = conn.cursor()
                        cursor.execute('''
                            INSERT INTO trading_signals (symbol, timestamp, signal_type, confidence, close_price, technical_signal, sentiment_signal, reason, notified, verified, outcome)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''', (
                            symbol,
                            str(latest_data['timestamp']),
                            'NO_SIGNAL',
                            0.0,
                            float(latest_data['close']),
                            None,
                            None,
                            'No signal generated (confidence below threshold)',
                            0,
                            0,
                            None
                        ))
                        conn.commit()
                        self.logger.info(f"Inserted NO_SIGNAL entry for {symbol} at {latest_data['timestamp']}")
                    except Exception as e:
                        self.logger.error(f"Error inserting 'no signal' entry for {symbol}: {e}")
                        raise
            except Exception as e:
                self.logger.error(f"Error generating signal for {symbol}: {e}")
            finally:
                if conn:
                    conn.close()
        return signals
        
    def _calculate_technical_signals(self, df):
        """Calculate basic technical signals from market data."""
        try:
            # Ensure numeric data
            df['close'] = pd.to_numeric(df['close'], errors='coerce')
            df['volume'] = pd.to_numeric(df['volume'], errors='coerce')
            
            if df['close'].isnull().all():
                self.logger.error("No valid price data found")
                return None
                
            # Calculate RSI
            delta = df['close'].diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
            rs = gain / loss
            rsi = float(100 - (100 / (1 + rs)).iloc[-1])
            
            # Calculate MACD
            exp1 = df['close'].ewm(span=12, adjust=False).mean()
            exp2 = df['close'].ewm(span=26, adjust=False).mean()
            macd = float(exp1.iloc[-1] - exp2.iloc[-1])
            macd_series = exp1 - exp2
            signal_line = float(macd_series.ewm(span=9, adjust=False).mean().iloc[-1])
            
            # Calculate %K and %D
            low_14 = df['close'].rolling(window=14).min()
            high_14 = df['close'].rolling(window=14).max()
            k = ((df['close'] - low_14) / (high_14 - low_14) * 100)
            d = k.rolling(window=3).mean()
            
            # Calculate simple momentum
            momentum = float((df['close'].iloc[-1] / df['close'].iloc[-14] - 1) * 100)
            
            # Initialize signals
            signals = {
                'overall': 'NEUTRAL',
                'rsi': 'NEUTRAL',
                'macd': 'NEUTRAL',
                'stochastic': 'NEUTRAL',
                'momentum': 'NEUTRAL'
            }
            
            # RSI signals
            if rsi < 30:
                signals['rsi'] = 'BUY'
            elif rsi > 70:
                signals['rsi'] = 'SELL'
            
            # MACD signals
            if macd > signal_line:
                signals['macd'] = 'BUY'
            elif macd < signal_line:
                signals['macd'] = 'SELL'
            
            # Stochastic signals
            stoch_k = float(k.iloc[-1])
            stoch_d = float(d.iloc[-1])
            if stoch_k < 20 and stoch_d < 20:
                signals['stochastic'] = 'BUY'
            elif stoch_k > 80 and stoch_d > 80:
                signals['stochastic'] = 'SELL'
            
            # Momentum signals
            if momentum > 0:
                signals['momentum'] = 'BUY'
            elif momentum < 0:
                signals['momentum'] = 'SELL'
            
            # Determine overall signal
            buy_signals = sum(1 for s in signals.values() if s == 'BUY')
            sell_signals = sum(1 for s in signals.values() if s == 'SELL')
            
            if buy_signals > sell_signals:
                signals['overall'] = 'BUY'
            elif sell_signals > buy_signals:
                signals['overall'] = 'SELL'
            
            # Calculate confidence based on signal agreement
            total_signals = len(signals) - 1  # Exclude 'overall'
            max_signals = max(buy_signals, sell_signals)
            confidence = max_signals / total_signals
            
            return {
                'signals': signals,
                'confidence': confidence,
                'indicators': {
                    'rsi': rsi,
                    'macd': macd,
                    'signal_line': signal_line,
                    'stoch_k': stoch_k,
                    'stoch_d': stoch_d,
                    'momentum': momentum
                }
            }
            
        except Exception as e:
            self.logger.error(f"Error calculating technical signals: {e}")
            return None
    
    def validate_data(self, symbol):
        """
        Validate all required data for signal generation.
        
        Args:
            symbol (str): Trading symbol to validate
            
        Returns:
            bool: True if all data is valid, False otherwise
        """
        # Validate market data
        market_validation = self.data_validator.validate_market_data(
            symbol,
            self.max_data_age_hours
        )
        if not market_validation['valid']:
            self.logger.warning(
                f"Market data validation failed for {symbol}: "
                f"{market_validation['issues']}"
            )
            return False
        
        # Validate technical indicators
        tech_validation = self.data_validator.validate_technical_indicators(symbol)
        if not tech_validation['valid']:
            self.logger.warning(
                f"Technical indicator validation failed for {symbol}: "
                f"{tech_validation['issues']}"
            )
            # Don't return False here, we can calculate indicators if needed
        
        # Validate sentiment data
        sentiment_validation = self.data_validator.validate_sentiment_data(
            symbol,
            self.max_data_age_hours
        )
        if not sentiment_validation['valid']:
            self.logger.warning(
                f"Sentiment data validation failed for {symbol}: "
                f"{sentiment_validation['issues']}"
            )
            # Don't return False, we can still generate signals without sentiment
        
        return True
    
    def _initialize_cache(self):
        """
        Initialize caching for frequently accessed data.
        """
        self.cache = {
            'technical_data': {},
            'market_regime': {},
            'sentiment_scores': {},
            'ml_predictions': {}
        }
        self.cache_ttl = {
            'technical_data': datetime.timedelta(minutes=5),
            'market_regime': datetime.timedelta(minutes=15),
            'sentiment_scores': datetime.timedelta(minutes=30),
            'ml_predictions': datetime.timedelta(minutes=15)
        }
        self.cache_timestamps = {k: datetime.datetime.min for k in self.cache.keys()}

    def _get_cached_data(self, cache_key, data_fetcher, *args):
        """
        Get data from cache or fetch and cache if expired.
        
        Args:
            cache_key (str): Key for accessing cache
            data_fetcher (callable): Function to fetch data if cache miss
            *args: Arguments for data_fetcher
            
        Returns:
            The cached or freshly fetched data
        """
        now = datetime.datetime.now()
        if (now - self.cache_timestamps[cache_key] > self.cache_ttl[cache_key] or
            cache_key not in self.cache):
            self.cache[cache_key] = data_fetcher(*args)
            self.cache_timestamps[cache_key] = now
        return self.cache[cache_key]

    def _fetch_market_data(self, symbol, timeframe='1d'):
        """
        Fetch market data from the database.
        
        Args:
            symbol (str): Trading symbol
            timeframe (str): Data timeframe
            
        Returns:
            pd.DataFrame: Market data
        """
        try:
            conn = sqlite3.connect(self.db_path)
            
            # Get recent market data
            query = """
            SELECT md.timestamp, md.open, md.high, md.low, md.close, md.volume
            FROM market_data md
            WHERE md.symbol = ?
            ORDER BY md.timestamp DESC
            LIMIT 60
            """
            
            df = pd.read_sql_query(query, conn, params=(symbol,))
            conn.close()
            
            if df.empty:
                self.logger.warning(f"No market data found for {symbol}")
                return None
                
            # Make sure timestamps are in datetime format
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            
            # Ensure numeric data
            for col in ['open', 'high', 'low', 'close', 'volume']:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Sort data by timestamp
            df = df.sort_values('timestamp')
            
            return df
            
        except Exception as e:
            self.logger.error(f"Error fetching market data for {symbol}: {str(e)}")
            return None