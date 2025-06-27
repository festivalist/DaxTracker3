import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger()

def generate_technical_analysis(symbol="AAPL"):
    """Generate technical analysis for a specific symbol"""
    
    logger.info(f"Starting technical analysis for {symbol}")
    
    # Connect to the database
    conn = sqlite3.connect('market_data.db')
    
    # Get market data
    query = f"""
    SELECT timestamp, open, high, low, close, volume
    FROM market_data
    WHERE symbol = '{symbol}'
    ORDER BY timestamp
    """
    
    df = pd.read_sql_query(query, conn)
    
    if len(df) < 30:
        logger.warning(f"Not enough data for {symbol}. Need at least 30 data points.")
        return False
    
    # Convert timestamp to datetime
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    # Calculate indicators
    # 1. Simple Moving Averages
    df['sma_20'] = df['close'].rolling(window=20).mean()
    df['sma_50'] = df['close'].rolling(window=50).mean()
    
    # 2. RSI
    delta = df['close'].diff()
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)
    
    avg_gain = gain.rolling(window=14).mean()
    avg_loss = loss.rolling(window=14).mean()
    
    rs = avg_gain / avg_loss
    df['rsi'] = 100 - (100 / (1 + rs))
    
    # 3. MACD
    exp1 = df['close'].ewm(span=12, adjust=False).mean()
    exp2 = df['close'].ewm(span=26, adjust=False).mean()
    df['macd_line'] = exp1 - exp2
    df['signal_line'] = df['macd_line'].ewm(span=9, adjust=False).mean()
    
    # Remove rows with NaN values
    df = df.dropna()
    
    # Generate a simple signal
    df['overall_signal'] = 'HOLD'
    
    # Bullish: RSI < 30 and MACD > Signal
    df.loc[(df['rsi'] < 30) & (df['macd_line'] > df['signal_line']), 'overall_signal'] = 'BUY'
    
    # Bearish: RSI > 70 and MACD < Signal
    df.loc[(df['rsi'] > 70) & (df['macd_line'] < df['signal_line']), 'overall_signal'] = 'SELL'
    
    # Save to database
    df_to_save = df[['timestamp', 'sma_20', 'sma_50', 'rsi', 'macd_line', 'signal_line', 'overall_signal']].copy()
    df_to_save['close_price'] = df['close']  # Map to the correct column name in the database
    df_to_save['symbol'] = symbol
    
    try:
        # First delete any existing analysis for this symbol
        cursor = conn.cursor()
        cursor.execute(f"DELETE FROM technical_analysis WHERE symbol = '{symbol}'")
        conn.commit()
        
        # Then insert the new analysis
        df_to_save.to_sql('technical_analysis', conn, if_exists='append', index=False)
        
        logger.info(f"Successfully saved technical analysis for {symbol} ({len(df_to_save)} records)")
        return True
    
    except sqlite3.Error as e:
        logger.error(f"Database error: {e}")
        return False
    finally:
        conn.close()

if __name__ == "__main__":
    # Choose a symbol that has enough market data
    # Based on your data, AAPL seems to be a good candidate
    symbol = "AAPL"
    generate_technical_analysis(symbol)
    
    # Also try with MSFT
    generate_technical_analysis("MSFT")
    
    # Also try with AMZN
    generate_technical_analysis("AMZN")
