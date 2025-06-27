import streamlit as st
import pandas as pd
import numpy as np
import sqlite3
import matplotlib.pyplot as plt
import datetime
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import os
import subprocess
from tab7_content import render_tab7
from tab6_content import render_ml_prediction_tab, verify_predictions

# Seitenkonfiguration
st.set_page_config(
    page_title="Trading Signal System Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Datenbankverbindung
@st.cache_resource
def get_connection():
    # Using check_same_thread=False to allow connections across threads in Streamlit
    # This is safe for read operations and carefully managed write operations
    conn = sqlite3.connect('market_data.db', check_same_thread=False)
    return conn
    
# Hilfsfunktion für sicheren Datenbankabfragen
def safe_query(query, params=None):
    try:
        conn = get_connection()
        # Make sure the connection is valid before using it
        if conn:
            df = pd.read_sql_query(query, conn, params=params)
            return df
        else:
            st.warning("Konnte keine Datenbankverbindung herstellen")
            return pd.DataFrame()
    except Exception as e:
        st.warning(f"Datenbankabfrage fehlgeschlagen: {e}")
        return pd.DataFrame()
        
# Stellt sicher dass alle benötigten Tabellen existieren
def ensure_tables_exist():
    try:
        # Get a connection from the cached connection
        conn = get_connection()
        
        # Create a cursor for database operations
        cursor = conn.cursor()
        
        # Check and create trading_signals table if it doesn't exist
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS trading_signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            timestamp DATETIME NOT NULL,
            signal_type TEXT NOT NULL,
            confidence REAL,
            close_price REAL,
            technical_signal TEXT,
            sentiment_signal TEXT,
            reason TEXT,
            notified INTEGER DEFAULT 0,
            verified INTEGER DEFAULT 0,
            outcome TEXT
        )
        """)
        
        # Check and create technical_analysis table if it doesn't exist
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS technical_analysis (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            timestamp DATETIME NOT NULL,
            close_price REAL,
            sma_20 REAL,
            sma_50 REAL,
            rsi REAL,
            macd_line REAL,
            signal_line REAL,
            overall_signal TEXT
        )
        """)
        
        # Check and create sentiment_results table if it doesn't exist
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS sentiment_results (
            news_id INTEGER PRIMARY KEY,
            symbol TEXT NOT NULL,
            negative_score REAL,
            neutral_score REAL,
            positive_score REAL,
            dominant_sentiment TEXT,
            confidence REAL,
            timestamp DATETIME NOT NULL
        )
        """)
        
        # Check and create news_data table if it doesn't exist
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS news_data (
            rowid INTEGER PRIMARY KEY,
            title TEXT,
            summary TEXT,
            url TEXT,
            timestamp DATETIME NOT NULL
        )
        """)
        
        # Check and create collection_stats table if it doesn't exist
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS collection_stats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp DATETIME NOT NULL,
            batch_size INTEGER,
            symbols_requested INTEGER,
            symbols_received INTEGER,
            elapsed_time REAL,
            interval TEXT,
            period TEXT,
            errors INTEGER
        )
        """)
        
        # Commit changes
        conn.commit()
        
        # Don't close the connection as it's managed by the cache
        # The connection will be reused across the app
    except Exception as e:
        st.error(f"Fehler beim Initialisieren der Datenbanktabellen: {e}")
        # Try to create a fresh connection
        try:            # Create market_data.db if it doesn't exist
            # Don't create a new connection here - use the cached connection manager
            st.info("Neue Datenbankverbindung hergestellt.")
        except:
            st.error("Konnte keine neue Datenbankverbindung herstellen. Bitte überprüfen Sie die Berechtigungen.")
    
# Ensure tables exist on startup
ensure_tables_exist()

# Daten laden
@st.cache_data(ttl=300)  # 5 Minuten Cache
def load_signals_data():
    query = """
    SELECT ts.id, ts.symbol, ts.timestamp, ts.signal_type, ts.confidence, 
           ts.close_price, ts.technical_signal, ts.sentiment_signal, 
           ts.reason, ts.notified, ts.verified, ts.outcome
    FROM trading_signals ts
    ORDER BY ts.timestamp DESC
    """
    df = safe_query(query)
    if not df.empty and 'timestamp' in df.columns:
        # Use errors='coerce' to handle malformed timestamps
        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
    return df

@st.cache_data(ttl=300)
def load_technical_data():
    query = """
    SELECT ta.id, ta.symbol, ta.timestamp, ta.close_price, ta.sma_20, ta.sma_50, 
           ta.rsi, ta.macd_line, ta.signal_line, ta.overall_signal
    FROM technical_analysis ta
    ORDER BY ta.timestamp DESC
    """
    df = safe_query(query)
    if not df.empty and 'timestamp' in df.columns:
        # Use errors='coerce' to handle malformed timestamps
        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
    return df

@st.cache_data(ttl=300)
def load_sentiment_data():
    query = """
    SELECT sr.news_id, sr.symbol, sr.negative_score, sr.neutral_score, sr.positive_score,
           sr.dominant_sentiment, sr.confidence, sr.timestamp, nd.title, nd.summary
    FROM sentiment_results sr
    JOIN news_data nd ON sr.news_id = nd.rowid
    ORDER BY sr.timestamp DESC
    """
    df = safe_query(query)
    if not df.empty and 'timestamp' in df.columns:
        # Use errors='coerce' to handle malformed timestamps
        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
    return df

# Function to load collection statistics data
@st.cache_data(ttl=60)  # Cache for 1 minute
def load_collection_stats():
    query = """
    SELECT id, timestamp, batch_size, symbols_requested, symbols_received, 
           elapsed_time, interval, period, errors
    FROM collection_stats
    ORDER BY timestamp DESC
    LIMIT 500
    """
    df = safe_query(query)
    if not df.empty and 'timestamp' in df.columns:
        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
    return df

# Titelbereich
st.title("Trading Signal System Dashboard")
st.subheader("Echtzeit-Überwachung und Performance-Analyse")

# Daten laden
try:
    signals_df = load_signals_data()
    # Drop rows with NaT timestamps that might have been created by coercing invalid timestamps
    if not signals_df.empty and 'timestamp' in signals_df.columns:
        signals_df = signals_df.dropna(subset=['timestamp'])
except Exception as e:
    st.warning(f"Fehler beim Laden der Trading-Signale: {e}. Die Tabelle 'trading_signals' existiert möglicherweise nicht oder ist leer.")
    signals_df = pd.DataFrame()
try:
    technical_df = load_technical_data()
    # Drop rows with NaT timestamps
    if not technical_df.empty and 'timestamp' in technical_df.columns:
        technical_df = technical_df.dropna(subset=['timestamp'])
except Exception as e:
    st.warning(f"Fehler beim Laden der technischen Analyse: {e}. Die Tabelle 'technical_analysis' existiert möglicherweise nicht oder ist leer.")
    technical_df = pd.DataFrame()
try:
    sentiment_df = load_sentiment_data()
    # Drop rows with NaT timestamps
    if not sentiment_df.empty and 'timestamp' in sentiment_df.columns:
        sentiment_df = sentiment_df.dropna(subset=['timestamp'])
except Exception as e:
    st.warning(f"Fehler beim Laden der Sentiment-Daten: {e}. Die Tabelle 'sentiment_results' oder 'news_data' existiert möglicherweise nicht oder ist leer.")
    sentiment_df = pd.DataFrame()

# Function to get recent signals (added for alert notifications)
@st.cache_data(ttl=15)  # Cache for 15 seconds
def get_recent_signals(minutes=30):
    """Get signals from the last X minutes"""
    time_threshold = datetime.datetime.now() - datetime.timedelta(minutes=minutes)
    formatted_time = time_threshold.strftime('%Y-%m-%d %H:%M:%S')
    
    query = """
    SELECT ts.id, ts.symbol, ts.timestamp, ts.signal_type, ts.confidence, 
           ts.close_price, ts.reason, ts.notified
    FROM trading_signals ts
    WHERE ts.timestamp > ? AND ts.signal_type != 'NO_SIGNAL' 
    ORDER BY ts.timestamp DESC
    LIMIT 5
    """
    df = safe_query(query, params=(formatted_time,))
    if not df.empty and 'timestamp' in df.columns:
        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
    return df

# Add this function before the tabs section in the main part of dashboard.py
def display_signal_alerts():
    """Display any recent signals as alert notifications"""
    # Use the globally defined alert_minutes or default to 30
    minutes = alert_minutes if 'alert_minutes' in globals() else 30
    recent_signals = get_recent_signals(minutes=minutes)
    
    if not recent_signals.empty:
        # Create a visually prominent notification area
        st.markdown("""
        <style>
        .alert-box {
            padding: 10px;
            border-radius: 5px;
            margin-bottom: 15px;
            background-color: #f8d7da;
            border: 1px solid #f5c6cb;
        }
        </style>
        <div class="alert-box">
            <h3>🔔 New Trading Signals Alert!</h3>
        </div>
        """, unsafe_allow_html=True)
        
        # Use an expander that starts expanded
        with st.expander("🔔 Recent Trading Signals", expanded=True):
            st.info(f"The following signals were generated in the last {minutes} minutes")
            
            # Display each signal as a notification
            for _, signal in recent_signals.iterrows():
                # Determine alert color based on signal type
                if signal['signal_type'] == 'BUY':
                    alert_type = "success"
                elif signal['signal_type'] == 'SELL':
                    alert_type = "error"
                else:
                    alert_type = "warning"
                
                # Format timestamp
                timestamp = signal['timestamp'].strftime('%Y-%m-%d %H:%M:%S')
                
                # Format confidence as percentage
                confidence = f"{signal['confidence']*100:.1f}%" if pd.notna(signal['confidence']) else "N/A"
                
                # Create alert message
                message = f"**{signal['symbol']}**: {signal['signal_type']} @ {signal['close_price']} ({confidence} confidence) - {timestamp}"
                
                # Display alert using appropriate styling
                if alert_type == "success":
                    st.success(message)
                elif alert_type == "error":
                    st.error(message)
                else:
                    st.warning(message)

# Add auto-refresh feature
st.sidebar.subheader("Signal Alerts")
auto_refresh = st.sidebar.checkbox("Enable Auto-Refresh", value=True)
refresh_interval = st.sidebar.slider("Refresh Interval (seconds)", min_value=10, max_value=300, value=60, step=10)
alert_minutes = st.sidebar.slider("Show alerts for last X minutes", min_value=5, max_value=120, value=30, step=5)
play_sound = st.sidebar.checkbox("Play Sound on New Signals", value=True)

# Function to check for new signals since last check
def has_new_signals(last_check_time):
    if not last_check_time:
        return False
        
    formatted_time = last_check_time.strftime('%Y-%m-%d %H:%M:%S')
    query = """
    SELECT COUNT(*) AS count
    FROM trading_signals 
    WHERE timestamp > ? AND signal_type != 'NO_SIGNAL'
    """
    result = safe_query(query, params=(formatted_time,))
    if not result.empty:
        return result.iloc[0]['count'] > 0
    return False

# Store the last check time in session state
if 'last_check_time' not in st.session_state:
    st.session_state.last_check_time = datetime.datetime.now()

# Check for new signals
new_signals = has_new_signals(st.session_state.last_check_time)
if new_signals and play_sound:
    # Play sound using HTML5 audio
    st.markdown("""
    <audio autoplay>
        <source src="https://www.soundjay.com/buttons/sounds/button-09.mp3" type="audio/mpeg">
    </audio>
    """, unsafe_allow_html=True)

# Update the last check time
st.session_state.last_check_time = datetime.datetime.now()

if st.sidebar.button("Test Alert"):
    # Insert a test signal to demonstrate alerts
    conn = get_connection()
    now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    cursor = conn.cursor()
    cursor.execute("""
    INSERT INTO trading_signals 
    (symbol, timestamp, signal_type, confidence, close_price, technical_signal, reason, notified)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, ('TEST', now, 'BUY', 0.95, 100.00, 'BUY', 'This is a test alert', 0))
    conn.commit()
    st.sidebar.success("Test alert created! The page will refresh shortly.")
    st.cache_data.clear()  # Clear cache to force reload of data

if auto_refresh:
    st.sidebar.text(f"Dashboard will refresh every {refresh_interval} seconds")
    st.sidebar.text("Last refresh: " + datetime.datetime.now().strftime("%H:%M:%S"))
    st.empty().info(f"Auto-refresh enabled. Checking for new signals every {refresh_interval} seconds.")
    # Using the client-side polling method 
    st.markdown(f"""
        <meta http-equiv="refresh" content="{refresh_interval}">
    """, unsafe_allow_html=True)

# Display recent signal alerts (if any)
display_signal_alerts()

# Tabs erstellen
tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8 = st.tabs([
    "Signal-Übersicht", 
    "Performance-Analyse", 
    "Technische Indikatoren", 
    "Sentiment-Analyse",
    "Backtest-Analyse",
    "Live ML Prediction",
    "Symbol-Abdeckung",
    "Batch Monitoring"
])

with tab1:
    # Signal-Übersicht
    st.header("Aktuelle Trading-Signale")
    # Check if signals_df has required columns
    required_cols = {'symbol', 'signal_type', 'timestamp', 'confidence', 'close_price', 'reason', 'verified', 'outcome'}
    if not required_cols.issubset(signals_df.columns):
        st.warning("Keine oder unvollständige Trading-Signale-Daten vorhanden. Bitte stellen Sie sicher, dass die Tabelle 'trading_signals' Daten enthält.")
    else:
        # Signal Distribution Overview
        st.subheader("Signal-Verteilung aller Symbole")
        
        # Get signal counts by type
        signal_type_counts = signals_df.groupby('signal_type').size().reset_index(name='count')
        
        # Plot with Plotly
        fig = px.pie(
            signal_type_counts, 
            values='count', 
            names='signal_type',
            title='Verteilung der Signaltypen',
            color='signal_type',
            color_discrete_map={
                'BUY': 'green',
                'SELL': 'red',
                'NO_SIGNAL': 'gray'
            }
        )
        st.plotly_chart(fig)
        
        # Latest signals per symbol
        st.subheader("Neueste Signale pro Symbol")
        
        # Get the latest signal for each symbol
        latest_signals = signals_df.sort_values('timestamp', ascending=False).drop_duplicates('symbol')
        
        # Create a table of latest signals
        latest_display = latest_signals[['symbol', 'timestamp', 'signal_type', 'confidence']].copy()
        latest_display['confidence'] = (latest_display['confidence'] * 100).round(1).astype(str) + '%'
        latest_display.rename(columns={
            'symbol': 'Symbol',
            'timestamp': 'Zeitpunkt',
            'signal_type': 'Signal-Typ',
            'confidence': 'Konfidenz'
        }, inplace=True)
        
        st.dataframe(latest_display, use_container_width=True)
        
        # Filter
        col1, col2, col3 = st.columns(3)
        
        with col1:
            symbol_filter = st.multiselect(
                "Symbol auswählen",
                options=sorted(signals_df['symbol'].unique()),
                default=[]
            )
        
        with col2:
            signal_type_filter = st.multiselect(
                "Signal-Typ",
                options=sorted(signals_df['signal_type'].unique()),
                default=[]
            )
        
        with col3:
            date_range = st.date_input(
                "Zeitraum",
                value=(datetime.datetime.now() - datetime.timedelta(days=7), datetime.datetime.now()),
                max_value=datetime.datetime.now()
            )
          # Daten filtern
        filtered_df = signals_df.copy()
        if symbol_filter:
            filtered_df = filtered_df[filtered_df['symbol'].isin(symbol_filter)]
        
        if signal_type_filter:
            filtered_df = filtered_df[filtered_df['signal_type'].isin(signal_type_filter)]
        
        if len(date_range) == 2:
            start_date, end_date = date_range
            try:
                # Safely filter by date range with error handling
                date_mask = (
                    (filtered_df['timestamp'].dt.date >= start_date) & 
                    (filtered_df['timestamp'].dt.date <= end_date)
                )
                filtered_df = filtered_df[date_mask]
            except Exception as e:
                st.warning(f"Fehler bei der Datumsfiltierung: {e}")
                # Continue without date filtering if there's an error
        
        # Signale anzeigen
        if not filtered_df.empty:
            # KPIs
            kpi1, kpi2, kpi3, kpi4 = st.columns(4)
            
            with kpi1:
                st.metric("Anzahl Signale", len(filtered_df))
            
            with kpi2:
                buy_count = len(filtered_df[filtered_df['signal_type'] == 'BUY'])
                sell_count = len(filtered_df[filtered_df['signal_type'] == 'SELL'])
                st.metric("BUY/SELL Verhältnis", f"{buy_count}/{sell_count}")
            
            with kpi3:
                verified_count = len(filtered_df[filtered_df['verified'] == 1])
                if verified_count > 0:
                    success_count = len(filtered_df[(filtered_df['verified'] == 1) & (filtered_df['outcome'] == 'SUCCESS')])
                    success_rate = success_count / verified_count * 100
                    st.metric("Erfolgsrate", f"{success_rate:.1f}%")
                else:
                    st.metric("Erfolgsrate", "N/A")
            
            with kpi4:
                avg_confidence = filtered_df['confidence'].mean() * 100
                st.metric("Durchschn. Konfidenz", f"{avg_confidence:.1f}%")
            
            # Signale-Tabelle
            st.subheader("Signal-Details")
            
            # DataFrame für die Anzeige vorbereiten
            display_df = filtered_df[['symbol', 'timestamp', 'signal_type', 'confidence', 'close_price', 'reason', 'verified', 'outcome']].copy()
            display_df['confidence'] = (display_df['confidence'] * 100).round(1).astype(str) + '%'
            display_df.rename(columns={
                'symbol': 'Symbol',
                'timestamp': 'Zeitpunkt',
                'signal_type': 'Signal-Typ',
                'confidence': 'Konfidenz',
                'close_price': 'Kurs',
                'reason': 'Begründung',
                'verified': 'Verifiziert',
                'outcome': 'Ergebnis'
            }, inplace=True)
            
            st.dataframe(display_df, use_container_width=True)
            
            # Signal-Verteilung visualisieren
            st.subheader("Signal-Verteilung")
            
            col1, col2 = st.columns(2)
            
            with col1:
                # Signal-Typen nach Symbol
                signal_counts = filtered_df.groupby(['symbol', 'signal_type']).size().reset_index(name='count')
                fig = px.bar(
                    signal_counts,
                    x='symbol',
                    y='count',
                    color='signal_type',
                    title='Signal-Typen nach Symbol',
                    barmode='group'
                )
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                # Zeitliche Verteilung der Signale
                fig = px.histogram(
                    filtered_df,
                    x='timestamp',
                    color='signal_type',
                    title='Zeitliche Verteilung der Signale',
                    nbins=20
                )
                st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Keine Daten gefunden für die ausgewählten Filter.")

with tab2:
    # Performance-Analyse
    st.header("Signal-Performance")
    if not {'symbol', 'outcome', 'verified', 'confidence', 'timestamp'}.issubset(signals_df.columns):
        st.warning("Keine oder unvollständige Trading-Signale-Daten für Performance-Analyse vorhanden.")
    else:
        # Nur verifizierte Signale
        verified_df = signals_df[signals_df['verified'] == 1].copy()
        
        if not verified_df.empty:
            # Erfolgsrate nach Symbol
            st.subheader("Erfolgsrate nach Symbol")
            
            success_rate_df = verified_df.groupby('symbol')['outcome'].apply(
                lambda x: (x == 'SUCCESS').mean() * 100
            ).reset_index(name='success_rate')
            
            fig = px.bar(
                success_rate_df,
                x='symbol',
                y='success_rate',
                title='Erfolgsrate nach Symbol (%)',
                labels={'success_rate': 'Erfolgsrate (%)'}
            )
            st.plotly_chart(fig, use_container_width=True)
            
            # Erfolgsrate nach Signal-Typ
            st.subheader("Erfolgsrate nach Signal-Typ")
            
            col1, col2 = st.columns(2)
            
            with col1:
                signal_success_df = verified_df.groupby('signal_type')['outcome'].apply(
                    lambda x: (x == 'SUCCESS').mean() * 100
                ).reset_index(name='success_rate')
                
                fig = px.bar(
                    signal_success_df,
                    x='signal_type',
                    y='success_rate',
                    title='Erfolgsrate nach Signal-Typ (%)',
                    labels={'success_rate': 'Erfolgsrate (%)'}
                )
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                # Performance im Zeitverlauf
                verified_df['date'] = verified_df['timestamp'].dt.date
                performance_over_time = verified_df.groupby('date')['outcome'].apply(
                    lambda x: (x == 'SUCCESS').mean() * 100
                ).reset_index(name='success_rate')
                
                fig = px.line(
                    performance_over_time,
                    x='date',
                    y='success_rate',
                    title='Performance im Zeitverlauf',
                    labels={'success_rate': 'Erfolgsrate (%)'}
                )
                st.plotly_chart(fig, use_container_width=True)
            
            # Konfidenz vs. Erfolgsrate
            st.subheader("Konfidenz vs. Erfolgsrate")
            
            # Konfidenz in Bins einteilen
            verified_df['confidence_bin'] = pd.cut(
                verified_df['confidence'] * 100,
                bins=[0, 70, 80, 90, 100],
                labels=['70-80%', '80-90%', '90-100%', '100%']
            )
            
            confidence_success_df = verified_df.groupby('confidence_bin')['outcome'].apply(
                lambda x: (x == 'SUCCESS').mean() * 100
            ).reset_index(name='success_rate')
            
            fig = px.bar(
                confidence_success_df,
                x='confidence_bin',
                y='success_rate',
                title='Erfolgsrate nach Konfidenz-Level',
                labels={'success_rate': 'Erfolgsrate (%)', 'confidence_bin': 'Konfidenz-Bereich'}
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Keine verifizierten Signale gefunden für die Performance-Analyse.")

with tab3:
    # Technische Indikatoren
    st.header("Technische Indikatoren")
    if not {'symbol', 'timestamp', 'close_price', 'sma_20', 'sma_50', 'rsi', 'macd_line', 'signal_line', 'overall_signal'}.issubset(technical_df.columns):
        st.warning("Keine oder unvollständige technische Analyse-Daten vorhanden.")
    else:
        # Symbol auswählen
        symbol = st.selectbox(
            "Symbol auswählen",
            options=sorted(technical_df['symbol'].unique())
        )
        
        # Daten für das ausgewählte Symbol filtern
        symbol_data = technical_df[technical_df['symbol'] == symbol].sort_values('timestamp')
        
        if not symbol_data.empty:
            # Technische Indikatoren visualisieren
            st.subheader(f"Technische Indikatoren für {symbol}")
            
            # Preischart mit SMAs
            fig = make_subplots(rows=3, cols=1, 
                               shared_xaxes=True, 
                               subplot_titles=("Preis & SMAs", "RSI", "MACD"),
                               vertical_spacing=0.1,
                               row_heights=[0.5, 0.25, 0.25])
            
            # Preischart
            fig.add_trace(
                go.Scatter(x=symbol_data['timestamp'], y=symbol_data['close_price'], name='Preis', line=dict(color='blue')),
                row=1, col=1
            )
            
            # SMAs
            fig.add_trace(
                go.Scatter(x=symbol_data['timestamp'], y=symbol_data['sma_20'], name='SMA 20', line=dict(color='orange')),
                row=1, col=1
            )
            
            fig.add_trace(
                go.Scatter(x=symbol_data['timestamp'], y=symbol_data['sma_50'], name='SMA 50', line=dict(color='green')),
                row=1, col=1
            )
            
            # RSI
            fig.add_trace(
                go.Scatter(x=symbol_data['timestamp'], y=symbol_data['rsi'], name='RSI', line=dict(color='purple')),
                row=2, col=1
            )
            
            # RSI-Linien bei 30 und 70
            fig.add_hline(y=30, line_dash="dot", row=2, col=1, line_color="red", annotation_text="Überverkauft")
            fig.add_hline(y=70, line_dash="dot", row=2, col=1, line_color="red", annotation_text="Überkauft")
            
            # MACD
            fig.add_trace(
                go.Scatter(x=symbol_data['timestamp'], y=symbol_data['macd_line'], name='MACD', line=dict(color='blue')),
                row=3, col=1
            )
            
            fig.add_trace(
                go.Scatter(x=symbol_data['timestamp'], y=symbol_data['signal_line'], name='Signal', line=dict(color='red')),
                row=3, col=1
            )
            
            # Layout anpassen
            fig.update_layout(height=800, title_text=f"Technische Analyse für {symbol}")
            st.plotly_chart(fig, use_container_width=True)
            
            # Signalverteilung
            signal_counts = symbol_data['overall_signal'].value_counts().reset_index()
            signal_counts.columns = ['Signal', 'Anzahl']
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("Signal-Verteilung")
                fig = px.pie(
                    signal_counts,
                    values='Anzahl',
                    names='Signal',
                    title=f'Signal-Verteilung für {symbol}'
                )
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                # Letzte technische Daten
                st.subheader("Aktuelle technische Daten")
                latest_data = symbol_data.iloc[-1]
                
                metrics = {
                    "Schlusskurs": f"${latest_data['close_price']:.2f}",
                    "SMA 20": f"${latest_data['sma_20']:.2f}",
                    "SMA 50": f"${latest_data['sma_50']:.2f}",
                    "RSI": f"{latest_data['rsi']:.1f}",
                    "MACD": f"{latest_data['macd_line']:.3f}",
                    "Signal Line": f"{latest_data['signal_line']:.3f}",
                    "Gesamtsignal": latest_data['overall_signal']
                }
                
                for metric, value in metrics.items():
                    st.metric(metric, value)
        else:
            st.info(f"Keine technischen Daten gefunden für {symbol}.")

with tab4:
    # Sentiment-Analyse
    st.header("Sentiment-Analyse")
    if not {'symbol', 'timestamp', 'dominant_sentiment', 'positive_score', 'neutral_score', 'negative_score', 'title', 'summary', 'confidence'}.issubset(sentiment_df.columns):
        st.warning("Keine oder unvollständige Sentiment-Daten vorhanden.")
    else:
        # Symbol auswählen
        symbol = st.selectbox(
            "Symbol auswählen",
            options=sorted(sentiment_df['symbol'].unique()),
            key="sentiment_symbol"
        )
        
        # Daten für das ausgewählte Symbol filtern
        symbol_sentiment = sentiment_df[sentiment_df['symbol'] == symbol].sort_values('timestamp')
        
        if not symbol_sentiment.empty:
            # Sentiment-Verteilung visualisieren
            st.subheader(f"Sentiment-Verteilung für {symbol}")
            
            col1, col2 = st.columns(2)
            
            with col1:
                # Sentiment-Verteilung
                sentiment_counts = symbol_sentiment['dominant_sentiment'].value_counts().reset_index()
                sentiment_counts.columns = ['Sentiment', 'Anzahl']
                
                fig = px.pie(
                    sentiment_counts,
                    values='Anzahl',
                    names='Sentiment',
                    title=f'Sentiment-Verteilung für {symbol}',
                    color='Sentiment',
                    color_discrete_map={'positive': 'green', 'neutral': 'blue', 'negative': 'red'}
                )
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                # Sentiment im Zeitverlauf
                fig = px.line(
                    symbol_sentiment,
                    x='timestamp',
                    y=['positive_score', 'neutral_score', 'negative_score'],
                    title=f'Sentiment-Scores im Zeitverlauf für {symbol}',
                    labels={
                        'positive_score': 'Positiv',
                        'neutral_score': 'Neutral',
                        'negative_score': 'Negativ',
                        'timestamp': 'Zeitpunkt'
                    }
                )
                st.plotly_chart(fig, use_container_width=True)
            
            # Neueste Nachrichten mit Sentiment
            st.subheader("Aktuelle Nachrichten mit Sentiment-Analyse")
            
            news_df = symbol_sentiment[['timestamp', 'title', 'summary', 'dominant_sentiment', 'confidence']].head(5)
            news_df['confidence'] = (news_df['confidence'] * 100).round(1).astype(str) + '%'
            news_df.rename(columns={
                'timestamp': 'Zeitpunkt',
                'title': 'Titel',
                'summary': 'Zusammenfassung',
                'dominant_sentiment': 'Sentiment',
                'confidence': 'Konfidenz'
            }, inplace=True)
            
            # Farbiges Sentiment
            def highlight_sentiment(val):
                if val == 'positive':
                    return 'background-color: rgba(0, 255, 0, 0.2)'
                elif val == 'negative':
                    return 'background-color: rgba(255, 0, 0, 0.2)'
                else:
                    return 'background-color: rgba(0, 0, 255, 0.2)'
            
            st.dataframe(news_df.style.applymap(highlight_sentiment, subset=['Sentiment']), use_container_width=True)
        else:
            st.info(f"Keine Sentiment-Daten gefunden für {symbol}.")

with tab5:
    st.header("Backtest-Analyse")
    st.info("Führen Sie Backtests mit erweiterten Parametern durch und visualisieren Sie die Ergebnisse.")
    
    # Parameter-Auswahl
    col1, col2, col3 = st.columns(3)
    with col1:
        bt_symbol = st.text_input("Symbol", value="DAX", key="bt_symbol")
    with col2:
        bt_start = st.date_input("Startdatum", key="bt_start")
    with col3:
        bt_end = st.date_input("Enddatum", key="bt_end")
    
    risk = st.slider("Risiko pro Trade (%)", 0.1, 5.0, 1.0, 0.1, key="bt_risk")
    walk_forward = st.checkbox("Walk-Forward Analyse", key="bt_wf")
    wf_window = st.number_input("Walk-Forward Fenstergröße (Tage)", min_value=5, max_value=60, value=10, step=1, key="bt_wf_win") if walk_forward else None
    wf_step = st.number_input("Walk-Forward Schrittweite (Tage)", min_value=1, max_value=30, value=5, step=1, key="bt_wf_step") if walk_forward else None
    monte_carlo = st.checkbox("Monte Carlo Simulation", key="bt_mc")
    mc_sims = st.number_input("Monte Carlo Simulationen", min_value=10, max_value=1000, value=100, step=10, key="bt_mc_sims") if monte_carlo else None
    mc_days = st.number_input("Monte Carlo Tage", min_value=5, max_value=252, value=30, step=1, key="bt_mc_days") if monte_carlo else None
    
    if st.button("Backtest starten", key="bt_run"):
        from backtesting import BacktestEngine
        engine = BacktestEngine('market_data.db', risk_per_trade=risk)
        results = engine.run_backtest([bt_symbol], bt_start, bt_end)
        st.subheader("Equity Curve")
        # Plot equity curve if available
        if 'equity_curve' in results:
            st.line_chart(results['equity_curve'])
        st.subheader("Trade-Statistiken")
        st.write(results)
        # Walk-Forward Analysis
        if walk_forward:
            st.subheader("Walk-Forward Analyse")
            wf_results = engine.run_walk_forward_analysis([bt_symbol], bt_start, bt_end, window_size=wf_window, step_size=wf_step)
            wf_df = pd.DataFrame(wf_results)
            st.write(wf_df)
            if not wf_df.empty:
                st.line_chart(wf_df['total_profit_loss'])
                st.bar_chart(wf_df['win_rate'])
        # Monte Carlo Simulation
        if monte_carlo:
            st.subheader("Monte Carlo Simulation")
            mc_results = engine.run_monte_carlo_simulation(n_simulations=mc_sims, n_days=mc_days)
            st.write(f"{mc_sims} Simulationen, {mc_days} Tage")
            fig, ax = plt.subplots()
            ax.hist(mc_results, bins=30, color='skyblue', edgecolor='black')
            ax.set_title('Monte Carlo Endwert-Verteilung')
            ax.set_xlabel('Endwert')
            ax.set_ylabel('Häufigkeit')
            st.pyplot(fig)
            st.write({
                'Mittelwert': np.mean(mc_results),
                'Median': np.median(mc_results),
                'Min': np.min(mc_results),
                'Max': np.max(mc_results),
                'Std': np.std(mc_results)
            })
    st.info("Wählen Sie Parameter und klicken Sie auf 'Backtest starten', um Ergebnisse zu sehen.")

with tab6:
    st.header("Live ML Prediction")
    st.info("Hier können Sie in Echtzeit Vorhersagen des ML-Modells abrufen.")
    
    # Set up columns for layout
    col1, col2 = st.columns([1, 1])
    
    with col1:
        # Symbol selection
        symbol = st.selectbox("Symbol", 
                            options=sorted(signals_df['symbol'].unique()) if 'symbol' in signals_df and not signals_df.empty else ["DAX"], 
                            key="ml_symbol")
        
        # Get latest price info for context
        latest_price = None
        price_change = None
        
        try:
            # Get latest price data
            price_data = safe_query("""
                SELECT close_price, 
                       LAG(close_price, 1) OVER (ORDER BY timestamp) as prev_close
                FROM market_data 
                WHERE symbol = ? 
                ORDER BY timestamp DESC 
                LIMIT 2
            """, params=(symbol,))
            
            if not price_data.empty:
                latest_price = price_data['close_price'].iloc[0]
                
                if 'prev_close' in price_data.columns and not pd.isna(price_data['prev_close'].iloc[0]):
                    prev_price = price_data['prev_close'].iloc[0]
                    price_change = (latest_price - prev_price) / prev_price * 100
        except Exception as e:
            st.warning(f"Fehler beim Laden der Preisdaten: {e}")
        
        # Display price information
        if latest_price is not None:
            st.metric(
                "Aktueller Kurs", 
                f"{latest_price:.2f}",
                f"{price_change:.2f}%" if price_change is not None else None,
                delta_color="normal"
            )
        
        # Get technical indicators for context
        try:
            tech_data = safe_query("""
                SELECT rsi, macd_line, signal_line, overall_signal
                FROM technical_analysis
                WHERE symbol = ?
                ORDER BY timestamp DESC
                LIMIT 1
            """, params=(symbol,))
            
            if not tech_data.empty:
                tech_indicators = {
                    "RSI": tech_data['rsi'].iloc[0],
                    "MACD": tech_data['macd_line'].iloc[0],
                    "Signal Line": tech_data['signal_line'].iloc[0],
                    "Technisches Signal": tech_data['overall_signal'].iloc[0]
                }
                
                # Display technical indicators
                for name, value in tech_indicators.items():
                    if name == "Technisches Signal":
                        st.info(f"{name}: {value}")
                    elif name == "RSI":
                        if value < 30:
                            st.warning(f"{name}: {value:.2f} (Überverkauft)")
                        elif value > 70:
                            st.warning(f"{name}: {value:.2f} (Überkauft)")
                        else:
                            st.info(f"{name}: {value:.2f}")
                    else:
                        st.info(f"{name}: {value:.4f}")
        except Exception as e:
            st.warning(f"Fehler beim Laden der technischen Indikatoren: {e}")
    
    # Button for prediction
    if st.button("ML-Vorhersage abrufen"):
        # Show spinner while loading
        with st.spinner("ML-Modell wird ausgeführt..."):
            try:
                # Import the market predictor here to avoid loading it unnecessarily
                from market_predictor import MarketPredictor
                
                # Initialize the predictor
                predictor = MarketPredictor('market_data.db')
                
                # Get prediction
                prediction = predictor.predict(symbol)
                
                if prediction and 'prediction' in prediction:
                    # Show the prediction result
                    with col2:
                        # Map prediction to user-friendly text and color
                        pred_map = {
                            'up': ('KAUFEN', 'green'),
                            'down': ('VERKAUFEN', 'red'),
                            'sideways': ('HALTEN', 'blue'),
                            'no_data': ('KEINE DATEN', 'gray'),
                            'insufficient_data': ('UNZUREICHENDE DATEN', 'orange'),
                            'error': ('FEHLER', 'red')
                        }
                        
                        pred_text, pred_color = pred_map.get(prediction['prediction'], ('UNBEKANNT', 'gray'))
                        
                        # Display prediction prominently
                        st.markdown(f"<h2 style='color:{pred_color};text-align:center;'>Prognose: {pred_text}</h2>", unsafe_allow_html=True)
                        
                        # Display confidence
                        conf_pct = prediction['confidence'] * 100
                        st.markdown(f"<h3 style='text-align:center;'>Konfidenz: {conf_pct:.1f}%</h3>", unsafe_allow_html=True)
                        
                        # Visualization
                        if 'probabilities' in prediction:
                            probs = prediction['probabilities']
                            prob_df = pd.DataFrame({
                                'Richtung': ['Aufwärts', 'Seitwärts', 'Abwärts'],
                                'Wahrscheinlichkeit': [probs.get('up', 0), probs.get('sideways', 0), probs.get('down', 0)]
                            })
                            
                            fig = px.bar(
                                prob_df, 
                                y='Richtung', 
                                x='Wahrscheinlichkeit', 
                                orientation='h',
                                color='Richtung',
                                color_discrete_map={
                                    'Aufwärts': 'green',
                                    'Seitwärts': 'blue',
                                    'Abwärts': 'red'
                                }
                            )
                            fig.update_layout(height=200)
                            st.plotly_chart(fig, use_container_width=True)
                        
                        # Explanation based on prediction
                        if prediction['prediction'] == 'up':
                            st.success("Das ML-Modell sagt eine Aufwärtsbewegung voraus.")
                        elif prediction['prediction'] == 'down':
                            st.error("Das ML-Modell sagt eine Abwärtsbewegung voraus.")
                        elif prediction['prediction'] == 'sideways':
                            st.info("Das ML-Modell sagt eine Seitwärtsbewegung voraus.")
                        elif prediction['prediction'] in ('no_data', 'insufficient_data'):
                            st.warning("Nicht genügend Daten für eine zuverlässige Vorhersage.")
                        else:
                            st.error(f"Fehler bei der Vorhersage: {prediction.get('error', 'Unbekannter Fehler')}")
                else:
                    st.error("Das ML-Modell konnte keine Vorhersage treffen.")
            except Exception as e:
                st.error(f"Fehler bei der ML-Vorhersage: {e}")
    else:
        # When no prediction is made yet, show some guidance
        with col2:
            st.info("Klicken Sie auf 'ML-Vorhersage abrufen', um eine Prognose für den ausgewählten Wert zu erhalten.")
            st.caption("Die Vorhersage basiert auf historischen Daten und technischen Indikatoren.")
            
    # Extra info section
    with st.expander("Informationen zum ML-Modell"):
        st.write("""
        Das Machine-Learning-Modell verwendet eine LSTM (Long Short-Term Memory) Netzwerkarchitektur, 
        trainiert auf historischen Marktdaten und technischen Indikatoren.
        
        **Eingangsdaten für das Modell:**
        - Historische Kursdaten (OHLCV)
        - Technische Indikatoren (RSI, MACD)
        - Marktmuster der letzten 60 Handelsperioden
        
        **Ausgabe:**
        - Wahrscheinlichkeit für drei mögliche Szenarien: Aufwärts, Seitwärts, Abwärts
        - Konfidenz der Vorhersage
        
        **Hinweis:** ML-Vorhersagen sollten nie als alleinige Entscheidungsgrundlage verwendet werden.
        Stets mit anderen Analysen und Risikomanagement kombinieren.
        """)

with tab7:
    # Use the refactored tab7_content module for better maintainability
    # and to avoid database connection issues
    PYTHON_EXEC = os.path.join(os.getcwd(), "trading_env", "Scripts", "python.exe")
    render_tab7(st, safe_query, PYTHON_EXEC)

with tab8:
    st.header("Batch Collection Monitoring")
    st.info("Monitor the performance and status of the batch data collection system")
    
    # Load collection stats
    with st.spinner("Loading batch collection data..."):
        stats_df = load_collection_stats()
    
    if stats_df.empty:
        st.warning("No batch collection statistics available yet. Run the batch collector first.")
    else:
        # Summary metrics
        st.subheader("Collection Performance Overview")
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            total_batches = len(stats_df)
            st.metric("Total Batches", total_batches)
        
        with col2:
            avg_elapsed = stats_df['elapsed_time'].mean()
            st.metric("Avg Time per Batch", f"{avg_elapsed:.2f}s")
            
        with col3:
            success_rate = (stats_df['symbols_received'] / stats_df['symbols_requested']).mean() * 100
            st.metric("Symbol Success Rate", f"{success_rate:.1f}%")
            
        with col4:
            error_rate = stats_df['errors'].sum() / len(stats_df)
            st.metric("Avg Errors per Batch", f"{error_rate:.2f}")
        
        # Time filters
        st.subheader("Collection History")
        
        # Calculate date range
        if not stats_df.empty and 'timestamp' in stats_df.columns:
            min_date = stats_df['timestamp'].min().date()
            max_date = stats_df['timestamp'].max().date()
            
            # Date range selector
            # Handle case where min_date and max_date are the same
            if min_date == max_date:
                selected_date_range = st.date_input(
                    "Select Date",
                    value=max_date,
                    min_value=min_date,
                    max_value=max_date
                )
                selected_date_range = (selected_date_range, selected_date_range)  # Convert to tuple for consistency
            else:
                default_start = max(min_date, max_date - datetime.timedelta(days=1))
                selected_date_range = st.date_input(
                    "Select Date Range",
                    value=(default_start, max_date),
                    min_value=min_date,
                    max_value=max_date
                )
            
            if len(selected_date_range) == 2:
                start_date, end_date = selected_date_range
                # Add one day to end_date to include the entire day
                end_date = pd.Timestamp(end_date) + pd.Timedelta(days=1)
                
                # Filter data by selected date range
                filtered_stats_df = stats_df[
                    (stats_df['timestamp'] >= pd.Timestamp(start_date)) & 
                    (stats_df['timestamp'] < end_date)
                ]
            else:
                filtered_stats_df = stats_df
        else:
            filtered_stats_df = stats_df
        
        # Display filtered data
        if not filtered_stats_df.empty:
            # Interval filter
            available_intervals = filtered_stats_df['interval'].unique()
            selected_interval = st.multiselect(
                "Filter by Interval",
                options=available_intervals,
                default=available_intervals
            )
            
            if selected_interval:
                filtered_stats_df = filtered_stats_df[filtered_stats_df['interval'].isin(selected_interval)]
            
            # Visualizations
            st.subheader("Collection Performance Trends")
            
            col1, col2 = st.columns(2)
            
            with col1:
                # Collection time trend
                if len(filtered_stats_df) > 1:
                    fig = px.line(
                        filtered_stats_df,
                        x='timestamp',
                        y='elapsed_time',
                        color='interval',
                        title='Collection Time Trend',
                        labels={'elapsed_time': 'Time (s)', 'timestamp': 'Date/Time'}
                    )
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("Not enough data points for trend visualization")
            
            with col2:
                # Success rate trend
                if len(filtered_stats_df) > 1:
                    filtered_stats_df['success_rate'] = (filtered_stats_df['symbols_received'] / filtered_stats_df['symbols_requested']) * 100
                    fig = px.line(
                        filtered_stats_df,
                        x='timestamp',
                        y='success_rate',
                        color='interval',
                        title='Symbol Collection Success Rate (%)',
                        labels={'success_rate': 'Success Rate (%)', 'timestamp': 'Date/Time'}
                    )
                    fig.update_layout(yaxis_range=[0, 100])
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("Not enough data points for trend visualization")
            
            # Batch Size vs Performance
            st.subheader("Batch Size vs. Performance")
            
            col1, col2 = st.columns(2)
            
            with col1:
                # Batch size vs collection time
                fig = px.scatter(
                    filtered_stats_df,
                    x='batch_size',
                    y='elapsed_time',
                    color='interval',
                    title='Batch Size vs. Collection Time',
                    labels={'elapsed_time': 'Time (s)', 'batch_size': 'Batch Size'}
                )
                st.plotly_chart(fig, use_container_width=True)
                
            with col2:
                # Error rate by interval
                interval_error_rate = filtered_stats_df.groupby('interval')['errors'].mean().reset_index()
                fig = px.bar(
                    interval_error_rate,
                    x='interval',
                    y='errors',
                    title='Average Errors by Interval',
                    labels={'errors': 'Avg. Errors', 'interval': 'Interval'}
                )
                st.plotly_chart(fig, use_container_width=True)
            
            # Raw data table
            st.subheader("Raw Collection Statistics")
            
            # Prepare display dataframe
            display_cols = ['timestamp', 'interval', 'period', 'batch_size', 'symbols_requested', 
                           'symbols_received', 'elapsed_time', 'errors']
            display_df = filtered_stats_df[display_cols].copy()
            display_df['success_rate'] = (display_df['symbols_received'] / display_df['symbols_requested'] * 100).round(1)
            
            # Add nice column names
            display_df = display_df.rename(columns={
                'timestamp': 'Timestamp',
                'interval': 'Interval',
                'period': 'Period',
                'batch_size': 'Batch Size',
                'symbols_requested': 'Requested',
                'symbols_received': 'Received',
                'elapsed_time': 'Time (s)',
                'errors': 'Errors',
                'success_rate': 'Success Rate (%)'
            })
            
            st.dataframe(display_df, use_container_width=True)
        else:
            st.info("No data available for the selected filters.")
    
    # Run batch collector section
    st.subheader("Run Batch Collection")
    
    with st.expander("Collect Data Now"):
        col1, col2, col3 = st.columns(3)
        
        with col1:
            mode = st.radio("Collection Mode", ["All Symbols", "Key Symbols Only"])
        
        with col2:
            interval = st.selectbox("Interval", ["1m", "5m", "15m", "30m", "1h", "1d"])
        
        with col3:
            period = st.selectbox("Period", ["1d", "5d", "1mo", "3mo"])
        
        batch_size = st.slider("Batch Size", min_value=10, max_value=100, value=40, step=5)
        
        if st.button("Start Collection"):
            with st.spinner("Running batch collection..."):
                symbols_arg = "key" if mode == "Key Symbols Only" else "all"
                command = f"python run_batch_pipeline.py --mode=enhanced --symbols={symbols_arg} --interval={interval} --period={period} --batch-size={batch_size}"
                
                try:
                    result = subprocess.run(
                        command.split(), 
                        check=True, 
                        capture_output=True, 
                        text=True
                    )
                    
                    st.success(f"Collection completed! {symbols_arg.capitalize()} symbols collected.")
                    st.code(result.stdout, language="text")
                    
                    # Clear cache to refresh data
                    st.cache_data.clear()
                except subprocess.CalledProcessError as e:
                    st.error(f"Error during collection: {e}")
                    st.code(e.stderr, language="text")

# Sidebar
st.sidebar.title("Trading Signal System")
st.sidebar.info("Dieses Dashboard bietet eine Echtzeit-Übersicht über generierte Trading-Signale und deren Performance für handelbare Instrumente auf Trade Republic.")

# System-Status
st.sidebar.header("System-Status")

# Letzte Aktualisierung
last_signal = signals_df['timestamp'].max() if not signals_df.empty and 'timestamp' in signals_df.columns else None
last_technical = technical_df['timestamp'].max() if not technical_df.empty and 'timestamp' in technical_df.columns else None
last_sentiment = sentiment_df['timestamp'].max() if not sentiment_df.empty and 'timestamp' in sentiment_df.columns else None

if last_signal is not None and pd.notnull(last_signal):
    try:
        st.sidebar.metric("Letztes Signal", last_signal.strftime('%d.%m.%Y %H:%M'))
    except:
        st.sidebar.metric("Letztes Signal", str(last_signal))

if last_technical is not None and pd.notnull(last_technical):
    try:
        st.sidebar.metric("Letzte technische Analyse", last_technical.strftime('%d.%m.%Y %H:%M'))
    except:
        st.sidebar.metric("Letzte technische Analyse", str(last_technical))

if last_sentiment is not None and pd.notnull(last_sentiment):
    try:
        st.sidebar.metric("Letzte Sentiment-Analyse", last_sentiment.strftime('%d.%m.%Y %H:%M'))
    except:
        st.sidebar.metric("Letzte Sentiment-Analyse", str(last_sentiment))

# Statistiken
st.sidebar.header("Statistiken")
st.sidebar.metric("Anzahl Signale (gesamt)", len(signals_df))

# Safely display unique symbols
if not signals_df.empty and 'symbol' in signals_df.columns:
    st.sidebar.metric("Analysierte Symbole", len(signals_df['symbol'].unique()))
else:
    st.sidebar.metric("Analysierte Symbole", "0")

# Verarbeitete Nachrichten
news_count = len(sentiment_df)
st.sidebar.metric("Verarbeitete Nachrichten", news_count)

# Aktualisieren-Button
if st.sidebar.button("Dashboard aktualisieren"):
    st.rerun()

# Symbol hinzufügen
st.sidebar.header("Symbol hinzufügen")
new_symbol = st.sidebar.text_input("Neues Symbol (z.B. AAPL, RHM.DE, K)")
add_symbol_btn = st.sidebar.button("Symbol-Daten abrufen und hinzufügen")
process_all_btn = st.sidebar.button("Alle Symbole verarbeiten")

PYTHON_EXEC = os.path.join(os.getcwd(), "trading_env", "Scripts", "python.exe")

if add_symbol_btn and new_symbol:
    import time
    progress = st.sidebar.progress(0, text="Starte Pipeline...")
    status = st.sidebar.empty()
    try:
        # Schritt 1: Daten abrufen
        status.info("[1/3] Marktdaten werden gesammelt...")
        result1 = subprocess.run([PYTHON_EXEC, "data_collector.py", "--symbol", new_symbol], 
                                capture_output=True, text=True)
        if result1.returncode != 0:
            raise RuntimeError(result1.stderr or result1.stdout)
            
        progress.progress(1/3, text="Technische Analyse läuft...")
        # Schritt 2: Technische Analyse
        result2 = subprocess.run([PYTHON_EXEC, "technical_analyzer.py", "--symbol", new_symbol], 
                                capture_output=True, text=True)
        if result2.returncode != 0:
            raise RuntimeError(result2.stderr or result2.stdout)
            
        progress.progress(2/3, text="Signale werden generiert...")
        # Schritt 3: Signal-Generierung
        result3 = subprocess.run([PYTHON_EXEC, "generate_all_signals_relaxed.py", "--single-symbol", new_symbol], 
                                capture_output=True, text=True)
        if result3.returncode != 0:
            raise RuntimeError(result3.stderr or result3.stdout)
            
        progress.progress(1.0, text="Fertig!")
        status.success(f"✅ Symbol '{new_symbol}' erfolgreich hinzugefügt und verarbeitet.")
        st.sidebar.balloons()
        
        # Clear cache to reload data after processing
        st.cache_data.clear()
        
        # Give the database a moment to finish any writes
        time.sleep(0.5)
    except Exception as e:
        status.error(f"Fehler in der Pipeline für '{new_symbol}': {e}")
        progress.empty()

# Process all symbols button
if process_all_btn:
    import time
    # Use subprocess to avoid threading issues with SQLite
    progress = st.sidebar.progress(0, text="Starte Batch-Verarbeitung...")
    status = st.sidebar.empty()
    try:
        status.info("Signale für alle Symbole werden generiert...")
        # Run in separate process to avoid database locking
        result = subprocess.run([PYTHON_EXEC, "generate_all_signals_relaxed.py"], 
                              capture_output=True, text=True)
        if result.returncode != 0:
            raise RuntimeError(result.stderr or result.stdout)
            
        progress.progress(1.0, text="Fertig!")
        status.success("✅ Alle Symbole erfolgreich verarbeitet.")
        st.sidebar.balloons()
        # Clear cache to reload data after processing
        st.cache_data.clear()
        # Give the database a moment to finish any writes
        time.sleep(0.5)
    except Exception as e:
        status.error(f"Fehler bei der Batch-Verarbeitung: {e}")
        progress.empty()

# Display recent signals alerts
display_signal_alerts()
