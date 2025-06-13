import streamlit as st
import pandas as pd
import numpy as np
import sqlite3
import matplotlib.pyplot as plt
import datetime
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

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
    return sqlite3.connect('market_data.db')

# Daten laden
@st.cache_data(ttl=300)  # 5 Minuten Cache
def load_signals_data():
    conn = get_connection()
    query = """
    SELECT ts.id, ts.symbol, ts.timestamp, ts.signal_type, ts.confidence, 
           ts.close_price, ts.technical_signal, ts.sentiment_signal, 
           ts.reason, ts.notified, ts.verified, ts.outcome
    FROM trading_signals ts
    ORDER BY ts.timestamp DESC
    """
    df = pd.read_sql_query(query, conn)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    return df

@st.cache_data(ttl=300)
def load_technical_data():
    conn = get_connection()
    query = """
    SELECT ta.id, ta.symbol, ta.timestamp, ta.close_price, ta.sma_20, ta.sma_50, 
           ta.rsi, ta.macd_line, ta.signal_line, ta.overall_signal
    FROM technical_analysis ta
    ORDER BY ta.timestamp DESC
    """
    df = pd.read_sql_query(query, conn)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    return df

@st.cache_data(ttl=300)
def load_sentiment_data():
    conn = get_connection()
    query = """
    SELECT sr.news_id, sr.symbol, sr.negative_score, sr.neutral_score, sr.positive_score,
           sr.dominant_sentiment, sr.confidence, sr.timestamp, nd.title, nd.summary
    FROM sentiment_results sr
    JOIN news_data nd ON sr.news_id = nd.rowid
    ORDER BY sr.timestamp DESC
    """
    df = pd.read_sql_query(query, conn)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    return df

# Titelbereich
st.title("Trading Signal System Dashboard")
st.subheader("Echtzeit-Überwachung und Performance-Analyse")

# Daten laden
signals_df = load_signals_data()
technical_df = load_technical_data()
sentiment_df = load_sentiment_data()

# Tabs erstellen
tab1, tab2, tab3, tab4 = st.tabs(["Signal-Übersicht", "Performance-Analyse", "Technische Indikatoren", "Sentiment-Analyse"])

with tab1:
    # Signal-Übersicht
    st.header("Aktuelle Trading-Signale")
    
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
        filtered_df = filtered_df[
            (filtered_df['timestamp'].dt.date >= start_date) & 
            (filtered_df['timestamp'].dt.date <= end_date)
        ]
    
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

# Sidebar
st.sidebar.title("Trading Signal System")
st.sidebar.info("Dieses Dashboard bietet eine Echtzeit-Übersicht über generierte Trading-Signale und deren Performance für handelbare Instrumente auf Trade Republic.")

# System-Status
st.sidebar.header("System-Status")

# Letzte Aktualisierung
last_signal = signals_df['timestamp'].max() if not signals_df.empty else None
last_technical = technical_df['timestamp'].max() if not technical_df.empty else None
last_sentiment = sentiment_df['timestamp'].max() if not sentiment_df.empty else None

if last_signal:
    st.sidebar.metric("Letztes Signal", last_signal.strftime('%d.%m.%Y %H:%M'))

if last_technical:
    st.sidebar.metric("Letzte technische Analyse", last_technical.strftime('%d.%m.%Y %H:%M'))

if last_sentiment:
    st.sidebar.metric("Letzte Sentiment-Analyse", last_sentiment.strftime('%d.%m.%Y %H:%M'))

# Statistiken
st.sidebar.header("Statistiken")
st.sidebar.metric("Anzahl Signale (gesamt)", len(signals_df))
st.sidebar.metric("Analysierte Symbole", len(signals_df['symbol'].unique()))

# Verarbeitete Nachrichten
news_count = len(sentiment_df)
st.sidebar.metric("Verarbeitete Nachrichten", news_count)

# Aktualisieren-Button
if st.sidebar.button("Dashboard aktualisieren"):
    st.experimental_rerun()
