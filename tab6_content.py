"""
ML Prediction Dashboard Tab (Tab 6)

This module provides enhanced visualization and performance metrics for ML predictions.
It is designed to be imported and used by the main dashboard.py file.
"""

import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import os
import sqlite3
from datetime import datetime, timedelta
import json

def render_ml_prediction_tab(st, safe_query):
    """
    Renders the ML prediction tab with enhanced visualizations and metrics
    
    Args:
        st: Streamlit instance
        safe_query: Function to safely query the database
    """
    st.header("Live ML Prediction")
    
    # Create tabs within the ML prediction tab for different views
    ml_tab1, ml_tab2, ml_tab3 = st.tabs([
        "Live Prediction", 
        "Model Performance", 
        "Historical Accuracy"
    ])
    
    # Get all available symbols for selection
    symbols_df = safe_query("SELECT DISTINCT symbol FROM market_data ORDER BY symbol")
    available_symbols = symbols_df['symbol'].tolist() if not symbols_df.empty else ["DAX"]
    
    # ----- TAB 1: LIVE PREDICTION -----
    with ml_tab1:
        st.info("Get real-time predictions from the ML model for market trends.")
        
        # Set up columns for layout
        col1, col2 = st.columns([1, 1])
        
        with col1:
            # Symbol selection
            symbol = st.selectbox("Symbol", 
                                options=available_symbols,
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
                st.warning(f"Error loading price data: {e}")
            
            # Display price information
            if latest_price is not None:
                st.metric(
                    "Current Price", 
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
                        "Technical Signal": tech_data['overall_signal'].iloc[0]
                    }
                    
                    # Display technical indicators
                    for name, value in tech_indicators.items():
                        if name == "Technical Signal":
                            st.info(f"{name}: {value}")
                        elif name == "RSI":
                            if value < 30:
                                st.warning(f"{name}: {value:.2f} (Oversold)")
                            elif value > 70:
                                st.warning(f"{name}: {value:.2f} (Overbought)")
                            else:
                                st.info(f"{name}: {value:.2f}")
                        else:
                            st.info(f"{name}: {value:.4f}")
            except Exception as e:
                st.warning(f"Error loading technical indicators: {e}")
        
        # Button for prediction
        if st.button("Get ML Prediction"):
            # Show spinner while loading
            with st.spinner("Running ML model..."):
                try:
                    # Import the market predictor here to avoid loading it unnecessarily
                    from market_predictor import MarketPredictor
                    
                    # Initialize the predictor
                    predictor = MarketPredictor('market_data.db')
                    
                    # Get prediction
                    prediction = predictor.predict(symbol=symbol)
                    
                    if prediction and 'prediction' in prediction:
                        # Show the prediction result
                        with col2:
                            # Map prediction to user-friendly text and color
                            pred_map = {
                                'up': ('BUY', 'green'),
                                'down': ('SELL', 'red'),
                                'sideways': ('HOLD', 'blue'),
                                'no_data': ('NO DATA', 'gray'),
                                'insufficient_data': ('INSUFFICIENT DATA', 'orange'),
                                'error': ('ERROR', 'red')
                            }
                            
                            pred_text, pred_color = pred_map.get(prediction['prediction'], ('UNKNOWN', 'gray'))
                            
                            # Display prediction prominently
                            st.markdown(f"<h2 style='color:{pred_color};text-align:center;'>Prediction: {pred_text}</h2>", unsafe_allow_html=True)
                            
                            # Display confidence
                            conf_pct = prediction['confidence'] * 100
                            st.markdown(f"<h3 style='text-align:center;'>Confidence: {conf_pct:.1f}%</h3>", unsafe_allow_html=True)
                            
                            # Visualization
                            if 'probabilities' in prediction:
                                probs = prediction['probabilities']
                                prob_df = pd.DataFrame({
                                    'Direction': ['Upward', 'Sideways', 'Downward'],
                                    'Probability': [probs.get('up', 0), probs.get('sideways', 0), probs.get('down', 0)]
                                })
                                
                                fig = px.bar(
                                    prob_df, 
                                    y='Direction', 
                                    x='Probability', 
                                    orientation='h',
                                    color='Direction',
                                    color_discrete_map={
                                        'Upward': 'green',
                                        'Sideways': 'blue',
                                        'Downward': 'red'
                                    }
                                )
                                fig.update_layout(height=200)
                                st.plotly_chart(fig, use_container_width=True)
                            
                            # Store prediction in the database for historical tracking
                            try:
                                conn = sqlite3.connect('market_data.db')
                                cursor = conn.cursor()
                                
                                # Create table if it doesn't exist
                                cursor.execute("""
                                CREATE TABLE IF NOT EXISTS ml_predictions (
                                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                                    timestamp TEXT,
                                    symbol TEXT,
                                    prediction TEXT,
                                    confidence REAL,
                                    probabilities TEXT,
                                    verified INTEGER DEFAULT 0,
                                    actual_outcome TEXT
                                )
                                """)
                                
                                # Insert prediction
                                cursor.execute("""
                                INSERT INTO ml_predictions 
                                (timestamp, symbol, prediction, confidence, probabilities)
                                VALUES (?, ?, ?, ?, ?)
                                """, (
                                    datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                    symbol,
                                    prediction['prediction'],
                                    prediction['confidence'],
                                    json.dumps(prediction['probabilities']) if 'probabilities' in prediction else None
                                ))
                                
                                conn.commit()
                                conn.close()
                            except Exception as e:
                                st.warning(f"Error storing prediction in database: {e}")
                            
                            # Explanation based on prediction
                            if prediction['prediction'] == 'up':
                                st.success("The ML model predicts an upward movement.")
                            elif prediction['prediction'] == 'down':
                                st.error("The ML model predicts a downward movement.")
                            elif prediction['prediction'] == 'sideways':
                                st.info("The ML model predicts a sideways movement.")
                            elif prediction['prediction'] in ('no_data', 'insufficient_data'):
                                st.warning("Not enough data for a reliable prediction.")
                            else:
                                st.error(f"Prediction error: {prediction.get('error', 'Unknown error')}")
                    else:
                        st.error("The ML model could not make a prediction.")
                except Exception as e:
                    st.error(f"Error during ML prediction: {e}")
        else:
            # When no prediction is made yet, show some guidance
            with col2:
                st.info("Click 'Get ML Prediction' to get a forecast for the selected symbol.")
                st.caption("The prediction is based on historical data and technical indicators.")
    
    # ----- TAB 2: MODEL PERFORMANCE -----
    with ml_tab2:
        st.info("View detailed performance metrics for the ML prediction model.")
        
        perf_col1, perf_col2 = st.columns([1, 1])
        
        with perf_col1:
            perf_symbol = st.selectbox("Symbol", 
                                    options=available_symbols,
                                    key="perf_symbol")
            
            eval_period = st.slider("Evaluation Period (Days)", 
                                 min_value=30, 
                                 max_value=180, 
                                 value=90, 
                                 step=30,
                                 key="eval_period")
        
        with perf_col2:
            st.markdown("### Model Metrics")
            
            # Placeholder for metrics that will be filled after loading data
            metrics_container = st.container()
        
        # Load model performance data
        try:
            # First check if we have actual metrics in the database
            performance_data = safe_query("""
                SELECT * FROM ml_model_metrics 
                WHERE symbol = ? 
                ORDER BY evaluation_date DESC 
                LIMIT 1
            """, params=(perf_symbol,))
            
            if not performance_data.empty:
                # Great! We have actual metrics
                with metrics_container:
                    accuracy = performance_data['accuracy'].iloc[0]
                    precision = performance_data['precision'].iloc[0] if 'precision' in performance_data.columns else None
                    recall = performance_data['recall'].iloc[0] if 'recall' in performance_data.columns else None
                    
                    metrics_col1, metrics_col2, metrics_col3 = st.columns(3)
                    metrics_col1.metric("Accuracy", f"{accuracy:.2%}")
                    if precision is not None:
                        metrics_col2.metric("Precision", f"{precision:.2%}")
                    if recall is not None:
                        metrics_col3.metric("Recall", f"{recall:.2%}")
                
                # Load confusion matrix data if available
                confusion_data = safe_query("""
                    SELECT * FROM ml_confusion_matrix 
                    WHERE symbol = ? 
                    ORDER BY evaluation_date DESC 
                    LIMIT 1
                """, params=(perf_symbol,))
                
                if not confusion_data.empty and 'matrix_json' in confusion_data.columns:
                    try:
                        cm_data = json.loads(confusion_data['matrix_json'].iloc[0])
                        
                        # Plot confusion matrix
                        fig = make_subplots(rows=1, cols=1)
                        
                        labels = ['Down', 'Sideways', 'Up']
                        
                        # Handle both dictionary and array formats
                        if isinstance(cm_data, dict):
                            # Convert dictionary format to 2D array
                            tp = cm_data.get('true_positive', 0)
                            fp = cm_data.get('false_positive', 0)
                            tn = cm_data.get('true_negative', 0)
                            fn = cm_data.get('false_negative', 0)
                            
                            # Create a simple 2x2 confusion matrix
                            cm_array = np.array([
                                [tn, fp],
                                [fn, tp]
                            ])
                            labels = ['Negative', 'Positive']
                        else:
                            # Already in array format
                            cm_array = np.array(cm_data)
                        
                        # Create heatmap
                        heatmap = go.Heatmap(
                            z=cm_array,
                            x=labels,
                            y=labels,
                            colorscale='Blues',
                            showscale=True,
                            text=cm_array,
                            texttemplate="%{text}",
                            textfont={"size": 20}
                        )
                        
                        fig.add_trace(heatmap)
                        fig.update_layout(
                            title='Confusion Matrix',
                            width=600,
                            height=500,
                            xaxis_title='Predicted',
                            yaxis_title='Actual'
                        )
                        
                        st.plotly_chart(fig)
                        
                        # Calculate class-specific metrics
                        class_metrics = []
                        for i, label in enumerate(labels):
                            true_pos = cm_array[i, i]
                            false_pos = cm_array[:, i].sum() - true_pos
                            false_neg = cm_array[i, :].sum() - true_pos
                            
                            class_precision = true_pos / (true_pos + false_pos) if (true_pos + false_pos) > 0 else 0
                            class_recall = true_pos / (true_pos + false_neg) if (true_pos + false_neg) > 0 else 0
                            class_f1 = 2 * (class_precision * class_recall) / (class_precision + class_recall) if (class_precision + class_recall) > 0 else 0
                            
                            class_metrics.append({
                                'Class': label,
                                'Precision': class_precision,
                                'Recall': class_recall,
                                'F1 Score': class_f1
                            })
                        
                        # Display class metrics
                        st.markdown("### Class-specific Metrics")
                        st.dataframe(pd.DataFrame(class_metrics).style.format({
                            'Precision': '{:.2%}',
                            'Recall': '{:.2%}',
                            'F1 Score': '{:.2%}'
                        }))
                        
                    except (json.JSONDecodeError, ValueError) as e:
                        st.warning(f"Could not parse confusion matrix data: {e}")
                
            else:
                # Get prediction and actual data to compute metrics on the fly
                results_data = safe_query("""
                    SELECT p.timestamp, p.prediction, p.confidence, p.probability_up, p.probability_down,
                           m.open, m.close, (m.close - m.open) / m.open * 100 as daily_return
                    FROM ml_predictions p
                    JOIN market_data m ON p.timestamp = m.timestamp AND p.symbol = m.symbol
                    WHERE p.symbol = ? 
                    AND p.timestamp >= date('now', ?)
                    ORDER BY p.timestamp DESC
                """, params=(perf_symbol, f'-{eval_period} days'))
                
                if not results_data.empty:
                    # Calculate basic metrics from predictions
                    results_data['actual'] = pd.cut(
                        results_data['daily_return'],
                        bins=[-np.inf, -0.1, 0.1, np.inf],
                        labels=['down', 'sideways', 'up']
                    )
                    results_data['correct'] = results_data['prediction'] == results_data['actual']
                    accuracy = results_data['correct'].mean()
                    
                    with metrics_container:
                        metrics_col1, metrics_col2 = st.columns(2)
                        metrics_col1.metric("Accuracy", f"{accuracy:.2%}")
                        metrics_col2.metric("Predictions Count", len(results_data))
                    
                    # Plot accuracy over time
                    results_data['timestamp'] = pd.to_datetime(results_data['timestamp'])
                    results_data = results_data.sort_values('timestamp')
                    
                    # Calculate rolling accuracy
                    window = min(14, len(results_data) // 3)
                    if window > 0:
                        results_data['rolling_accuracy'] = results_data['correct'].rolling(window=window, min_periods=1).mean()
                        
                        fig = px.line(results_data, x='timestamp', y='rolling_accuracy',
                                     title=f'Rolling {window}-day Prediction Accuracy')
                        fig.add_hline(y=0.33, line_dash="dash", line_color="red", 
                                     annotation_text="Random Guess (33%)")
                        fig.add_hline(y=accuracy, line_color="green",
                                     annotation_text=f"Overall Accuracy ({accuracy:.2%})")
                        fig.update_layout(yaxis_tickformat='.0%')
                        st.plotly_chart(fig)
                    
                    # Create a primitive confusion matrix
                    cm = pd.crosstab(results_data['actual'], results_data['prediction'])
                    
                    # Display the confusion matrix
                    st.markdown("### Confusion Matrix")
                    if not cm.empty:
                        # Normalize
                        cm_norm = cm.div(cm.sum(axis=1), axis=0)
                        
                        # Plot
                        fig = make_subplots(rows=1, cols=2, subplot_titles=("Counts", "Percentage"))
                        
                        # Counts heatmap
                        heatmap1 = go.Heatmap(
                            z=cm.values,
                            x=cm.columns,
                            y=cm.index,
                            colorscale='Blues',
                            showscale=False,
                            text=cm.values,
                            texttemplate="%{text}",
                            textfont={"size": 20}
                        )
                        
                        # Percentage heatmap
                        heatmap2 = go.Heatmap(
                            z=cm_norm.values,
                            x=cm_norm.columns,
                            y=cm_norm.index,
                            colorscale='Blues',
                            showscale=True,
                            text=cm_norm.values,
                            texttemplate="%{text:.0%}",
                            textfont={"size": 20}
                        )
                        
                        fig.add_trace(heatmap1, row=1, col=1)
                        fig.add_trace(heatmap2, row=1, col=2)
                        fig.update_layout(width=800, height=400)
                        st.plotly_chart(fig)
                else:
                    st.warning(f"No prediction data found for {perf_symbol} in the last {eval_period} days.")
                    
        except Exception as e:
            st.error(f"Error loading performance data: {str(e)}")
    
    # ----- TAB 3: HISTORICAL ACCURACY -----
    with ml_tab3:
        st.info("Compare historical predictions with actual market movements.")
        
        # Set up parameters
        hist_col1, hist_col2 = st.columns([1, 1])
        
        with hist_col1:
            hist_symbol = st.selectbox("Symbol", 
                                    options=available_symbols,
                                    key="hist_symbol")
            
        with hist_col2:
            lookback = st.slider("Lookback Period (Days)", 
                               min_value=30, 
                               max_value=180, 
                               value=90, 
                               step=30,
                               key="hist_lookback")
        
        # Load historical predictions and actual price data
        try:
            # Get prediction data
            pred_data = safe_query("""
                SELECT p.timestamp, p.prediction, p.confidence, 
                       p.probability_up, p.probability_down, p.probability_sideways
                FROM ml_predictions p
                WHERE p.symbol = ? 
                AND p.timestamp >= date('now', ?)
                ORDER BY p.timestamp ASC
            """, params=(hist_symbol, f'-{lookback} days'))
            
            # Get market data
            market_data = safe_query("""
                SELECT timestamp, open, high, low, close, volume
                FROM market_data
                WHERE symbol = ?
                AND timestamp >= date('now', ?)
                ORDER BY timestamp ASC
            """, params=(hist_symbol, f'-{lookback} days'))
            
            if not pred_data.empty and not market_data.empty:
                # Convert timestamps
                pred_data['timestamp'] = pd.to_datetime(pred_data['timestamp'])
                market_data['timestamp'] = pd.to_datetime(market_data['timestamp'])
                
                # Merge data
                merged_data = pd.merge(market_data, pred_data, on='timestamp', how='left')
                merged_data['prediction'].fillna('unknown', inplace=True)
                
                # Calculate returns
                merged_data['daily_return'] = merged_data['close'].pct_change() * 100
                
                # Calculate actual movement
                merged_data['actual_movement'] = pd.cut(
                    merged_data['daily_return'],
                    bins=[-np.inf, -0.1, 0.1, np.inf],
                    labels=['down', 'sideways', 'up']
                )
                
                # Fill missing values
                merged_data['actual_movement'].fillna('unknown', inplace=True)
                
                # Mark correct predictions
                merged_data['correct'] = merged_data['prediction'] == merged_data['actual_movement']
                merged_data['correct'] = merged_data['correct'].astype(float)  # Convert to numeric for rolling avg
                
                # Only consider rows with predictions
                pred_rows = merged_data[merged_data['prediction'] != 'unknown']
                
                if len(pred_rows) > 0:
                    # Calculate accuracy
                    accuracy = pred_rows['correct'].mean()
                    pred_count = len(pred_rows)
                    
                    # Display metrics
                    met_col1, met_col2 = st.columns(2)
                    met_col1.metric("Overall Accuracy", f"{accuracy:.2%}")
                    met_col2.metric("Total Predictions", pred_count)
                    
                    # Create combination chart (price line + prediction markers)
                    fig = make_subplots(specs=[[{"secondary_y": True}]])
                    
                    # Price line
                    fig.add_trace(
                        go.Scatter(
                            x=merged_data['timestamp'],
                            y=merged_data['close'],
                            name="Price",
                            line=dict(color='blue'),
                            showlegend=True
                        )
                    )
                    
                    # Up predictions (green triangles)
                    up_preds = merged_data[merged_data['prediction'] == 'up']
                    fig.add_trace(
                        go.Scatter(
                            x=up_preds['timestamp'],
                            y=up_preds['close'],
                            mode='markers',
                            marker=dict(
                                symbol='triangle-up',
                                size=12,
                                color='green',
                                line=dict(width=1)
                            ),
                            name="Up Prediction"
                        )
                    )
                    
                    # Down predictions (red triangles)
                    down_preds = merged_data[merged_data['prediction'] == 'down']
                    fig.add_trace(
                        go.Scatter(
                            x=down_preds['timestamp'],
                            y=down_preds['close'],
                            mode='markers',
                            marker=dict(
                                symbol='triangle-down',
                                size=12,
                                color='red',
                                line=dict(width=1)
                            ),
                            name="Down Prediction"
                        )
                    )
                    
                    # Sideways predictions (yellow circles)
                    sideways_preds = merged_data[merged_data['prediction'] == 'sideways']
                    fig.add_trace(
                        go.Scatter(
                            x=sideways_preds['timestamp'],
                            y=sideways_preds['close'],
                            mode='markers',
                            marker=dict(
                                symbol='circle',
                                size=10,
                                color='yellow',
                                line=dict(width=1, color='black')
                            ),
                            name="Sideways Prediction"
                        )
                    )
                    
                    # Add accuracy line if we have enough data
                    if len(pred_rows) >= 5:  # Need at least 5 predictions for rolling average
                        # Calculate rolling accuracy
                        window_size = min(14, max(5, len(pred_rows) // 4))
                        merged_data['rolling_accuracy'] = merged_data['correct'].rolling(
                            window=window_size, min_periods=3).mean()
                        
                        fig.add_trace(
                            go.Scatter(
                                x=merged_data['timestamp'],
                                y=merged_data['rolling_accuracy'],
                                name=f"{window_size}-day Rolling Accuracy",
                                line=dict(color='purple', dash='dash'),
                                yaxis="y2"
                            ), 
                            secondary_y=True
                        )
                        
                        # Add baseline
                        fig.add_trace(
                            go.Scatter(
                                x=[merged_data['timestamp'].min(), merged_data['timestamp'].max()],
                                y=[1/3, 1/3],  # Random guess (3 classes)
                                name="Random Guess (33%)",
                                line=dict(color='gray', dash='dot'),
                                yaxis="y2"
                            ),
                            secondary_y=True
                        )
                    
                    # Update layout
                    fig.update_layout(
                        title=f"{hist_symbol} Price with Predictions (Last {lookback} Days)",
                        xaxis_title="Date",
                        yaxis_title="Price",
                        legend=dict(
                            orientation="h",
                            yanchor="bottom",
                            y=1.02,
                            xanchor="center",
                            x=0.5
                        ),
                        height=600
                    )
                    
                    # Configure y-axes
                    fig.update_yaxes(title_text="Price", secondary_y=False)
                    fig.update_yaxes(title_text="Accuracy", 
                                  secondary_y=True, 
                                  tickformat='.0%',
                                  range=[0, 1])
                    
                    st.plotly_chart(fig, use_container_width=True)
                    
                    # Show prediction distribution
                    st.markdown("### Prediction Distribution")
                    
                    pred_counts = pred_rows['prediction'].value_counts().reset_index()
                    pred_counts.columns = ['Prediction', 'Count']
                    
                    fig_pie = px.pie(
                        pred_counts, 
                        values='Count', 
                        names='Prediction',
                        color='Prediction', 
                        color_discrete_map={
                            'up': 'green',
                            'down': 'red',
                            'sideways': 'yellow'
                        },
                        title='Prediction Distribution'
                    )
                    
                    st.plotly_chart(fig_pie, use_container_width=True)
                    
                    # Show detailed prediction performance by type
                    st.markdown("### Prediction Performance by Type")
                    
                    performance_by_type = pred_rows.groupby('prediction')['correct'].agg(
                        ['mean', 'count']).reset_index()
                    performance_by_type.columns = ['Prediction Type', 'Accuracy', 'Count']
                    
                    # Format
                    performance_by_type['Accuracy'] = performance_by_type['Accuracy'].apply(
                        lambda x: f"{x:.2%}")
                    
                    st.dataframe(performance_by_type, use_container_width=True)
                    
                else:
                    st.warning(f"No predictions found for {hist_symbol} in the last {lookback} days.")
            else:
                st.warning(f"Insufficient data for {hist_symbol} in the last {lookback} days.")
                
        except Exception as e:
            st.error(f"Error processing historical data: {str(e)}")
    
    # Information about the ML model
    with st.expander("About the ML Model"):
        st.write("""
        The Machine Learning model uses an LSTM (Long Short-Term Memory) network architecture, 
        trained on historical market data and technical indicators.
        
        **Input features:**
        - Historical price data (OHLCV)
        - Technical indicators (RSI, MACD)
        - Market patterns from the last 60 trading periods
        
        **Output:**
        - Probability for three possible scenarios: Upward, Sideways, Downward
        - Prediction confidence level
        
        **Performance tracking:**
        - The system tracks all predictions and verifies them against actual outcomes
        - Performance metrics are updated daily to reflect prediction accuracy
        """)
        
        st.info("Symbol-specific models are used when available. Otherwise, a general model is applied.")

def verify_predictions(conn):
    """
    Verifies and creates ML prediction-related tables if they don't exist
    
    Args:
        conn: SQLite database connection
    """
    try:
        cursor = conn.cursor()
        
        # ML Predictions table - Stores individual predictions
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS ml_predictions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            symbol TEXT,
            prediction TEXT,
            confidence REAL,
            probability_up REAL,
            probability_down REAL,
            probability_sideways REAL,
            UNIQUE(timestamp, symbol)
        )
        """)
        
        # ML Model Metrics table - Stores aggregated performance metrics
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS ml_model_metrics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT,
            evaluation_date TEXT,
            accuracy REAL,
            precision REAL,
            recall REAL,
            f1_score REAL,
            data_points INTEGER,
            lookback_days INTEGER,
            model_version TEXT
        )
        """)
        
        # ML Confusion Matrix table - Stores confusion matrix data
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS ml_confusion_matrix (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT,
            evaluation_date TEXT,
            matrix_json TEXT,
            model_version TEXT
        )
        """)
        
        conn.commit()
        return True
    
    except Exception as e:
        print(f"Error verifying ML tables: {e}")
        return False

def verify_predictions(safe_query):
    """
    Verifies past predictions against actual outcomes
    
    Args:
        safe_query: Function to safely query the database
    """
    try:
        # Get unverified predictions older than 1 day
        conn = sqlite3.connect('market_data.db')
        cursor = conn.cursor()
        
        # Get unverified predictions
        cursor.execute("""
        SELECT id, timestamp, symbol, prediction
        FROM ml_predictions
        WHERE verified = 0 AND date(timestamp) < date('now', '-1 day')
        """)
        
        predictions = cursor.fetchall()
        
        for pred_id, timestamp, symbol, prediction in predictions:
            # Get actual price movement the next day
            pred_date = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S').date()
            next_day = pred_date + timedelta(days=1)
            
            # Get closing prices
            cursor.execute("""
            SELECT close_price
            FROM market_data
            WHERE symbol = ? AND date(timestamp) = ?
            """, (symbol, pred_date))
            
            pred_day_price = cursor.fetchone()
            
            cursor.execute("""
            SELECT close_price
            FROM market_data
            WHERE symbol = ? AND date(timestamp) = ?
            """, (symbol, next_day))
            
            next_day_price = cursor.fetchone()
            
            if pred_day_price and next_day_price:
                # Calculate actual movement
                price_change = (next_day_price[0] - pred_day_price[0]) / pred_day_price[0]
                
                if price_change > 0.001:
                    actual_outcome = 'up'
                elif price_change < -0.001:
                    actual_outcome = 'down'
                else:
                    actual_outcome = 'sideways'
                
                # Update prediction with verification
                cursor.execute("""
                UPDATE ml_predictions
                SET verified = 1, actual_outcome = ?
                WHERE id = ?
                """, (actual_outcome, pred_id))
        
        conn.commit()
        conn.close()
        
    except Exception as e:
        print(f"Error verifying predictions: {e}")
