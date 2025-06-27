def render_tab7(st, safe_query, PYTHON_EXEC):
    """
    Render the Symbol-Abdeckung tab content with proper database handling
    """
    import subprocess
    
    st.header("Symbol-Abdeckung")
    
    # Get stats using safe direct SQL instead of connecting multiple times
    market_data_df = safe_query("SELECT COUNT(DISTINCT symbol) AS count FROM market_data")
    market_data_count = market_data_df['count'].iloc[0] if not market_data_df.empty else 0
    
    signal_df = safe_query("SELECT COUNT(DISTINCT symbol) AS count FROM trading_signals")
    signal_count = signal_df['count'].iloc[0] if not signal_df.empty else 0
    
    # Signal type distribution
    signal_types_df = safe_query("""
        SELECT signal_type, COUNT(*) as count
        FROM trading_signals 
        GROUP BY signal_type
        ORDER BY COUNT(*) DESC
    """)
    signal_types = signal_types_df.values.tolist() if not signal_types_df.empty else []
    
    # Get all symbols with data
    all_symbols_df = safe_query("SELECT DISTINCT symbol FROM market_data")
    all_symbols = all_symbols_df['symbol'].tolist() if not all_symbols_df.empty else []
    
    # Get all symbols with signals
    signal_symbols_df = safe_query("SELECT DISTINCT symbol FROM trading_signals")
    signal_symbols = signal_symbols_df['symbol'].tolist() if not signal_symbols_df.empty else []
    
    # Symbols without signals
    symbols_without_signals = list(set(all_symbols) - set(signal_symbols))
    
    # Display KPIs
    kpi1, kpi2, kpi3 = st.columns(3)
    with kpi1:
        st.metric("Symbole mit Marktdaten", market_data_count)
    with kpi2:
        st.metric("Symbole mit Signalen", signal_count)
    with kpi3:
        coverage_pct = round((signal_count / market_data_count * 100) if market_data_count > 0 else 0, 1)
        st.metric("Abdeckung", f"{coverage_pct}%")
    
    # Signal types distribution
    st.subheader("Signaltyp-Verteilung")
    if signal_types:
        import pandas as pd
        import plotly.express as px
        
        signal_types_df = pd.DataFrame(signal_types, columns=['signal_type', 'count'])
        fig = px.bar(
            signal_types_df,
            x='signal_type',
            y='count',
            title="Anzahl der Signale nach Typ",
            color='signal_type',
            color_discrete_map={
                'BUY': 'green',
                'SELL': 'red',
                'NO_SIGNAL': 'gray'
            }
        )
        st.plotly_chart(fig, use_container_width=True)
    
    # Symbols with most signals
    top_symbols_df = safe_query("""
        SELECT symbol, COUNT(*) as signal_count
        FROM trading_signals
        GROUP BY symbol
        ORDER BY signal_count DESC
        LIMIT 10
    """)
    top_symbols = top_symbols_df.values.tolist() if not top_symbols_df.empty else []
    
    if top_symbols:
        import pandas as pd
        import plotly.express as px
        
        st.subheader("Top 10 Symbole nach Signalanzahl")
        top_symbols_df = pd.DataFrame(top_symbols, columns=['symbol', 'signal_count'])
        fig = px.bar(
            top_symbols_df,
            x='symbol',
            y='signal_count',
            title="Top 10 Symbole nach Signalanzahl"
        )
        st.plotly_chart(fig, use_container_width=True)
    
    # Symbols without signals
    if symbols_without_signals:
        st.subheader("Symbole ohne Signale")
        st.write(f"{len(symbols_without_signals)} Symbole haben noch keine Signale")
        st.write(symbols_without_signals)
        
        if st.button("Signale für alle fehlenden Symbole generieren"):
            progress = st.progress(0, text="Generiere Signale für fehlende Symbole...")
            
            try:
                for i, symbol in enumerate(symbols_without_signals):
                    progress_value = (i + 1) / len(symbols_without_signals)
                    progress_text = f"Verarbeite Symbol {i+1}/{len(symbols_without_signals)}: {symbol}"
                    progress.progress(progress_value, text=progress_text)
                    
                    subprocess.run(
                        [PYTHON_EXEC, "generate_all_signals_relaxed.py", "--single-symbol", symbol],
                        capture_output=True, 
                        text=True
                    )
                
                st.success(f"Signale für {len(symbols_without_signals)} Symbole generiert")
                st.rerun()
            except Exception as e:
                st.error(f"Fehler bei der Signalgenerierung: {e}")
