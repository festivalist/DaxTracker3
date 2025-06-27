@echo off
echo Starting Trading Signal System Dashboard...
cd %~dp0
call trading_env\Scripts\activate
streamlit run dashboard.py
