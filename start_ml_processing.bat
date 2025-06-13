@echo off
echo Starting ML Processing at %time% on %date% >> ml_startup.log
cd C:\path\to\your\project
call ml_env\Scripts\activate
python run_ml_processor.py
