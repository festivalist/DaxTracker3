@echo off
echo Setting up Telegram notifications...
echo.

REM Activate virtual environment
call .\trading_env\Scripts\activate.bat

REM Run setup script
python telegram_config_setup.py

echo.
if errorlevel 1 (
    echo Setup failed. Please check the error messages above.
) else (
    echo Setup completed successfully!
    echo You can now start the trading signal server.
)

pause
