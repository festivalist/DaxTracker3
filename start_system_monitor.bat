@echo off
REM Start System Monitor and Backup Service for DaxTracker3
echo Starting DaxTracker3 System Monitor and Backup Service
echo --------------------------------------------------

REM Check if python is available
where python >nul 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo Python not found. Please install Python and try again.
    exit /b 1
)

REM Create initial backup
echo Creating initial database backup...
python system_monitor.py --backup
if %ERRORLEVEL% NEQ 0 (
    echo Warning: Initial backup failed. Continuing anyway...
)

REM Start monitoring service
echo Starting system monitoring service...
start "DaxTracker3 System Monitor" python system_monitor.py --monitor

echo System monitor started in background.
echo You can close this window, the monitor will continue running.
echo To stop the monitor, close the "DaxTracker3 System Monitor" window.
echo --------------------------------------------------

timeout /t 10
