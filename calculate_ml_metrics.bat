@echo off
echo DaxTracker3 ML Model Metrics Calculation
echo ------------------------------------
echo.

REM Process command line arguments
SET symbol=%1
SET mode=%2

IF "%mode%"=="demo" (
  echo Running in DEMO MODE
  IF "%symbol%"=="" (
    echo Generating demo metrics for common symbols...
    python demo_ml_metrics.py --symbol AAPL
    python demo_ml_metrics.py --symbol MSFT
    python demo_ml_metrics.py --symbol ^GDAXI
    python demo_ml_metrics.py --symbol AMZN
    python demo_ml_metrics.py --symbol GOOGL
  ) ELSE (
    echo Generating demo metrics for %symbol%...
    python demo_ml_metrics.py --symbol %symbol%
  )
) ELSE (
  REM Regular metrics calculation
  IF "%symbol%"=="" (
    echo Running metrics calculation for all models...
    python ml_metrics.py
  ) ELSE (
    echo Running metrics calculation for %symbol%...
    python ml_metrics.py --symbol %symbol% --lookback 30
  )
)

echo.
echo Metrics calculation complete!
echo Press any key to exit...
pause > nul
