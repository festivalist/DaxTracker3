@echo off
echo DaxTracker3 ML Model Training
echo --------------------------
echo.

IF "%1"=="" (
  echo Training models for all symbols...
  python batch_train_ml_models.py
) ELSE (
  echo Training models for %1...
  python batch_train_ml_models.py --symbols %1
)

echo.
echo Press any key to exit...
pause > nul
