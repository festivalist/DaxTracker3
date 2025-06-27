@echo off
echo DaxTracker3 ML Model Evaluation
echo ----------------------------
echo.

IF "%1"=="" (
  echo Evaluating all models...
  python ml_evaluator.py
) ELSE (
  echo Evaluating models for %1...
  python ml_evaluator.py --symbols %1
)

echo.
echo Press any key to exit...
pause > nul
