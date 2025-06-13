@echo off
echo Pausing ML Processing at %time% on %date% >> ml_pause.log
cd C:\path\to\your\project
call ml_env\Scripts\activate
python -c "import os, signal; os.kill(int(open('ml_processor.pid').read()), signal.SIGINT)"
