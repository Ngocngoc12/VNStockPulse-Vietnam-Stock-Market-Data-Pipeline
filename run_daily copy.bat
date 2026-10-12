@echo off
cd /d C:\Users\Admin\Downloads\vnstockpulse
call venv\Scripts\activate.bat
python -m ingestion.run_ingestion >> logs\ingestion_log_%date:~-4,4%%date:~-10,2%%date:~-7,2%.txt 2>&1