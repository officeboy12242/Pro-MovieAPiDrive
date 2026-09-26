@echo off
REM ============================================================
REM  mkvbase pusher starter
REM  1. Reads the sync key from data\sync_key.txt
REM  2. Stops any pusher already running
REM  3. Starts a fresh pusher (hidden), log: data\pusher.log
REM  Run this AFTER setting MKV_SYNC_KEY on Render.
REM ============================================================
cd /d "%~dp0"
setlocal

if not exist data\sync_key.txt (
  echo Missing data\sync_key.txt - generate it first:
  echo   .venv\Scripts\python -c "import secrets; open('data/sync_key.txt','w').write(secrets.token_urlsafe(32))"
  pause
  exit /b 1
)

set /p MKV_SYNC_KEY=<data\sync_key.txt
set MKV_RENDER_URL=https://pro-movieapidrive.onrender.com
set MKV_DATA_DIR=data

REM stop any pusher already running (matches the -m app.pusher command line)
wmic process where "name='python.exe' and commandline like '%%app.pusher%%'" call terminate >nul 2>&1
powershell -NoProfile -Command "Get-CimInstance Win32_Process -Filter \"Name='python.exe'\" | Where-Object { $_.CommandLine -like '*app.pusher*' } | ForEach-Object { Stop-Process -Id $_.ProcessId -Force }" >nul 2>&1

start "mkvbase-pusher" /min .venv\Scripts\python.exe -m app.pusher --terms "godzilla,interstellar,predestination,oppenheimer" --discover
echo Pusher started (minimized window). Log: data\pusher.log
echo Watch it:   type data\pusher.log
echo Stop it:    taskkill /F /IM python.exe ...  ^(or close the window^)
pause
