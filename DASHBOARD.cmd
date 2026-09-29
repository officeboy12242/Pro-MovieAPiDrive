@echo off
REM ============================================================
REM  Live vault dashboard: http://127.0.0.1:8766
REM  Shows mongo pushes, velocity, fleet + idgap status, log.
REM  Read-only; safe to run alongside the fleet.
REM ============================================================
cd /d "%~dp0"
if exist .env for /f "usebackq eol=# tokens=1,* delims==" %%A in (".env") do set "%%A=%%B"
if exist data\mongo_uri.txt set /p MKV_MONGODB_URI=<data\mongo_uri.txt

REM --- stop a previous dashboard instance (cmd-safe) ---
powershell -NoProfile -Command "Get-CimInstance Win32_Process | Where-Object { $_.Name -eq 'python.exe' -and $_.CommandLine -like '*app.dashboard*' } | ForEach-Object { Stop-Process -Id $_.ProcessId -Force }" >nul 2>&1

echo starting dashboard at http://127.0.0.1:8766 ...
start "mkvbase-dashboard" /min .venv\Scripts\python.exe -m app.dashboard
echo.
echo Open:  http://127.0.0.1:8766
echo Stop:  STOP-all-crawlers.cmd also stops this; or close its window.
ping -n 4 127.0.0.1 >nul
