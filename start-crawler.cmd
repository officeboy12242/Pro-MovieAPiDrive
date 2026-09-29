@echo off
REM ============================================================
REM  Local crawler: pusher + multi-agent discovery -> shared
REM  Mongo vault (Atlas). Render (serve-only) serves from it.
REM  Env: data\mongo_uri.txt (URI) + .env (everything else).
REM  Safe to re-run; stops any old local crawler first.
REM ============================================================
cd /d "%~dp0"

echo [1/3] loading .env + stopping old crawler...
if exist .env for /f "usebackq eol=# tokens=1,* delims==" %%A in (".env") do set "%%A=%%B"
if exist data\mongo_uri.txt set /p MKV_MONGODB_URI=<data\mongo_uri.txt

powershell -NoProfile -Command "Get-CimInstance Win32_Process -Filter \"Name='python.exe'\" | Where-Object { $_.CommandLine -like '*app.pusher*' } | ForEach-Object { Stop-Process -Id $_.ProcessId -Force }" >nul 2>&1
powershell -NoProfile -Command "Get-CimInstance Win32_Process -Filter \"Name='cmd.exe'\" | Where-Object { $_.CommandLine -match 'run-crawler-loop\.cmd|run-pusher\.cmd' } | ForEach-Object { Stop-Process -Id $_.ProcessId -Force }" >nul 2>&1
timeout /t 2 /nobreak >nul

echo [2/3] starting crawler loop (minimized)...
start "mkvbase-crawler" /min cmd /c ""%~dp0run-pusher.cmd""

echo [3/3] checking...
timeout /t 5 /nobreak >nul
powershell -NoProfile -Command "$p = Get-CimInstance Win32_Process -Filter \"Name='python.exe'\" | Where-Object { $_.CommandLine -like '*app.pusher*' }; if ($p) { Write-Output ('CRAWLER RUNNING: PID ' + ($p.ProcessId -join ',')) } else { Write-Output 'NOT STARTED YET - re-run or check data\pusher.log' }"
echo.
echo Log: data\pusher.log   |   Stop: STOP-local-crawler.cmd
pause
