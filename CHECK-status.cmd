@echo off
setlocal
cd /d "%~dp0"
title MKVBASE status
echo ============================================================
echo   MKVBASE VAULT - STATUS
echo ============================================================

echo.
echo [FLEET] crawler processes on this PC
powershell -NoProfile -Command "$p = Get-CimInstance Win32_Process -Filter \"Name='python.exe'\" | Where-Object { $_.CommandLine -like '*app.pusher*' }; if ($p) { Write-Output ('  RUNNING  - PID ' + ($p.ProcessId -join ', ')) } else { Write-Output '  OFF      - no crawler process (start: START-auto-everything.cmd)' }"
powershell -NoProfile -Command "if (Get-CimInstance Win32_Process -Filter \"Name='cmd.exe'\" | Where-Object { $_.CommandLine -like '*run-crawler-loop*' -or $_.CommandLine -like '*run-pusher*' }) { Write-Output '  loop     - auto-restart loop active' } else { Write-Output '  loop     - not active' }"

echo.
echo [AUTOSTART] at logon
if exist "%APPDATA%\Microsoft\Windows\Start Menu\Programs\Startup\mkvbase-autostart.cmd" (echo   ENABLED  - fleet starts by itself at every logon) else if exist "%APPDATA%\Microsoft\Windows\Start Menu\Programs\Startup\mkvbase-autostart.cmd.disabled" (echo   disabled - won't start at logon) else (echo   not installed)

echo.
if exist .venv\Scripts\python.exe (
  .venv\Scripts\python.exe _status.py 2>nul
) else (
  python _status.py 2>nul
)
if errorlevel 1 echo   (status details failed to load - check .venv)

echo.
echo ============================================================
echo   START: START-auto-everything.cmd     STOP: STOP-all-crawlers.cmd
echo   (also: start-crawler.cmd / STOP-local-crawler.cmd)
echo   SEARCH: search.cmd                   TEST: _check_series_agent.py
echo ============================================================
pause
