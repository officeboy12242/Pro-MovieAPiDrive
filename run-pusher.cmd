@echo off
REM ============================================================
REM  mkvbase pusher + discovery crawler - THE one auto-restart
REM  wrapper. Everything starts THIS script (START-auto-everything,
REM  start-crawler.cmd, logon autostart). run-crawler-loop.cmd is
REM  just an alias that forwards here.
REM  Single-instance guard refuses a second fleet, so the
REM  "file in use by another process" log fight can't happen.
REM  NOTE: powershell lines avoid embedded \" - cmd breaks on it.
REM ============================================================
title mkvbase-crawler-fleet
cd /d "%~dp0"

REM ---- rotate the log if it grew past 50 MB (once per wrapper start) ----
powershell -NoProfile -Command "$f='data\pusher.log'; if(Test-Path $f -PathType Leaf){ if((Get-Item $f).Length -gt 50MB){ if(Test-Path 'data\pusher.old.log'){Remove-Item 'data\pusher.old.log' -Force}; Move-Item $f 'data\pusher.old.log' -Force } }" >nul 2>&1

REM ---- single-instance guard: refuse if ANOTHER wrapper or pusher already runs ----
powershell -NoProfile -Command "$me=(Get-CimInstance Win32_Process -Filter ('ProcessId='+$PID)).ParentProcessId; $w=@(Get-CimInstance Win32_Process | Where-Object { $_.Name -eq 'cmd.exe' -and $_.CommandLine -match 'run-pusher\.cmd|run-crawler-loop\.cmd' -and $_.ProcessId -ne $me }); $p=@(Get-CimInstance Win32_Process | Where-Object { $_.Name -eq 'python.exe' -and $_.CommandLine -like '*app.pusher*' }); if($w.Count -ge 1 -or $p.Count -ge 1){ exit 1 }"
if errorlevel 1 (
  echo [%date% %time%] A crawler fleet is ALREADY running - not starting a second one.
  echo Stop it first with STOP-all-crawlers.cmd if you want a fresh start.
  ping -n 11 127.0.0.1 >nul
  exit /b 1
)

if exist .env for /f "usebackq eol=# tokens=1,* delims==" %%A in (".env") do set "%%A=%%B"
if exist data\mongo_uri.txt set /p MKV_MONGODB_URI=<data\mongo_uri.txt
set MKV_DATA_DIR=data
set PYTHONIOENCODING=utf-8
set PYTHONUTF8=1
REM Multi-agent discovery: 10 bots simultaneous + day-walk (env in .env; kept
REM here as defaults when .env is absent)
if not defined MKV_DISCOVERY_AGENTS set MKV_DISCOVERY_AGENTS=10
if not defined MKV_DISCOVERY_GAP_S set MKV_DISCOVERY_GAP_S=15
if not defined MKV_DISCOVERY_VAULT_PULL set MKV_DISCOVERY_VAULT_PULL=5000
REM Cap concurrent mkvbase GETs - agents used to stampede and kill cf_clearance
if not defined MKV_HTTP_CONCURRENCY set MKV_HTTP_CONCURRENCY=2
if not defined MKV_HTTP_GAP_S set MKV_HTTP_GAP_S=0.4
REM Headless Camoufox: Cloudflare verification runs invisibly (no popup windows)
if not defined MKV_ENGINE set MKV_ENGINE=camoufox
if not defined MKV_HEADLESS set MKV_HEADLESS=true
if not defined MKV_RENDER_URL set MKV_RENDER_URL=https://pro-movieapidrive.onrender.com
if not defined MKV_PUSHER_TERMS set MKV_PUSHER_TERMS=godzilla,interstellar,predestination,oppenheimer
REM idgap: 6 agents, 6s gap - it has slot priority and the best yield/search
if not defined MKV_IDGAP_AGENTS set MKV_IDGAP_AGENTS=6
if not defined MKV_IDGAP_GAP_S set MKV_IDGAP_GAP_S=6
if exist data\sync_key.txt set /p MKV_SYNC_KEY=<data\sync_key.txt

echo [%date% %time%] fleet wrapper starting >> data\pusher.log

:loop
.venv\Scripts\python.exe -m app.pusher --terms "%MKV_PUSHER_TERMS%" --discover >> data\pusher.log 2>&1
echo [%date% %time%] pusher exited, restarting in 10s >> data\pusher.log
ping -n 11 127.0.0.1 >nul
goto loop
