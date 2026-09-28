@echo off
REM mkvbase pusher + discovery crawler - auto-restart wrapper.
REM Runs at logon / via START-auto-everything.cmd; loops forever.
cd /d "%~dp0"
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
if exist data\sync_key.txt set /p MKV_SYNC_KEY=<data\sync_key.txt
:loop
.venv\Scripts\python.exe -m app.pusher --terms "%MKV_PUSHER_TERMS%" --discover >> data\pusher.log 2>&1
echo [%date% %time%] pusher exited, restarting in 10s >> data\pusher.log
timeout /t 10 /nobreak >nul
goto loop
