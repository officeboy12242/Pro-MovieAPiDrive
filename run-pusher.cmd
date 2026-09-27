@echo off
REM mkvbase pusher + discovery crawler - auto-restart wrapper.
REM Registered as a Scheduled Task (mkvbase-pusher); runs at logon, loops forever.
cd /d E:\Projects\mkvbase-cf-api
set MKV_RENDER_URL=https://pro-movieapidrive.onrender.com
set MKV_DATA_DIR=data
set PYTHONIOENCODING=utf-8
set PYTHONUTF8=1
REM Multi-agent discovery: 10 bots simultaneous (priority x3, day, year, alpha,
REM words, facet/OTT x2, words) + day-walk that also priority-pulls each day
set MKV_DISCOVERY_AGENTS=10
set MKV_DISCOVERY_GAP_S=15
set MKV_DISCOVERY_VAULT_PULL=5000
REM Cap concurrent mkvbase GETs — 10 agents used to stampede and kill cf_clearance
set MKV_HTTP_CONCURRENCY=2
set MKV_HTTP_GAP_S=0.4
set /p MKV_SYNC_KEY=<data\sync_key.txt
set /p MKV_MONGODB_URI=<data\mongo_uri.txt
:loop
.venv\Scripts\python.exe -m app.pusher --terms "godzilla,interstellar,predestination,oppenheimer" --discover >> data\pusher.log 2>&1
echo [%date% %time%] pusher exited, restarting in 10s >> data\pusher.log
timeout /t 10 /nobreak >nul
goto loop
