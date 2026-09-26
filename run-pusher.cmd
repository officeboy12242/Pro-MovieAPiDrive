@echo off
REM mkvbase pusher + discovery crawler - auto-restart wrapper.
REM Registered as a Scheduled Task (mkvbase-pusher); runs at logon, loops forever.
cd /d E:\Projects\mkvbase-cf-api
set MKV_RENDER_URL=https://pro-movieapidrive.onrender.com
set MKV_DATA_DIR=data
set /p MKV_SYNC_KEY=<data\sync_key.txt
set /p MKV_MONGODB_URI=<data\mongo_uri.txt
:loop
.venv\Scripts\python.exe -m app.pusher --terms "godzilla,interstellar,predestination,oppenheimer" --discover >> data\pusher.log 2>&1
echo [%date% %time%] pusher exited, restarting in 10s >> data\pusher.log
timeout /t 10 /nobreak >nul
goto loop
