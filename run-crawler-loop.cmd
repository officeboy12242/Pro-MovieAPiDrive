@echo off
REM Auto-restart wrapper around the crawler. Started by start-crawler.cmd.
cd /d "%~dp0"
if exist .env for /f "usebackq eol=# tokens=1,* delims==" %%A in (".env") do set "%%A=%%B"
if exist data\mongo_uri.txt set /p MKV_MONGODB_URI=<data\mongo_uri.txt
:loop
.venv\Scripts\python.exe -m app.pusher --terms "%MKV_PUSHER_TERMS%" --discover >> data\pusher.log 2>&1
echo [%date% %time%] crawler exited, restarting in 10s >> data\pusher.log
timeout /t 10 /nobreak >nul
goto loop
