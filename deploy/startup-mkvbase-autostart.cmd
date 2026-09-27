@echo off
REM ============================================================
REM  mkvbase auto-start launcher - drop a copy of THIS FILE into
REM  the Startup folder (Win+R -> shell:startup). It runs
REM  run-pusher.cmd minimized at every logon, only if no pusher
REM  is already running. Real logic lives in run-pusher.cmd.
REM  START-auto-everything.cmd activates/copies this for you.
REM ============================================================
powershell -NoProfile -Command "if (Get-CimInstance Win32_Process -Filter \"Name='python.exe'\" | Where-Object { $_.CommandLine -like '*app.pusher*' }) { exit 1 }"
if %errorlevel%==0 (
  start "mkvbase-pusher" /min cmd /c ""E:\Projects\mkvbase-cf-api\run-pusher.cmd""
)
