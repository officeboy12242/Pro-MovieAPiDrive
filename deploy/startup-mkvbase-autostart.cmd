@echo off
REM ============================================================
REM  mkvbase auto-start launcher - a copy of THIS FILE lives in
REM  the Startup folder (Win+R -> shell:startup). It runs
REM  run-pusher.cmd minimized at every logon, only if no pusher
REM  is already running. Real logic lives in run-pusher.cmd.
REM  START-auto-everything.cmd activates/copies this for you.
REM  %~dp0 of THIS copy resolves to the Startup folder, so the
REM  repo root is passed in by START when it refreshes the copy;
REM  standalone copies must update REPO below.
REM ============================================================
set "REPO=D:\GIT_PROJECTS\Pro-MovieAPiDrive"

powershell -NoProfile -Command "if (Get-CimInstance Win32_Process -Filter \"Name='python.exe'\" | Where-Object { $_.CommandLine -like '*app.pusher*' }) { exit 1 }"
if %errorlevel%==0 (
  start "mkvbase-pusher" /min cmd /c "\"%REPO%\run-pusher.cmd\""
)
