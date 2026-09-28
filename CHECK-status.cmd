@echo off
REM ============================================================
REM  CHECK: is the crawler fleet running or not?
REM  Shows: crawler processes, logon autostart state, live log
REM  tail, and the vault total on Render.
REM ============================================================
echo ================= MKVBASE CRAWLER STATUS =================

echo [1] Crawler processes:
powershell -NoProfile -Command "$p = Get-CimInstance Win32_Process -Filter \"Name='python.exe'\" | Where-Object { $_.CommandLine -like '*app.pusher*' }; if ($p) { Write-Output ('    RUNNING - PID ' + ($p.ProcessId -join ', ')) } else { Write-Output '    NOT RUNNING (crawling is off)' }"

echo [2] Autostart at logon:
if exist "%APPDATA%\Microsoft\Windows\Start Menu\Programs\Startup\mkvbase-autostart.cmd" (echo     ENABLED - fleet starts by itself at every logon) else if exist "%APPDATA%\Microsoft\Windows\Start Menu\Programs\Startup\mkvbase-autostart.cmd.disabled" (echo     disabled - won't start at logon) else (echo     not installed)

echo [3] Log heartbeat (last activity + newest lines):
powershell -NoProfile -Command "$age = ((Get-Date) - (Get-Item 'E:\Projects\mkvbase-cf-api\data\pusher.log').LastWriteTime).TotalMinutes; if ($age -lt 5) { Write-Output ('    log updated ' + [int]$age + ' min ago -> RUNNING') } elseif ($age -lt 1440) { Write-Output ('    log updated ' + [int]$age + ' min ago -> STOPPED (stale log)') } else { Write-Output ('    log updated ' + [int]($age/60) + ' hours ago -> STOPPED (stale log)') }; Get-Content 'E:\Projects\mkvbase-cf-api\data\pusher.log' -Tail 2 | ForEach-Object { Write-Output ('    ' + $_) }"

echo [4] Vault on Render:
curl -s https://pro-movieapidrive.onrender.com/health
echo.
echo ==========================================================
echo Start: START-auto-everything.cmd    Stop: STOP-all-crawlers.cmd
pause
