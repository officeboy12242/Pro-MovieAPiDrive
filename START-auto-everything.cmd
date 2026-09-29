@echo off
REM ============================================================
REM  START auto-everything: full fleet (10 discovery agents +
REM  series sweep agent) + auto-restart loop + logon autostart.
REM  Run again any time - safe to re-run. Kills any old fleet
REM  (BOTH wrapper kinds) first, so exactly one fleet runs.
REM  NOTE: powershell lines avoid embedded \" - cmd breaks on it.
REM ============================================================
cd /d "%~dp0"

echo [1/4] installing/refreshing logon autostart...
if exist "%APPDATA%\Microsoft\Windows\Start Menu\Programs\Startup\mkvbase-autostart.cmd.disabled" (
  ren "%APPDATA%\Microsoft\Windows\Start Menu\Programs\Startup\mkvbase-autostart.cmd.disabled" "mkvbase-autostart.cmd"
)
if exist "deploy\startup-mkvbase-autostart.cmd" (
  copy /y "deploy\startup-mkvbase-autostart.cmd" "%APPDATA%\Microsoft\Windows\Start Menu\Programs\Startup\mkvbase-autostart.cmd" >nul
  echo   autostart installed + refreshed (starts at every logon)
) else (
  echo   WARNING: no autostart source found, continuing without it
)

echo [2/4] stopping ANY old fleet (both wrapper kinds + pusher pythons + browser)...
powershell -NoProfile -Command "Get-CimInstance Win32_Process | Where-Object { $_.Name -eq 'cmd.exe' -and $_.CommandLine -match 'run-pusher\.cmd|run-crawler-loop\.cmd|start-pusher' } | ForEach-Object { Stop-Process -Id $_.ProcessId -Force }" >nul 2>&1
powershell -NoProfile -Command "Get-CimInstance Win32_Process | Where-Object { $_.Name -eq 'python.exe' -and $_.CommandLine -like '*app.pusher*' } | ForEach-Object { Stop-Process -Id $_.ProcessId -Force }" >nul 2>&1
powershell -NoProfile -Command "Get-CimInstance Win32_Process | Where-Object { $_.Name -eq 'firefox.exe' -and $_.CommandLine -like '*camoufox*' } | ForEach-Object { Stop-Process -Id $_.ProcessId -Force }" >nul 2>&1
ping -n 4 127.0.0.1 >nul

echo [3/4] starting crawler fleet (minimized)...
start "mkvbase-pusher" /min "%~dp0run-pusher.cmd"
ping -n 9 127.0.0.1 >nul

echo [4/4] checking...
powershell -NoProfile -Command "$p=@(Get-CimInstance Win32_Process | Where-Object { $_.Name -eq 'python.exe' -and $_.CommandLine -like '*app.pusher*' }); if ($p.Count) { Write-Output ('PUSHER RUNNING: PID ' + ($p.ProcessId -join ',')) } else { Write-Output 'NOT STARTED YET - loop may still be launching; re-run this or check data\pusher.log' }"
echo.
echo Log: data\pusher.log  -  Stop everything: STOP-all-crawlers.cmd
pause
