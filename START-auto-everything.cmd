@echo off
REM ============================================================
REM  START auto-everything: full fleet (10 discovery agents +
REM  series sweep agent) + auto-restart loop + logon autostart.
REM  Same as start-pusher.bat, plus re-enabling autostart.
REM  Run again any time - safe to re-run.
REM ============================================================
cd /d "%~dp0"

echo [1/3] re-enabling logon autostart...
if exist "%APPDATA%\Microsoft\Windows\Start Menu\Programs\Startup\mkvbase-autostart.cmd.disabled" (
  ren "%APPDATA%\Microsoft\Windows\Start Menu\Programs\Startup\mkvbase-autostart.cmd.disabled" "mkvbase-autostart.cmd"
  echo   autostart re-enabled (starts again at every logon)
) else if exist "%APPDATA%\Microsoft\Windows\Start Menu\Programs\Startup\mkvbase-autostart.cmd" (
  echo   autostart already active
) else (
  echo   autostart script not found in Startup folder - copying from repo...
  if exist "deploy\startup-mkvbase-autostart.cmd" (
    copy /y "deploy\startup-mkvbase-autostart.cmd" "%APPDATA%\Microsoft\Windows\Start Menu\Programs\Startup\mkvbase-autostart.cmd" >nul
    echo   copied + activated
  ) else (
    echo   WARNING: no autostart source found, continuing without it
  )
)

echo [2/3] stopping any old pusher first (clean single instance)...
powershell -NoProfile -Command "Get-CimInstance Win32_Process -Filter \"Name='cmd.exe'\" | Where-Object { $_.CommandLine -like '*run-pusher*' } | ForEach-Object { Stop-Process -Id $_.ProcessId -Force }" >nul 2>&1
powershell -NoProfile -Command "Get-CimInstance Win32_Process -Filter \"Name='python.exe'\" | Where-Object { $_.CommandLine -like '*app.pusher*' } | ForEach-Object { Stop-Process -Id $_.ProcessId -Force }" >nul 2>&1
timeout /t 2 /nobreak >nul

echo [3/3] starting crawler loop (minimized)...
start "mkvbase-pusher" /min cmd /c ""%~dp0run-pusher.cmd""
timeout /t 5 /nobreak >nul
powershell -NoProfile -Command "$p = Get-CimInstance Win32_Process -Filter \"Name='python.exe'\" | Where-Object { $_.CommandLine -like '*app.pusher*' }; if ($p) { Write-Output ('PUSHER RUNNING: PID ' + ($p.ProcessId -join ',')) } else { Write-Output 'NOT STARTED YET - loop may still be launching; re-run this or check data\pusher.log' }"
echo.
echo Log: data\pusher.log   |   Stop everything: STOP-all-crawlers.cmd
pause
