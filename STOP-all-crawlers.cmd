@echo off
REM ============================================================
REM  STOP all mkvbase crawlers COMPLETELY
REM  1. kills the auto-restart loop + every pusher python process
REM  2. disables the logon autostart (Startup folder)
REM  Run again any time - safe to re-run.
REM ============================================================
echo [1/3] stopping pusher + restart loop...
powershell -NoProfile -Command "Get-CimInstance Win32_Process -Filter \"Name='cmd.exe'\" | Where-Object { $_.CommandLine -like '*run-crawler-loop*' -or $_.CommandLine -like '*run-pusher*' -or $_.CommandLine -like '*start-pusher*' } | ForEach-Object { Stop-Process -Id $_.ProcessId -Force; Write-Output ('  killed loop ' + $_.ProcessId) }"
powershell -NoProfile -Command "Get-CimInstance Win32_Process -Filter \"Name='python.exe'\" | Where-Object { $_.CommandLine -like '*app.pusher*' } | ForEach-Object { Stop-Process -Id $_.ProcessId -Force; Write-Output ('  killed crawler ' + $_.ProcessId) }"

echo [2/3] disabling logon autostart...
if exist "%APPDATA%\Microsoft\Windows\Start Menu\Programs\Startup\mkvbase-autostart.cmd" (
  ren "%APPDATA%\Microsoft\Windows\Start Menu\Programs\Startup\mkvbase-autostart.cmd" "mkvbase-autostart.cmd.disabled"
  echo   autostart disabled
) else (
  echo   autostart already disabled / absent
)

echo [3/3] verifying...
timeout /t 3 /nobreak >nul
powershell -NoProfile -Command "$left = Get-CimInstance Win32_Process -Filter \"Name='python.exe'\" | Where-Object { $_.CommandLine -like '*app.pusher*' }; if ($left) { Write-Output ('STILL RUNNING: ' + ($left.ProcessId -join ',')) } else { Write-Output 'ALL CRAWLERS STOPPED' }"
echo.
echo Note: /movie and the Render vault keep working. Only crawling is paused.
pause
