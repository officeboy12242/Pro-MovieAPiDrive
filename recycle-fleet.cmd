@echo off
REM ============================================================
REM  Fleet recycler - Task Scheduler runs this every 2 hours
REM  (task name: mkvbase-fleet-restart).
REM  Pusher running  -> kill it; run-pusher.cmd wrapper auto-
REM                     respawns a fresh one in ~10s (vault state
REM                     persists in data/idgap_state.json - same
REM                     as every manual restart).
REM  Nothing running -> relaunch the wrapper (covers reboot /
REM                     wrapper crash).
REM  Wrapper only (10s respawn sleep) -> do nothing.
REM  Idempotent: safe to fire at any moment.
REM ============================================================
cd /d "%~dp0"

powershell -NoProfile -Command "$p=@(Get-CimInstance Win32_Process | Where-Object { $_.Name -eq 'python.exe' -and $_.CommandLine -like '*app.pusher*' }); if($p.Count -gt 0){ $p | ForEach-Object { Stop-Process -Id $_.ProcessId -Force }; Write-Output ('['+(Get-Date -Format s)+'] recycled: killed '+$p.Count+' pusher process(es)') } else { $w=@(Get-CimInstance Win32_Process | Where-Object { $_.Name -eq 'cmd.exe' -and $_.CommandLine -like '*run-pusher*' }); if($w.Count -eq 0){ Start-Process -FilePath cmd.exe -ArgumentList '/c','D:\GIT_PROJECTS\Pro-MovieAPiDrive\run-pusher.cmd' -WorkingDirectory 'D:\GIT_PROJECTS\Pro-MovieAPiDrive' -WindowStyle Minimized; Write-Output ('['+(Get-Date -Format s)+'] fleet was DOWN: relaunched run-pusher.cmd') } else { Write-Output ('['+(Get-Date -Format s)+'] wrapper present, respawning on its own - no action') } }" >> data\recycle.log 2>&1
