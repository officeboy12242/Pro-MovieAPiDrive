# Restart the mkvbase pusher cleanly.
# Usage: powershell -NoProfile -ExecutionPolicy Bypass -File _restart_pusher.ps1
$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot

# 1. stop any pusher already running
Get-CimInstance Win32_Process -Filter "Name='python.exe'" |
  Where-Object { $_.CommandLine -like '*app.pusher*' } |
  ForEach-Object { Stop-Process -Id $_.ProcessId -Force; "stopped old pusher PID $($_.ProcessId)" }

# 2. load env
$env:MKV_SYNC_KEY  = (Get-Content "data\sync_key.txt" -Raw).Trim()
$env:MKV_RENDER_URL = "https://pro-movieapidrive.onrender.com"
$env:MKV_DATA_DIR   = "data"

# 3. start fresh (hidden, survives this console)
Start-Process -FilePath ".venv\Scripts\python.exe" `
  -ArgumentList "-m","app.pusher","--terms","godzilla,interstellar,predestination,oppenheimer","--discover" `
  -WindowStyle Hidden `
  -RedirectStandardOutput "data\pusher.log" `
  -RedirectStandardError "data\pusher.err.log"

Start-Sleep -Seconds 4

# 4. verify
$alive = Get-CimInstance Win32_Process -Filter "Name='python.exe'" |
  Where-Object { $_.CommandLine -like '*app.pusher*' }
if ($alive) {
  "PUSHER RUNNING: PID(s) $($alive.ProcessId -join ', ')"
  "--- log ---"
  Get-Content "data\pusher.log" -Tail 5
} else {
  "PUSHER FAILED TO START - check data\pusher.err.log"
  Get-Content "data\pusher.err.log" -Tail 10 -ErrorAction SilentlyContinue
}
