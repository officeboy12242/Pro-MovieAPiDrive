@echo off
REM ============================================================
REM  One-shot search: type a title, it crawls mkvbase NOW,
REM  saves to the vault AND pushes to Render. No fleet, no loop.
REM  (Same as: .venv\Scripts\python _search.py "<title>")
REM ============================================================
cd /d "%~dp0"
if not exist .venv\Scripts\python.exe (
  echo venv missing - run: python -m venv .venv ^&^& .venv\Scripts\pip install -r requirements.txt
  pause
  exit /b 1
)
:again
set "TITLE="
set /p TITLE=Title to search (e.g. tribhuvan mishra ca topper s01): 
if not defined TITLE goto :eof
.venv\Scripts\python.exe _search.py "%TITLE%"
echo.
goto :again
