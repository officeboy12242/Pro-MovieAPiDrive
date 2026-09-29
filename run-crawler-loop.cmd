@echo off
REM Legacy alias - forwards to run-pusher.cmd (the one true fleet wrapper).
REM Kept so old habits/shortcuts still work without ever running two fleets.
call "%~dp0run-pusher.cmd"
