@echo off
setlocal EnableExtensions

set "STREAMCRAFT_HOME=%~dp0.."
if "%STREAMCRAFT_PID_FILE%"=="" set "STREAMCRAFT_PID_FILE=%STREAMCRAFT_HOME%\streamcraft-service.pid"

if not exist "%STREAMCRAFT_PID_FILE%" (
  echo StreamCraft service is not running.
  exit /b 0
)

set "STREAMCRAFT_PID="
set /p STREAMCRAFT_PID=<"%STREAMCRAFT_PID_FILE%"
del "%STREAMCRAFT_PID_FILE%" >nul 2>nul

if not defined STREAMCRAFT_PID goto :stale
echo %STREAMCRAFT_PID%|findstr /r /c:"^[0-9][0-9]*$" >nul 2>nul
if errorlevel 1 goto :stale

rem Kill the whole process tree (/T). The java.exe resolved from PATH may be the
rem Oracle javapath forwarder, which spawns the real JVM as a child process, so
rem killing the recorded pid alone can leave the service running.
taskkill /PID %STREAMCRAFT_PID% /T /F >nul 2>nul
if errorlevel 1 goto :verify

echo StreamCraft service stopped, pid=%STREAMCRAFT_PID%.
exit /b 0

:verify
rem taskkill failed - only report success if the process is really gone
tasklist /FI "PID eq %STREAMCRAFT_PID%" 2>nul | findstr /c:"%STREAMCRAFT_PID%" >nul
if errorlevel 1 goto :stale
echo Failed to stop StreamCraft service, pid=%STREAMCRAFT_PID%.
exit /b 1

:stale
echo StreamCraft service is not running, removed stale pid file.
exit /b 0
