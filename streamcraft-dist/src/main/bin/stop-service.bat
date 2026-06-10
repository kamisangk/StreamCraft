@echo off
setlocal

set "STREAMCRAFT_HOME=%~dp0.."
if "%STREAMCRAFT_PID_FILE%"=="" set "STREAMCRAFT_PID_FILE=%STREAMCRAFT_HOME%\streamcraft-service.pid"

if not exist "%STREAMCRAFT_PID_FILE%" (
  echo StreamCraft service is not running.
  exit /b 0
)

set /p STREAMCRAFT_PID=<"%STREAMCRAFT_PID_FILE%"
powershell -NoProfile -ExecutionPolicy Bypass -Command "if (Get-Process -Id %STREAMCRAFT_PID% -ErrorAction SilentlyContinue) { Stop-Process -Id %STREAMCRAFT_PID%; exit 0 } else { exit 3 }"
del "%STREAMCRAFT_PID_FILE%" >nul 2>nul
echo StreamCraft service stopped.
