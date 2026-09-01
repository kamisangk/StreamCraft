@echo off
setlocal EnableExtensions

set "STREAMCRAFT_HOME=%~dp0.."
if "%STREAMCRAFT_CONF_DIR%"=="" set "STREAMCRAFT_CONF_DIR=%STREAMCRAFT_HOME%\conf"
if "%STREAMCRAFT_LOG_DIR%"=="" set "STREAMCRAFT_LOG_DIR=%STREAMCRAFT_HOME%\logs"
if "%STREAMCRAFT_DATA_DIR%"=="" set "STREAMCRAFT_DATA_DIR=%STREAMCRAFT_HOME%\data"
if "%STREAMCRAFT_PID_FILE%"=="" set "STREAMCRAFT_PID_FILE=%STREAMCRAFT_HOME%\streamcraft-service.pid"
if "%STREAMCRAFT_SERVICE_MAIN_CLASS%"=="" set "STREAMCRAFT_SERVICE_MAIN_CLASS=com.streamcraft.service.StreamCraftServiceApplication"
if "%STREAMCRAFT_CORE_JAR_PATH%"=="" set "STREAMCRAFT_CORE_JAR_PATH=%STREAMCRAFT_HOME%\flink-libs\streamcraft-core.jar"
set "STREAMCRAFT_SERVICE_CLASSPATH=%STREAMCRAFT_CONF_DIR%;%STREAMCRAFT_HOME%\libs\*"

if not exist "%STREAMCRAFT_LOG_DIR%" mkdir "%STREAMCRAFT_LOG_DIR%"
if not exist "%STREAMCRAFT_DATA_DIR%" mkdir "%STREAMCRAFT_DATA_DIR%"

set "JAVA_BIN=java"
if defined JAVA_HOME if exist "%JAVA_HOME%\bin\java.exe" set "JAVA_BIN=%JAVA_HOME%\bin\java.exe"

if "%JAVA_BIN%"=="java" (
  where java >nul 2>nul
  if errorlevel 1 (
    echo Error: java executable not found. Set JAVA_HOME or add java to PATH.
    exit /b 1
  )
)

rem SPRING_PROFILES_ACTIVE is passed via the environment; Spring picks it up natively.
set "APP_ARGS=--spring.config.additional-location=%STREAMCRAFT_CONF_DIR%\"

powershell -NoProfile -ExecutionPolicy Bypass -Command "$argumentList = @(); if (-not [string]::IsNullOrWhiteSpace($env:JAVA_OPTS)) { $argumentList += $env:JAVA_OPTS.Split(' ', [System.StringSplitOptions]::RemoveEmptyEntries) }; $argumentList += @('-cp', $env:STREAMCRAFT_SERVICE_CLASSPATH, $env:STREAMCRAFT_SERVICE_MAIN_CLASS, $env:APP_ARGS); $p = Start-Process -FilePath $env:JAVA_BIN -ArgumentList $argumentList -RedirectStandardOutput ($env:STREAMCRAFT_LOG_DIR + '\streamcraft-service.out') -RedirectStandardError ($env:STREAMCRAFT_LOG_DIR + '\streamcraft-service.err') -WindowStyle Hidden -PassThru; Set-Content -Path $env:STREAMCRAFT_PID_FILE -Value $p.Id"
if errorlevel 1 (
  echo Error: failed to start StreamCraft service.
  exit /b 1
)

set "STREAMCRAFT_PID="
set /p STREAMCRAFT_PID=<"%STREAMCRAFT_PID_FILE%"
echo StreamCraft service started, pid=%STREAMCRAFT_PID%.
