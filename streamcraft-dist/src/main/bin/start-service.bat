@echo off
setlocal

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
if not "%JAVA_HOME%"=="" set "JAVA_BIN=%JAVA_HOME%\bin\java"

set "APP_ARGS=--spring.config.additional-location=%STREAMCRAFT_CONF_DIR%\"
if not "%SPRING_PROFILES_ACTIVE%"=="" set "APP_ARGS=%APP_ARGS% --spring.profiles.active=%SPRING_PROFILES_ACTIVE%"

powershell -NoProfile -ExecutionPolicy Bypass -Command "$p = Start-Process -FilePath '%JAVA_BIN%' -ArgumentList @('%JAVA_OPTS%','-cp','%STREAMCRAFT_SERVICE_CLASSPATH%','%STREAMCRAFT_SERVICE_MAIN_CLASS%','%APP_ARGS%') -RedirectStandardOutput '%STREAMCRAFT_LOG_DIR%\streamcraft-service.out' -RedirectStandardError '%STREAMCRAFT_LOG_DIR%\streamcraft-service.err' -WindowStyle Hidden -PassThru; Set-Content -Path '%STREAMCRAFT_PID_FILE%' -Value $p.Id"
echo StreamCraft service started.
