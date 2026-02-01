@echo off
REM Automated backup script for OSRS Market Data Collector (Windows)
REM Backs up TimescaleDB and Grafana configurations

setlocal EnableDelayedExpansion

REM Load configuration
if exist .env (
    for /f "tokens=1,2 delims==" %%a in (.env) do (
        if "%%a"=="POSTGRES_USER" set POSTGRES_USER=%%b
        if "%%a"=="POSTGRES_DB" set POSTGRES_DB=%%b
        if "%%a"=="BACKUP_DIR" set BACKUP_DIR=%%b
        if "%%a"=="BACKUP_RETENTION_DAYS" set RETENTION_DAYS=%%b
    )
)

REM Set defaults
if "!POSTGRES_USER!"=="" set POSTGRES_USER=osrs_trader
if "!POSTGRES_DB!"=="" set POSTGRES_DB=osrs_market
if "!BACKUP_DIR!"=="" set BACKUP_DIR=./backups
if "!RETENTION_DAYS!"=="" set RETENTION_DAYS=30

set CONTAINER_NAME=osrs-timescaledb

REM Create backup directory with timestamp
for /f "tokens=2-4 delims=/ " %%a in ('date /t') do (set mydate=%%c%%a%%b)
for /f "tokens=1-2 delims=/:" %%a in ('time /t') do (set mytime=%%a%%b)
set TIMESTAMP=%mydate%_%mytime%
set BACKUP_PATH=%BACKUP_DIR%\%TIMESTAMP%

if not exist "%BACKUP_PATH%" mkdir "%BACKUP_PATH%"

echo ==========================================
echo Starting OSRS Market Data Backup
echo Timestamp: %TIMESTAMP%
echo ==========================================

REM Check if Docker is running
docker ps >nul 2>&1
if errorlevel 1 (
    echo ERROR: Docker is not running!
    exit /b 1
)

REM Check if container is running
docker ps | findstr "%CONTAINER_NAME%" >nul
if errorlevel 1 (
    echo ERROR: TimescaleDB container (%CONTAINER_NAME%) is not running!
    exit /b 1
)

REM Backup TimescaleDB
echo [1/4] Creating TimescaleDB backup...
docker exec %CONTAINER_NAME% pg_dump -U %POSTGRES_USER% -d %POSTGRES_DB% -Fc -Z 9 -f /tmp/osrs_backup.dump
if errorlevel 1 (
    echo ERROR: Database backup failed!
    exit /b 1
)
docker cp %CONTAINER_NAME%:/tmp/osrs_backup.dump "%BACKUP_PATH%\database_backup.dump"
docker exec %CONTAINER_NAME% rm /tmp/osrs_backup.dump
echo       Database backup: %BACKUP_PATH%\database_backup.dump

REM Backup schema
echo [2/4] Creating schema backup...
docker exec %CONTAINER_NAME% pg_dump -U %POSTGRES_USER% -d %POSTGRES_DB% --schema-only -f /tmp/osrs_schema.sql
docker cp %CONTAINER_NAME%:/tmp/osrs_schema.sql "%BACKUP_PATH%\schema_backup.sql"
docker exec %CONTAINER_NAME% rm /tmp/osrs_schema.sql
echo       Schema backup: %BACKUP_PATH%\schema_backup.sql

REM Backup Grafana config
echo [3/4] Backing up Grafana configuration...
if exist .\grafana (
    xcopy /E /I /Y .\grafana "%BACKUP_PATH%\grafana_config" >nul
    echo       Grafana config: %BACKUP_PATH%\grafana_config
)

REM Backup .env (sanitized)
echo [4/4] Backing up configuration...
if exist .env (
    powershell -Command "(Get-Content .env) -replace 'PASSWORD=.*', 'PASSWORD=***REDACTED***' | Set-Content '%BACKUP_PATH%\env_config.txt'"
    echo       Config backup: %BACKUP_PATH%\env_config.txt
)

REM Create manifest
echo OSRS Market Data Collector Backup > "%BACKUP_PATH%\backup_manifest.txt"
echo ================================== >> "%BACKUP_PATH%\backup_manifest.txt"
echo Timestamp: %TIMESTAMP% >> "%BACKUP_PATH%\backup_manifest.txt"
echo Database: %POSTGRES_DB% >> "%BACKUP_PATH%\backup_manifest.txt"
echo User: %POSTGRES_USER% >> "%BACKUP_PATH%\backup_manifest.txt%"

echo.
echo ==========================================
echo Backup completed successfully!
echo Location: %BACKUP_PATH%
echo ==========================================

REM Cleanup old backups (older than retention days)
echo.
echo Cleaning up backups older than %RETENTION_DAYS% days...
forfiles /P "%BACKUP_DIR%" /M "20*" /D -%RETENTION_DAYS% /C "cmd /c rmdir /S /Q @path" 2>nul

echo.
echo Backup complete.

endlocal
