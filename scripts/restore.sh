#!/bin/bash
# Restore script for OSRS Market Data Collector
# Restores database from backup files

set -e

# Load environment variables
if [ -f .env ]; then
    export $(cat .env | grep -v '^#' | xargs)
fi

# Configuration
POSTGRES_USER="${POSTGRES_USER:-osrs_trader}"
POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-change_me_in_env_file}"
POSTGRES_DB="${POSTGRES_DB:-osrs_market}"
CONTAINER_NAME="${CONTAINER_NAME:-osrs-timescaledb}"

# Usage
usage() {
    echo "Usage: $0 <backup_path> [--full | --schema-only]"
    echo ""
    echo "Arguments:"
    echo "  backup_path    Path to backup directory or .dump file"
    echo "  --full         Full database restore (default)"
    echo "  --schema-only  Restore only schema (no data)"
    echo ""
    echo "Examples:"
    echo "  $0 ./backups/20250101_120000"
    echo "  $0 ./backups/latest/database_backup.dump --full"
    echo "  $0 ./backups/20250101_120000/schema_backup.sql --schema-only"
    exit 1
}

# Check arguments
if [ $# -lt 1 ]; then
    usage
fi

BACKUP_PATH="$1"
RESTORE_MODE="${2:---full}"

# Validate backup path
if [ ! -e "$BACKUP_PATH" ]; then
    echo "ERROR: Backup path does not exist: $BACKUP_PATH"
    exit 1
fi

# Determine backup type and file
if [ -d "$BACKUP_PATH" ]; then
    # Directory provided - find backup file
    if [ -f "$BACKUP_PATH/database_backup.dump" ]; then
        BACKUP_FILE="$BACKUP_PATH/database_backup.dump"
        echo "Found full backup: $BACKUP_FILE"
    elif [ -f "$BACKUP_PATH/schema_backup.sql" ]; then
        BACKUP_FILE="$BACKUP_PATH/schema_backup.sql"
        echo "Found schema backup: $BACKUP_FILE"
        RESTORE_MODE="--schema-only"
    else
        echo "ERROR: No backup files found in $BACKUP_PATH"
        exit 1
    fi
else
    # File provided directly
    BACKUP_FILE="$BACKUP_PATH"
fi

# Validate container is running
if ! docker ps | grep -q "$CONTAINER_NAME"; then
    echo "ERROR: TimescaleDB container ($CONTAINER_NAME) is not running!"
    echo "Start services with: docker-compose up -d"
    exit 1
fi

echo ""
echo "=========================================="
echo "Database Restore"
echo "Backup: $BACKUP_FILE"
echo "Mode: $RESTORE_MODE"
echo "=========================================="
echo ""

# Warning
if [ "$RESTORE_MODE" == "--full" ]; then
    echo "WARNING: This will REPLACE all existing data in the database!"
    echo "Current database: $POSTGRES_DB"
    read -p "Are you sure you want to continue? (yes/no): " confirm
    if [ "$confirm" != "yes" ]; then
        echo "Restore cancelled."
        exit 0
    fi
fi

# Copy backup to container
echo "[1/3] Copying backup to container..."
BACKUP_FILENAME=$(basename "$BACKUP_FILE")
docker cp "$BACKUP_FILE" "$CONTAINER_NAME:/tmp/$BACKUP_FILENAME"

# Perform restore
echo "[2/3] Restoring database..."
if [[ "$BACKUP_FILE" == *.dump ]]; then
    # pg_restore for .dump files
    echo "      Using pg_restore (compressed backup)..."
    docker exec -e PGPASSWORD="$POSTGRES_PASSWORD" "$CONTAINER_NAME" pg_restore \
        -U "$POSTGRES_USER" \
        -d "$POSTGRES_DB" \
        --clean \
        --if-exists \
        -Z 9 \
        "/tmp/$BACKUP_FILENAME" 2>&1 | head -20
elif [[ "$BACKUP_FILE" == *.sql ]]; then
    # psql for .sql files
    echo "      Using psql (SQL backup)..."
    docker exec -e PGPASSWORD="$POSTGRES_PASSWORD" "$CONTAINER_NAME" psql \
        -U "$POSTGRES_USER" \
        -d "$POSTGRES_DB" \
        -f "/tmp/$BACKUP_FILENAME" 2>&1 | tail -20
else
    echo "ERROR: Unknown backup file format"
    exit 1
fi

# Cleanup
echo "[3/3] Cleaning up..."
docker exec "$CONTAINER_NAME" rm -f "/tmp/$BACKUP_FILENAME"

# Verify restore
echo ""
echo "Verifying restore..."
TABLE_COUNT=$(docker exec -e PGPASSWORD="$POSTGRES_PASSWORD" "$CONTAINER_NAME" psql \
    -U "$POSTGRES_USER" \
    -d "$POSTGRES_DB" \
    -t \
    -c "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='public';")

echo "Tables restored: $TABLE_COUNT"

# Check recent data if osrs_price_history exists
if docker exec -e PGPASSWORD="$POSTGRES_PASSWORD" "$CONTAINER_NAME" psql \
    -U "$POSTGRES_USER" \
    -d "$POSTGRES_DB" \
    -t \
    -c "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name='osrs_price_history');" | grep -q t; then
    
    RECORD_COUNT=$(docker exec -e PGPASSWORD="$POSTGRES_PASSWORD" "$CONTAINER_NAME" psql \
        -U "$POSTGRES_USER" \
        -d "$POSTGRES_DB" \
        -t \
        -c "SELECT COUNT(*) FROM osrs_price_history;")
    
    echo "Price records restored: $RECORD_COUNT"
fi

echo ""
echo "=========================================="
echo "Restore completed successfully!"
echo "=========================================="
