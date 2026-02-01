#!/bin/bash
# Automated backup script for OSRS Market Data Collector
# Backs up TimescaleDB and Grafana configurations

set -e

# Load environment variables
if [ -f .env ]; then
    export $(cat .env | grep -v '^#' | xargs)
fi

# Configuration
BACKUP_DIR="${BACKUP_DIR:-./backups}"
POSTGRES_USER="${POSTGRES_USER:-osrs_trader}"
POSTGRES_DB="${POSTGRES_DB:-osrs_market}"
CONTAINER_NAME="${CONTAINER_NAME:-osrs-timescaledb}"
RETENTION_DAYS="${BACKUP_RETENTION_DAYS:-30}"

# Create backup directory with timestamp
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
BACKUP_PATH="$BACKUP_DIR/$TIMESTAMP"
mkdir -p "$BACKUP_PATH"

echo "=========================================="
echo "Starting OSRS Market Data Backup"
echo "Timestamp: $TIMESTAMP"
echo "=========================================="

# Function to check if container is running
check_container() {
    if ! docker ps | grep -q "$CONTAINER_NAME"; then
        echo "ERROR: TimescaleDB container ($CONTAINER_NAME) is not running!"
        exit 1
    fi
}

# Backup TimescaleDB
echo "[1/4] Creating TimescaleDB backup..."
check_container

# Custom backup with compression
docker exec "$CONTAINER_NAME" pg_dump \
    -U "$POSTGRES_USER" \
    -d "$POSTGRES_DB" \
    -Fc \
    -Z 9 \
    -f /tmp/osrs_backup.dump

# Copy from container
docker cp "$CONTAINER_NAME:/tmp/osrs_backup.dump" "$BACKUP_PATH/database_backup.dump"
docker exec "$CONTAINER_NAME" rm /tmp/osrs_backup.dump

echo "      Database backup: $BACKUP_PATH/database_backup.dump"

# Backup schema separately (useful for restores)
echo "[2/4] Creating schema backup..."
docker exec "$CONTAINER_NAME" pg_dump \
    -U "$POSTGRES_USER" \
    -d "$POSTGRES_DB" \
    --schema-only \
    -f /tmp/osrs_schema.sql

docker cp "$CONTAINER_NAME:/tmp/osrs_schema.sql" "$BACKUP_PATH/schema_backup.sql"
docker exec "$CONTAINER_NAME" rm /tmp/osrs_schema.sql

echo "      Schema backup: $BACKUP_PATH/schema_backup.sql"

# Backup Grafana data (if using bind mounts)
echo "[3/4] Backing up Grafana configuration..."
if [ -d "./grafana" ]; then
    cp -r ./grafana "$BACKUP_PATH/grafana_config"
    echo "      Grafana config: $BACKUP_PATH/grafana_config"
fi

# Backup .env file (sanitized)
echo "[4/4] Backing up configuration..."
if [ -f .env ]; then
    # Copy without actual passwords
    cat .env | sed 's/PASSWORD=.*/PASSWORD=***REDACTED***/' > "$BACKUP_PATH/env_config.txt"
    echo "      Config backup: $BACKUP_PATH/env_config.txt"
fi

# Create backup manifest
cat > "$BACKUP_PATH/backup_manifest.txt" <<EOF
OSRS Market Data Collector Backup
==================================
Timestamp: $TIMESTAMP
Database: $POSTGRES_DB
User: $POSTGRES_USER

Contents:
- database_backup.dump: Full compressed database dump
- schema_backup.sql: Schema-only backup
- grafana_config: Grafana dashboards and datasources
- env_config.txt: Environment configuration (sanitized)

Backup Commands:
# Full restore
pg_restore -d $POSTGRES_DB -U $POSTGRES_USER database_backup.dump

# Schema-only restore
psql -d $POSTGRES_DB -U $POSTGRES_USER -f schema_backup.sql
EOF

echo ""
echo "=========================================="
echo "Backup completed successfully!"
echo "Location: $BACKUP_PATH"
echo "Size: $(du -sh $BACKUP_PATH | cut -f1)"
echo "=========================================="

# Cleanup old backups
echo ""
echo "Cleaning up backups older than $RETENTION_DAYS days..."
find "$BACKUP_DIR" -type d -name "20*" -mtime +$RETENTION_DAYS -exec rm -rf {} + 2>/dev/null || true
echo "Cleanup complete."

# Create a symlink to latest backup
ln -sfn "$BACKUP_PATH" "$BACKUP_DIR/latest"

echo ""
echo "Backup complete. Latest backup: $BACKUP_DIR/latest"
