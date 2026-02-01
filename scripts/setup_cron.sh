#!/bin/bash
# Setup automated backup cron job

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
BACKUP_SCRIPT="$SCRIPT_DIR/backup.sh"

# Default: Daily at 2 AM
CRON_SCHEDULE="${1:-0 2 * * *}"

echo "=========================================="
echo "OSRS Market Data Backup - Cron Setup"
echo "=========================================="
echo ""

# Check if cron is installed
if ! command -v crontab &> /dev/null; then
    echo "ERROR: crontab is not installed"
    echo "Install with: sudo apt-get install cron (Debian/Ubuntu)"
    echo "           or: sudo yum install cronie (RHEL/CentOS)"
    exit 1
fi

# Check if backup script exists
if [ ! -f "$BACKUP_SCRIPT" ]; then
    echo "ERROR: Backup script not found at: $BACKUP_SCRIPT"
    exit 1
fi

# Make backup script executable
chmod +x "$BACKUP_SCRIPT"

# Create cron job entry
CRON_JOB="$CRON_SCHEDULE cd $PROJECT_DIR && $BACKUP_SCRIPT >> $PROJECT_DIR/logs/backup_cron.log 2>&1"

echo "Cron schedule: $CRON_SCHEDULE"
echo "Project directory: $PROJECT_DIR"
echo "Backup script: $BACKUP_SCRIPT"
echo ""

# Check if job already exists
if crontab -l 2>/dev/null | grep -q "$BACKUP_SCRIPT"; then
    echo "Cron job already exists. Updating..."
    # Remove old job
    crontab -l 2>/dev/null | grep -v "$BACKUP_SCRIPT" | crontab -
fi

# Add new cron job
(crontab -l 2>/dev/null; echo "$CRON_JOB") | crontab -

echo "Cron job added successfully!"
echo ""
echo "To verify: crontab -l"
echo "To remove: crontab -l | grep -v 'backup.sh' | crontab -"
echo ""
echo "Backup will run: $CRON_SCHEDULE"
echo "Logs will be saved to: $PROJECT_DIR/logs/backup_cron.log"
echo ""
echo "Current crontab:"
crontab -l | grep "$BACKUP_SCRIPT"
