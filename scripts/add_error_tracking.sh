#!/bin/bash
# Improved error tracking schema migration
# Run this after the database is initialized to add better error tracking

set -e

# Load environment variables
if [ -f .env ]; then
    export $(cat .env | grep -v '^#' | xargs)
fi

POSTGRES_USER="${POSTGRES_USER:-osrs_trader}"
POSTGRES_DB="${POSTGRES_DB:-osrs_market}"
CONTAINER_NAME="${CONTAINER_NAME:-osrs-timescaledb}"

echo "=========================================="
echo "Adding Enhanced Error Tracking"
echo "=========================================="

# Check if container is running
if ! docker ps | grep -q "$CONTAINER_NAME"; then
    echo "ERROR: TimescaleDB container is not running!"
    exit 1
fi

# SQL to add enhanced error tracking
docker exec -i "$CONTAINER_NAME" psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" <<'EOF'

-- Add additional columns to collection_errors if they don't exist
DO $$
BEGIN
    -- Add error_category column
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_name = 'collection_errors' AND column_name = 'error_category') THEN
        ALTER TABLE collection_errors ADD COLUMN error_category VARCHAR(50);
        CREATE INDEX idx_collection_errors_category ON collection_errors(error_category);
    END IF;
    
    -- Add stack_trace column for detailed debugging
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_name = 'collection_errors' AND column_name = 'stack_trace') THEN
        ALTER TABLE collection_errors ADD COLUMN stack_trace TEXT;
    END IF;
    
    -- Add api_endpoint column to track which endpoint failed
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_name = 'collection_errors' AND column_name = 'api_endpoint') THEN
        ALTER TABLE collection_errors ADD COLUMN api_endpoint VARCHAR(255);
    END IF;
    
    -- Add http_status_code for API errors
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_name = 'collection_errors' AND column_name = 'http_status_code') THEN
        ALTER TABLE collection_errors ADD COLUMN http_status_code INTEGER;
    END IF;
    
    -- Add retry_count to track retry attempts
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_name = 'collection_errors' AND column_name = 'retry_count') THEN
        ALTER TABLE collection_errors ADD COLUMN retry_count INTEGER DEFAULT 0;
    END IF;
    
    -- Add resolved_timestamp to track when errors were resolved
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_name = 'collection_errors' AND column_name = 'resolved_timestamp') THEN
        ALTER TABLE collection_errors ADD COLUMN resolved_timestamp TIMESTAMPTZ;
        CREATE INDEX idx_collection_errors_resolved ON collection_errors(resolved_timestamp) WHERE resolved_timestamp IS NULL;
    END IF;
END $$;

-- Create a view for error analysis
CREATE OR REPLACE VIEW error_summary AS
SELECT 
    collector_name,
    error_category,
    DATE_TRUNC('hour', error_timestamp) as error_hour,
    COUNT(*) as error_count,
    MIN(error_timestamp) as first_occurrence,
    MAX(error_timestamp) as last_occurrence,
    array_agg(DISTINCT LEFT(error_message, 100)) as sample_messages
FROM collection_errors
WHERE resolved_timestamp IS NULL
GROUP BY collector_name, error_category, DATE_TRUNC('hour', error_timestamp)
ORDER BY error_hour DESC, error_count DESC;

-- Create function to categorize errors automatically
CREATE OR REPLACE FUNCTION categorize_error(error_msg TEXT)
RETURNS VARCHAR(50) AS $$
BEGIN
    IF error_msg ILIKE '%connection%' OR error_msg ILIKE '%timeout%' OR error_msg ILIKE '%network%' THEN
        RETURN 'NETWORK';
    ELSIF error_msg ILIKE '%rate limit%' OR error_msg ILIKE '%429%' OR error_msg ILIKE '%too many requests%' THEN
        RETURN 'RATE_LIMIT';
    ELSIF error_msg ILIKE '%database%' OR error_msg ILIKE '%sql%' OR error_msg ILIKE '%postgres%' THEN
        RETURN 'DATABASE';
    ELSIF error_msg ILIKE '%api%' OR error_msg ILIKE '%http%' OR error_msg ILIKE '%json%' THEN
        RETURN 'API';
    ELSIF error_msg ILIKE '%memory%' OR error_msg ILIKE '%disk%' OR error_msg ILIKE '%resource%' THEN
        RETURN 'RESOURCE';
    ELSE
        RETURN 'UNKNOWN';
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Create trigger to auto-categorize errors
CREATE OR REPLACE FUNCTION auto_categorize_error()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.error_category IS NULL THEN
        NEW.error_category := categorize_error(NEW.error_message);
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_auto_categorize_error ON collection_errors;
CREATE TRIGGER trigger_auto_categorize_error
    BEFORE INSERT ON collection_errors
    FOR EACH ROW
    EXECUTE FUNCTION auto_categorize_error();

-- Mark old errors as resolved (errors older than 24 hours without recurrence)
UPDATE collection_errors
SET resolved_timestamp = NOW()
WHERE resolved_timestamp IS NULL
  AND error_timestamp < NOW() - INTERVAL '24 hours'
  AND retry_count >= 3;

echo ""
echo "Enhanced error tracking added successfully!"
echo ""
echo "New features:"
echo "  - Error categorization (NETWORK, RATE_LIMIT, DATABASE, API, RESOURCE)"
echo "  - Stack trace storage"
echo "  - API endpoint tracking"
echo "  - HTTP status code tracking"
echo "  - Retry count tracking"
echo "  - Error resolution tracking"
echo "  - error_summary view for analysis"
echo ""
EOF
