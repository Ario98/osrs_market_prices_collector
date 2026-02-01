-- Data Retention and Compression Policies for OSRS Market Data
-- These policies help manage database size and performance

-- ==========================================
-- 1. CONTINUOUS AGGREGATION (5-min to hourly)
-- ==========================================
-- Create hourly aggregates for fast queries on long time ranges

-- Drop if exists for idempotency
DROP MATERIALIZED VIEW IF EXISTS osrs_price_hourly CASCADE;

-- Create hourly aggregated data
CREATE MATERIALIZED VIEW osrs_price_hourly
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour'::interval, "timestamp") AS bucket,
    item_id,
    avg_price,
    high_price,
    low_price,
    avg(avg_price) as avg_price_hourly,
    max(high_price) as high_price_hourly,
    min(low_price) as low_price_hourly,
    sum(buy_volume) as buy_volume_hourly,
    sum(sell_volume) as sell_volume_hourly,
    sum(total_volume) as total_volume_hourly,
    count(*) as sample_count
FROM osrs_price_history
GROUP BY bucket, item_id, avg_price, high_price, low_price;

-- Add retention policy for raw 5-min data (keep 6 months)
SELECT add_retention_policy('osrs_price_history', INTERVAL '6 months');

-- ==========================================
-- 2. COMPRESSION POLICY
-- ==========================================
-- Compress data older than 7 days to save space

-- Enable compression on price history table
ALTER TABLE osrs_price_history SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'item_id'
);

-- Add compression policy (compress chunks older than 7 days)
SELECT add_compression_policy('osrs_price_history', INTERVAL '7 days');

-- ==========================================
-- 3. RETENTION POLICIES FOR OTHER TABLES
-- ==========================================

-- Clean old collection metrics (keep 90 days)
SELECT add_retention_policy('collection_metrics', INTERVAL '90 days');

-- Clean old error logs (keep 30 days)
SELECT add_retention_policy('collection_errors', INTERVAL '30 days');

-- Clean old API logs (keep 30 days)
SELECT add_retention_policy('api_request_log', INTERVAL '30 days');

-- Clean old backfill progress (keep 180 days)
SELECT add_retention_policy('backfill_progress', INTERVAL '180 days');

-- ==========================================
-- 4. INDEX OPTIMIZATION
-- ==========================================

-- Ensure indexes exist for common query patterns
CREATE INDEX IF NOT EXISTS idx_price_history_item_time 
    ON osrs_price_history (item_id, "timestamp" DESC);

CREATE INDEX IF NOT EXISTS idx_price_history_timestamp 
    ON osrs_price_history ("timestamp" DESC);

-- Index for hourly view
CREATE INDEX IF NOT EXISTS idx_price_hourly_item_bucket 
    ON osrs_price_hourly (item_id, bucket DESC);

-- ==========================================
-- 5. VACUUM AND ANALYZE
-- ==========================================

-- Update statistics for query planner
ANALYZE osrs_price_history;
ANALYZE osrs_items;
ANALYZE collection_metrics;

-- ==========================================
-- 6. MONITORING QUERIES
-- ==========================================

-- View chunk information
SELECT 
    chunk_name,
    range_start,
    range_end,
    is_compressed,
    pg_size_pretty(total_bytes) as size
FROM chunks_detailed_size('osrs_price_history')
ORDER BY range_start DESC
LIMIT 10;

-- View retention and compression policies
SELECT * FROM timescaledb_information.jobs WHERE application_name LIKE '%retention%' OR application_name LIKE '%compression%';

-- Check table sizes
SELECT 
    hypertable_name,
    pg_size_pretty(total_bytes) as total_size,
    pg_size_pretty(compressed_total_bytes) as compressed_size,
    pg_size_pretty(uncompressed_total_bytes) as uncompressed_size,
    compression_ratio
FROM hypertable_compression_stats('public');
