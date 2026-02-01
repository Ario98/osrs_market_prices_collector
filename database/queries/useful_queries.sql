-- =============================================================================
-- Useful SQL Queries for OSRS Market Data
-- =============================================================================
-- Copy these queries into psql or Grafana to analyze your data
-- =============================================================================

-- =============================================================================
-- DATA INSPECTION QUERIES
-- =============================================================================

-- Check latest data timestamp
SELECT 
    MAX(timestamp) as latest_timestamp,
    COUNT(*) as total_records_last_hour
FROM prices 
WHERE timestamp > NOW() - INTERVAL '1 hour';

-- Count total records
SELECT COUNT(*) as total_records FROM prices;

-- Check date range of data
SELECT 
    MIN(timestamp) as oldest_timestamp,
    MAX(timestamp) as newest_timestamp,
    MAX(timestamp) - MIN(timestamp) as data_span
FROM prices;

-- =============================================================================
-- ITEM ANALYSIS QUERIES
-- =============================================================================

-- Most active items by volume (last 24 hours)
SELECT 
    i.name,
    p.item_id,
    SUM(p.total_volume) as total_volume,
    AVG(p.avg_high_price) as avg_high_price,
    AVG(p.avg_low_price) as avg_low_price,
    AVG(p.spread) as avg_spread
FROM prices p
JOIN items i ON p.item_id = i.item_id
WHERE p.timestamp > NOW() - INTERVAL '24 hours'
GROUP BY p.item_id, i.name
ORDER BY total_volume DESC
LIMIT 20;

-- Items with highest spread (potential arbitrage)
SELECT 
    i.name,
    p.item_id,
    AVG(p.spread) as avg_spread,
    AVG(p.avg_high_price) as avg_price,
    (AVG(p.spread) / NULLIF(AVG(p.avg_high_price), 0) * 100) as spread_percent
FROM prices p
JOIN items i ON p.item_id = i.item_id
WHERE p.timestamp > NOW() - INTERVAL '24 hours'
    AND p.spread IS NOT NULL
    AND p.avg_high_price > 1000
GROUP BY p.item_id, i.name
HAVING AVG(p.spread) > 0
ORDER BY spread_percent DESC
LIMIT 20;

-- Price trend for specific item (last 7 days)
SELECT 
    time_bucket('1 hour', timestamp) as hour,
    AVG(avg_high_price) as avg_price,
    SUM(total_volume) as volume
FROM prices
WHERE item_id = 4151  -- Abyssal whip
    AND timestamp > NOW() - INTERVAL '7 days'
GROUP BY hour
ORDER BY hour;

-- =============================================================================
-- DATA QUALITY QUERIES
-- =============================================================================

-- Detect gaps in 5-minute intervals (last 24 hours)
WITH time_series AS (
    SELECT generate_series(
        date_trunc('hour', NOW() - INTERVAL '24 hours'),
        date_trunc('hour', NOW()),
        INTERVAL '5 minutes'
    ) as expected_time
)
SELECT 
    ts.expected_time,
    COUNT(p.timestamp) as records_found
FROM time_series ts
LEFT JOIN prices p ON ts.expected_time = p.timestamp
GROUP BY ts.expected_time
HAVING COUNT(p.timestamp) = 0
ORDER BY ts.expected_time;

-- Items with missing data (no records in last hour)
SELECT 
    i.item_id,
    i.name,
    i.tradeable
FROM items i
LEFT JOIN prices p ON i.item_id = p.item_id 
    AND p.timestamp > NOW() - INTERVAL '1 hour'
WHERE p.item_id IS NULL
    AND i.tradeable = true
ORDER BY i.item_id;

-- Data completeness by item
SELECT 
    i.name,
    p.item_id,
    COUNT(*) as record_count,
    MIN(timestamp) as first_seen,
    MAX(timestamp) as last_seen
FROM prices p
JOIN items i ON p.item_id = i.item_id
WHERE p.timestamp > NOW() - INTERVAL '7 days'
GROUP BY p.item_id, i.name
ORDER BY record_count DESC
LIMIT 30;

-- =============================================================================
-- COLLECTOR MONITORING QUERIES
-- =============================================================================

-- Current collector status
SELECT 
    collector_name,
    status,
    last_processed_timestamp,
    consecutive_errors,
    updated_at,
    NOW() - updated_at as time_since_update
FROM collector_state
ORDER BY collector_name;

-- Backfill progress
SELECT 
    collector_name,
    current_timestamp,
    target_timestamp,
    status,
    ROUND(progress_percentage, 2) as pct_complete,
    intervals_processed,
    api_calls_made,
    ROUND((intervals_processed::NUMERIC / NULLIF(EXTRACT(EPOCH FROM (current_timestamp - target_timestamp))/300, 0) * 100), 2) as calculated_progress,
    updated_at
FROM backfill_progress
WHERE status IN ('running', 'paused')
ORDER BY started_at DESC
LIMIT 1;

-- Recent data gaps
SELECT 
    item_id,
    gap_start,
    gap_end,
    duration_minutes,
    status,
    detected_at,
    fill_attempts
FROM data_gaps
WHERE detected_at > NOW() - INTERVAL '7 days'
ORDER BY detected_at DESC
LIMIT 50;

-- =============================================================================
-- STORAGE & PERFORMANCE QUERIES
-- =============================================================================

-- Database size
SELECT 
    pg_size_pretty(pg_database_size('osrs_market')) as total_size,
    pg_database_size('osrs_market') as bytes;

-- Table sizes
SELECT 
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as total_size,
    pg_size_pretty(pg_relation_size(schemaname||'.'||tablename)) as data_size,
    pg_size_pretty(pg_indexes_size(schemaname||'.'||tablename)) as index_size
FROM pg_tables 
WHERE schemaname = 'public'
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;

-- Hypertable chunks
SELECT 
    chunk_name,
    range_start,
    range_end,
    pg_size_pretty(pg_total_relation_size(chunk_name)) as size,
    (SELECT COUNT(*) FROM pg_class WHERE relname = chunk_name) as exists
FROM timescaledb_information.chunks
WHERE hypertable_name = 'prices'
ORDER BY range_start DESC
LIMIT 10;

-- Compression stats
SELECT 
    chunk_name,
    compression_status,
    before_compression_total_bytes,
    after_compression_total_bytes,
    ROUND(
        (1 - (after_compression_total_bytes::NUMERIC / NULLIF(before_compression_total_bytes, 0))) * 100, 
        2
    ) as compression_ratio_pct
FROM timescaledb_information.compression_settings
WHERE hypertable_name = 'prices'
ORDER BY chunk_name DESC
LIMIT 10;

-- =============================================================================
-- MARKET ANALYSIS QUERIES
-- =============================================================================

-- Price volatility (standard deviation of prices)
SELECT 
    i.name,
    p.item_id,
    STDDEV(p.avg_high_price) as price_stddev,
    AVG(p.avg_high_price) as avg_price,
    (STDDEV(p.avg_high_price) / NULLIF(AVG(p.avg_high_price), 0) * 100) as volatility_pct
FROM prices p
JOIN items i ON p.item_id = i.item_id
WHERE p.timestamp > NOW() - INTERVAL '24 hours'
    AND p.avg_high_price IS NOT NULL
GROUP BY p.item_id, i.name
HAVING COUNT(*) >= 10
ORDER BY volatility_pct DESC
LIMIT 20;

-- Volume trend (compare last hour to previous hour)
WITH hourly_volume AS (
    SELECT 
        item_id,
        time_bucket('1 hour', timestamp) as hour,
        SUM(total_volume) as volume
    FROM prices
    WHERE timestamp > NOW() - INTERVAL '2 hours'
    GROUP BY item_id, hour
)
SELECT 
    i.name,
    hv1.item_id,
    hv1.volume as last_hour_volume,
    hv2.volume as previous_hour_volume,
    hv1.volume - hv2.volume as volume_change,
    ROUND(((hv1.volume - hv2.volume)::NUMERIC / NULLIF(hv2.volume, 0) * 100), 2) as volume_change_pct
FROM hourly_volume hv1
JOIN hourly_volume hv2 ON hv1.item_id = hv2.item_id AND hv1.hour > hv2.hour
JOIN items i ON hv1.item_id = i.item_id
ORDER BY ABS(volume_change) DESC
LIMIT 20;

-- Best times to trade (highest volume by hour of day)
SELECT 
    EXTRACT(hour FROM timestamp) as hour_of_day,
    AVG(total_volume) as avg_volume,
    COUNT(*) as data_points
FROM prices
WHERE timestamp > NOW() - INTERVAL '7 days'
GROUP BY hour_of_day
ORDER BY avg_volume DESC;

-- =============================================================================
-- MAINTENANCE QUERIES
-- =============================================================================

-- Find and fill specific gaps (manual gap filling)
-- First, identify the gap:
-- SELECT * FROM data_gaps WHERE status = 'open' LIMIT 5;

-- After filling, mark as resolved:
-- UPDATE data_gaps 
-- SET status = 'filled', filled_at = NOW(), filled_by = 'manual'
-- WHERE id = <gap_id>;

-- Vacuum analyze for performance
-- VACUUM ANALYZE prices;
-- VACUUM ANALYZE items;

-- Reindex for performance (run during low activity)
-- REINDEX INDEX CONCURRENTLY idx_prices_item_time;

-- =============================================================================
