-- =============================================================================
-- OSRS Market Data Collector - Useful SQL Queries
-- =============================================================================
-- This file contains reference queries for monitoring and analyzing the OSRS
-- market data. All queries use standard PostgreSQL/TimescaleDB syntax.
-- 
-- Designed to run every 5 minutes for continuous monitoring.
-- =============================================================================

-- =============================================================================
-- GAP DETECTION
-- =============================================================================

-- Query 1: Find Missing 5-Minute Intervals (Last Hour)
-- Shows all 5-minute timestamps that should have data but don't
-- Run every: 5 minutes
-- Expected output: List of missing timestamps

WITH expected_intervals AS (
    SELECT generate_series(
        date_trunc('hour', NOW() - INTERVAL '1 hour'),
        date_trunc('hour', NOW()) + INTERVAL '1 hour',
        INTERVAL '5 minutes'
    ) as expected_time
),
actual_intervals AS (
    SELECT DISTINCT timestamp as actual_time
    FROM prices
    WHERE timestamp > NOW() - INTERVAL '2 hours'
)
SELECT
    expected_time as missing_timestamp,
    'GAP' as status,
    EXTRACT(EPOCH FROM (expected_time - LAG(expected_time) OVER (ORDER BY expected_time))) / 60 as minutes_since_previous
FROM expected_intervals
LEFT JOIN actual_intervals ON expected_time = actual_time
WHERE actual_time IS NULL
  AND expected_time < NOW()
ORDER BY expected_time DESC
LIMIT 50;


-- Query 2: Count Expected vs Actual Intervals (Data Completeness)
-- Shows percentage of intervals that have data
-- Run every: 5 minutes
-- Expected output: 1 row with counts and percentage

WITH expected AS (
    SELECT COUNT(*) as total_expected
    FROM generate_series(
        NOW() - INTERVAL '1 hour',
        NOW(),
        INTERVAL '5 minutes'
    )
),
actual AS (
    SELECT COUNT(DISTINCT timestamp) as total_actual
    FROM prices
    WHERE timestamp > NOW() - INTERVAL '1 hour'
)
SELECT 
    e.total_expected,
    a.total_actual,
    ROUND((a.total_actual::NUMERIC / NULLIF(e.total_expected, 0) * 100), 2) as completeness_percentage,
    CASE 
        WHEN a.total_actual = e.total_expected THEN 'COMPLETE'
        WHEN a.total_actual >= e.total_expected * 0.95 THEN 'NEAR_COMPLETE'
        WHEN a.total_actual >= e.total_expected * 0.80 THEN 'PARTIAL'
        ELSE 'CRITICAL'
    END as status
FROM expected e, actual a;


-- Query 3: Find Consecutive Gaps (Outage Detection)
-- Identifies periods with multiple consecutive missing intervals
-- Run every: 5 minutes
-- Expected output: Start/end of gap periods

WITH gaps AS (
    SELECT 
        timestamp as gap_start,
        LEAD(timestamp) OVER (ORDER BY timestamp) as next_timestamp,
        timestamp - LAG(timestamp) OVER (ORDER BY timestamp) as gap_duration
    FROM prices
    WHERE timestamp > NOW() - INTERVAL '6 hours'
),
gap_periods AS (
    SELECT 
        gap_start,
        next_timestamp,
        EXTRACT(EPOCH FROM (next_timestamp - gap_start)) / 60 as gap_minutes
    FROM gaps
    WHERE next_timestamp - gap_start > INTERVAL '10 minutes'
)
SELECT 
    gap_start as outage_start,
    next_timestamp as outage_end,
    gap_minutes as duration_minutes,
    ROUND(gap_minutes / 5) as missing_intervals
FROM gap_periods
ORDER BY gap_start DESC
LIMIT 10;


-- Query 4: Data Presence Heatmap Data
-- Returns record counts per 5-minute bucket for visualization
-- Run every: 5 minutes
-- Expected output: Time buckets with record counts

SELECT
    time_bucket('5 minutes', timestamp) as time_interval,
    COUNT(*) as record_count,
    COUNT(DISTINCT item_id) as unique_items
FROM prices
WHERE timestamp > NOW() - INTERVAL '6 hours'
GROUP BY time_interval
ORDER BY time_interval DESC;


-- =============================================================================
-- DATA QUALITY
-- =============================================================================

-- Query 5: Records Per 5-Minute Interval
-- Shows how many records were inserted in each recent interval
-- Run every: 5 minutes
-- Expected output: Recent intervals with record counts

SELECT 
    time_bucket('5 minutes', timestamp) as interval_time,
    COUNT(*) as total_records,
    COUNT(DISTINCT item_id) as unique_items,
    SUM(CASE WHEN avg_high_price IS NOT NULL THEN 1 ELSE 0 END) as with_high_price,
    SUM(CASE WHEN avg_low_price IS NOT NULL THEN 1 ELSE 0 END) as with_low_price
FROM prices
WHERE timestamp > NOW() - INTERVAL '1 hour'
GROUP BY interval_time
ORDER BY interval_time DESC
LIMIT 12;


-- Query 6: Items With Missing Recent Data
-- Finds items that haven't had data in the last hour
-- Run every: 5 minutes
-- Expected output: List of item IDs with last seen timestamp

SELECT 
    item_id,
    MAX(timestamp) as last_seen,
    EXTRACT(EPOCH FROM (NOW() - MAX(timestamp))) / 60 as minutes_ago
FROM prices
WHERE timestamp > NOW() - INTERVAL '24 hours'
GROUP BY item_id
HAVING MAX(timestamp) < NOW() - INTERVAL '1 hour'
ORDER BY last_seen DESC
LIMIT 20;


-- Query 7: Volume Statistics Per Interval
-- Shows trading volume distribution
-- Run every: 5 minutes
-- Expected output: Volume stats per interval

SELECT 
    time_bucket('5 minutes', timestamp) as interval_time,
    SUM(high_price_volume) as total_high_volume,
    SUM(low_price_volume) as total_low_volume,
    SUM(COALESCE(high_price_volume, 0) + COALESCE(low_price_volume, 0)) as combined_volume,
    AVG(high_price_volume) as avg_high_volume,
    AVG(low_price_volume) as avg_low_volume
FROM prices
WHERE timestamp > NOW() - INTERVAL '1 hour'
GROUP BY interval_time
ORDER BY interval_time DESC
LIMIT 12;


-- =============================================================================
-- MONITORING
-- =============================================================================

-- Query 8: Latest Data Timestamp
-- Shows the most recent data in the database
-- Run every: 5 minutes
-- Expected output: 1 row with latest timestamp and lag

SELECT 
    MAX(timestamp) as latest_timestamp,
    NOW() as current_time,
    EXTRACT(EPOCH FROM (NOW() - MAX(timestamp))) / 60 as minutes_lag,
    CASE 
        WHEN EXTRACT(EPOCH FROM (NOW() - MAX(timestamp))) / 60 <= 5 THEN 'HEALTHY'
        WHEN EXTRACT(EPOCH FROM (NOW() - MAX(timestamp))) / 60 <= 15 THEN 'WARNING'
        ELSE 'CRITICAL'
    END as status
FROM prices;


-- Query 9: Total Records Count
-- Shows overall database statistics
-- Run every: 5 minutes
-- Expected output: 1 row with counts

SELECT 
    COUNT(*) as total_records,
    COUNT(DISTINCT item_id) as unique_items,
    MIN(timestamp) as oldest_record,
    MAX(timestamp) as newest_record,
    EXTRACT(EPOCH FROM (MAX(timestamp) - MIN(timestamp))) / 3600 as data_span_hours
FROM prices;


-- Query 10: Collector State Check
-- Shows current status of all collectors
-- Run every: 5 minutes
-- Expected output: Status of each collector

SELECT 
    collector_name,
    status,
    last_processed_timestamp,
    EXTRACT(EPOCH FROM (NOW() - last_processed_timestamp)) / 60 as minutes_since_last,
    consecutive_errors,
    metadata->>'mode' as mode
FROM collector_state
ORDER BY collector_name;


-- Query 11: Backfill Progress
-- Shows detailed backfill progress if running
-- Run every: 5 minutes
-- Expected output: Backfill statistics

SELECT 
    collector_name,
    target_timestamp,
    current_timestamp,
    intervals_processed,
    items_processed,
    api_calls_made,
    started_at,
    EXTRACT(EPOCH FROM (NOW() - started_at)) / 3600 as hours_running,
    CASE 
        WHEN current_timestamp IS NOT NULL AND target_timestamp IS NOT NULL 
        THEN ROUND((EXTRACT(EPOCH FROM (current_timestamp - target_timestamp)) / 
                   EXTRACT(EPOCH FROM (NOW() - target_timestamp)) * 100), 2)
        ELSE 0 
    END as estimated_progress_percentage
FROM backfill_progress
WHERE completed_at IS NULL
ORDER BY started_at DESC
LIMIT 1;


-- =============================================================================
-- MAINTENANCE
-- =============================================================================

-- Query 12: Database Size
-- Shows storage usage for the database
-- Run every: Hourly (not every 5 min - expensive query)
-- Expected output: Size statistics

SELECT 
    pg_size_pretty(pg_database_size('osrs_market')) as database_size,
    pg_size_pretty(pg_total_relation_size('prices')) as prices_table_size,
    pg_size_pretty(pg_total_relation_size('items')) as items_table_size,
    pg_size_pretty(pg_total_relation_size('latest_prices')) as latest_prices_size;


-- Query 13: Chunk Information
-- Shows TimescaleDB chunk details
-- Run every: Hourly
-- Expected output: Chunk statistics

SELECT 
    chunk_name,
    range_start,
    range_end,
    pg_size_pretty(total_bytes) as size,
    total_bytes
FROM chunks_detailed_size('prices')
ORDER BY range_start DESC
LIMIT 10;


-- Query 14: Recent Insert Rate
-- Shows how many records are being inserted per minute
-- Run every: 5 minutes
-- Expected output: Insert rate statistics

WITH recent_inserts AS (
    SELECT 
        time_bucket('1 minute', inserted_at) as minute,
        COUNT(*) as records
    FROM prices
    WHERE inserted_at > NOW() - INTERVAL '10 minutes'
    GROUP BY minute
)
SELECT 
    AVG(records) as avg_records_per_minute,
    MAX(records) as peak_records_per_minute,
    MIN(records) as min_records_per_minute,
    COUNT(*) as minutes_sampled
FROM recent_inserts;


-- =============================================================================
-- QUICK CHECKS (For Manual Use)
-- =============================================================================

-- Quick Check: Is data flowing?
-- Returns YES/NO if data exists in last 10 minutes
SELECT 
    CASE 
        WHEN EXISTS (
            SELECT 1 FROM prices 
            WHERE timestamp > NOW() - INTERVAL '10 minutes'
        ) THEN 'YES - Data flowing'
        ELSE 'NO - No recent data'
    END as data_status;


-- Quick Check: Count records in last 5 minutes
SELECT COUNT(*) as records_last_5min
FROM prices
WHERE timestamp > NOW() - INTERVAL '5 minutes';


-- Quick Check: Sample recent data
SELECT *
FROM prices
ORDER BY timestamp DESC
LIMIT 5;


-- =============================================================================
-- NOTES
-- =============================================================================
-- 
-- All queries are optimized for TimescaleDB and use:
-- - time_bucket() for efficient time-series aggregation
-- - Proper indexing on timestamp and item_id columns
-- - Relative time ranges (NOW() - INTERVAL) for continuous monitoring
--
-- Performance Tips:
-- - Queries on 'prices' table use the hypertable structure
-- - Time-based WHERE clauses leverage chunk exclusion
-- - DISTINCT operations are efficient due to proper indexing
--
-- To run these queries:
-- 1. Connect to database: psql -U osrs_trader -d osrs_market
-- 2. Copy and paste query
-- 3. Or use: psql -U osrs_trader -d osrs_market -f query_file.sql
--
-- For Grafana integration:
-- - Replace NOW() - INTERVAL with $__timeFilter() macros
-- - Use time_bucket() with Grafana's interval variables
-- - Add appropriate aliases for column names
-- =============================================================================
