-- =============================================================================
-- OSRS Market Data Collector - Database Schema
-- =============================================================================
-- This script initializes the TimescaleDB database with:
--   - Main prices hypertable for time-series data
--   - Item metadata table
--   - State management tables for collectors
--   - Data gap tracking
--   - Optimized indexes for common query patterns
-- 
-- IMPORTANT: All timestamps are stored in UTC timezone
-- Storage Estimate: ~40-50GB for 6 months of 5-minute data for ~4,000 items
-- =============================================================================

-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- =============================================================================
-- TIMEZONE VERIFICATION
-- =============================================================================
-- Ensure database is running in UTC timezone
DO $$
BEGIN
    IF current_setting('timezone') != 'UTC' THEN
        RAISE WARNING 'Database timezone is %, expected UTC. Please configure timezone properly.', current_setting('timezone');
    END IF;
END $$;

-- =============================================================================
-- ITEM METADATA TABLE
-- =============================================================================
-- Stores static information about OSRS items
-- Updated infrequently (daily or on-demand)
-- Primary key: item_id (matches OSRS item IDs)
-- =============================================================================

CREATE TABLE IF NOT EXISTS items (
    item_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT,
    members BOOLEAN DEFAULT false,
    tradeable BOOLEAN DEFAULT true,
    tradeable_on_ge BOOLEAN DEFAULT true,
    stackable BOOLEAN DEFAULT false,
    noted BOOLEAN DEFAULT false,
    noteable BOOLEAN DEFAULT false,
    equipable BOOLEAN DEFAULT false,
    equipable_by_player BOOLEAN DEFAULT false,
    equipable_weapon BOOLEAN DEFAULT false,
    cost INTEGER,
    lowalch INTEGER,
    highalch INTEGER,
    weight NUMERIC(10, 3),
    buy_limit INTEGER,
    quest_item BOOLEAN DEFAULT false,
    release_date DATE,
    duplicate BOOLEAN DEFAULT false,
    placeholder BOOLEAN DEFAULT false,
    -- Wiki-specific fields
    wiki_name TEXT,
    wiki_url TEXT,
    -- Timestamps
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Indexes for item lookups
CREATE INDEX IF NOT EXISTS idx_items_name ON items(name);
CREATE INDEX IF NOT EXISTS idx_items_members ON items(members);
CREATE INDEX IF NOT EXISTS idx_items_tradeable ON items(tradeable);
CREATE INDEX IF NOT EXISTS idx_items_updated_at ON items(updated_at);

-- Comment explaining table purpose
COMMENT ON TABLE items IS 'Static metadata for OSRS items. Updated infrequently via metadata collector.';
COMMENT ON COLUMN items.item_id IS 'Unique OSRS item identifier from the game/Wiki API';
COMMENT ON COLUMN items.buy_limit IS 'Grand Exchange buy limit (maximum quantity per 4 hours)';

-- =============================================================================
-- MAIN PRICES TABLE (TimescaleDB Hypertable)
-- =============================================================================
-- Stores 5-minute interval price and volume data
-- Partitioned by time for efficient time-series queries
-- ~207 million rows expected for 6 months of 4,000 items
-- Estimated size: 25-30GB raw data, 40-50GB with indexes
-- =============================================================================

CREATE TABLE IF NOT EXISTS prices (
    -- Time-series partitioning key (must be first column for hypertable)
    timestamp TIMESTAMPTZ NOT NULL,
    
    -- Item reference
    item_id INTEGER NOT NULL,
    
    -- Price data from Wiki API /5m endpoint
    avg_high_price BIGINT,           -- Average price at high volume
    avg_low_price BIGINT,            -- Average price at low volume
    high_price_volume BIGINT,        -- Volume traded at high price
    low_price_volume BIGINT,         -- Volume traded at low price
    
    -- Calculated fields (computed on insert or via triggers)
    spread BIGINT GENERATED ALWAYS AS (
        CASE 
            WHEN avg_high_price IS NOT NULL AND avg_low_price IS NOT NULL 
            THEN avg_high_price - avg_low_price 
            ELSE NULL 
        END
    ) STORED,
    
    total_volume BIGINT GENERATED ALWAYS AS (
        COALESCE(high_price_volume, 0) + COALESCE(low_price_volume, 0)
    ) STORED,
    
    -- Data quality flags
    has_high_data BOOLEAN GENERATED ALWAYS AS (
        avg_high_price IS NOT NULL AND high_price_volume IS NOT NULL
    ) STORED,
    
    has_low_data BOOLEAN GENERATED ALWAYS AS (
        avg_low_price IS NOT NULL AND low_price_volume IS NOT NULL
    ) STORED,
    
    -- Insert metadata
    inserted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    -- Primary key for idempotent inserts (excludes generated columns)
    PRIMARY KEY (timestamp, item_id)
);

-- Convert to hypertable with daily chunks
-- Daily chunks provide good balance between query performance and chunk management
-- For 6 months of data: ~180 chunks, each ~115MB with ~1.15M rows
SELECT create_hypertable(
    'prices', 
    'timestamp', 
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

-- Critical indexes for time-series queries
-- 1. Item + time queries (most common: get price history for specific item)
CREATE INDEX IF NOT EXISTS idx_prices_item_time 
    ON prices (item_id, timestamp DESC);

-- 2. Time-only queries (get all items for specific time range)
CREATE INDEX IF NOT EXISTS idx_prices_timestamp 
    ON prices (timestamp DESC);

-- 3. Volume-based queries (identify high-activity items)
CREATE INDEX IF NOT EXISTS idx_prices_volume 
    ON prices (total_volume DESC, timestamp DESC) 
    WHERE total_volume > 0;

-- 4. Spread queries (for arbitrage analysis)
CREATE INDEX IF NOT EXISTS idx_prices_spread 
    ON prices (spread DESC, timestamp DESC) 
    WHERE spread IS NOT NULL;

-- Comments
COMMENT ON TABLE prices IS 'Time-series price data for all OSRS items. 5-minute intervals. TimescaleDB hypertable with daily chunks.';
COMMENT ON COLUMN prices.timestamp IS 'UTC timestamp of the 5-minute interval. All times in UTC.';
COMMENT ON COLUMN prices.avg_high_price IS 'Average price of items sold at the high price point (seller perspective)';
COMMENT ON COLUMN prices.avg_low_price IS 'Average price of items sold at the low price point (buyer perspective)';
COMMENT ON COLUMN prices.spread IS 'Price spread (high - low). Indicator of market liquidity.';
COMMENT ON COLUMN prices.total_volume IS 'Combined trading volume at both price points';

-- =============================================================================
-- LATEST PRICES TABLE
-- =============================================================================
-- Stores most recent price snapshot for quick access
-- Updated continuously by real-time collector
-- Only one row per item (updated in place)
-- =============================================================================

CREATE TABLE IF NOT EXISTS latest_prices (
    item_id INTEGER PRIMARY KEY REFERENCES items(item_id) ON DELETE CASCADE,
    timestamp TIMESTAMPTZ NOT NULL,
    
    -- Price data (volume not available from /latest API endpoint)
    avg_high_price BIGINT,
    avg_low_price BIGINT,
    
    -- Calculated spread
    spread BIGINT GENERATED ALWAYS AS (
        CASE 
            WHEN avg_high_price IS NOT NULL AND avg_low_price IS NOT NULL 
            THEN avg_high_price - avg_low_price 
            ELSE NULL 
        END
    ) STORED,
    
    -- Metadata
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Index for time-based queries
CREATE INDEX IF NOT EXISTS idx_latest_prices_timestamp 
    ON latest_prices (timestamp DESC);

COMMENT ON TABLE latest_prices IS 'Current/latest price snapshot for each item. Updated in real-time.';

-- =============================================================================
-- COLLECTOR STATE MANAGEMENT TABLE
-- =============================================================================
-- Tracks state for all data collectors
-- Enables safe restarts and crash recovery
-- One row per collector instance
-- =============================================================================

CREATE TABLE IF NOT EXISTS collector_state (
    collector_name TEXT PRIMARY KEY,
    
    -- Last processed timestamp (UTC)
    last_processed_timestamp TIMESTAMPTZ,
    
    -- Collector status
    status TEXT NOT NULL DEFAULT 'stopped' 
        CHECK (status IN ('running', 'stopped', 'error', 'paused')),
    
    -- Collector-specific metadata (flexible JSON for different collector types)
    metadata JSONB DEFAULT '{}',
    
    -- Timing information
    started_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    -- Error tracking
    last_error_message TEXT,
    last_error_at TIMESTAMPTZ,
    consecutive_errors INTEGER DEFAULT 0
);

-- Index for status monitoring
CREATE INDEX IF NOT EXISTS idx_collector_state_status 
    ON collector_state (status);

-- Insert default states for known collectors
INSERT INTO collector_state (collector_name, status, metadata) VALUES
    ('realtime_collector', 'stopped', '{"type": "realtime", "mode": "normal"}'),
    ('backfill_collector', 'stopped', '{"type": "backfill", "direction": "backward"}'),
    ('metadata_collector', 'stopped', '{"type": "metadata", "update_interval_hours": 24}')
ON CONFLICT (collector_name) DO NOTHING;

COMMENT ON TABLE collector_state IS 'Persistent state management for data collectors. Enables restart safety and progress tracking.';
COMMENT ON COLUMN collector_state.last_processed_timestamp IS 'Last successfully processed timestamp. Source of truth for resume after restart.';
COMMENT ON COLUMN collector_state.metadata IS 'JSON blob for collector-specific configuration and progress data';

-- =============================================================================
-- BACKFILL PROGRESS TRACKING TABLE
-- =============================================================================
-- Detailed tracking for historical backfill operations
-- Separate from collector_state for more granular progress monitoring
-- =============================================================================

CREATE TABLE IF NOT EXISTS backfill_progress (
    id SERIAL PRIMARY KEY,
    collector_name TEXT NOT NULL DEFAULT 'backfill_collector',
    
    -- Target range
    target_timestamp TIMESTAMPTZ NOT NULL,  -- How far back we're going (e.g., 6 months ago)
    "current_timestamp" TIMESTAMPTZ,         -- Where we are now (quoted due to reserved keyword)
    
    -- Progress tracking
    direction TEXT NOT NULL DEFAULT 'backward' CHECK (direction IN ('backward', 'forward')),
    status TEXT NOT NULL DEFAULT 'pending' 
        CHECK (status IN ('pending', 'running', 'completed', 'paused', 'error')),
    
    -- Progress metrics
    total_intervals_to_process INTEGER,
    intervals_processed INTEGER DEFAULT 0,
    progress_percentage NUMERIC(5, 2) GENERATED ALWAYS AS (
        CASE 
            WHEN total_intervals_to_process > 0 
            THEN ROUND((intervals_processed::NUMERIC / total_intervals_to_process) * 100, 2)
            ELSE 0
        END
    ) STORED,
    
    -- Timing
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    estimated_completion_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    -- Statistics
    items_processed INTEGER DEFAULT 0,
    api_calls_made INTEGER DEFAULT 0,
    errors_encountered INTEGER DEFAULT 0,
    
    -- Notes
    notes TEXT
);

-- Index for active backfill lookup
CREATE INDEX IF NOT EXISTS idx_backfill_progress_status 
    ON backfill_progress (status, collector_name) 
    WHERE status IN ('running', 'paused');

COMMENT ON TABLE backfill_progress IS 'Detailed progress tracking for historical data backfill operations.';
COMMENT ON COLUMN backfill_progress.target_timestamp IS 'The oldest timestamp we want to reach (e.g., 6 months ago from backfill start)';
COMMENT ON COLUMN backfill_progress."current_timestamp" IS 'Current position in the backfill process';

-- =============================================================================
-- DATA GAPS TRACKING TABLE
-- =============================================================================
-- Records detected gaps in 5-minute interval data
-- Used for manual or automated gap filling
-- =============================================================================

CREATE TABLE IF NOT EXISTS data_gaps (
    id SERIAL PRIMARY KEY,
    
    -- Gap identification
    item_id INTEGER,
    gap_start TIMESTAMPTZ NOT NULL,
    gap_end TIMESTAMPTZ NOT NULL,
    duration_minutes INTEGER GENERATED ALWAYS AS (
        EXTRACT(EPOCH FROM (gap_end - gap_start)) / 60
    ) STORED,
    
    -- Gap metadata
    expected_intervals INTEGER,  -- How many 5-minute intervals should exist
    missing_intervals INTEGER,   -- How many are actually missing
    
    -- Detection info
    detected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    detected_by TEXT,            -- Which process detected the gap
    
    -- Resolution tracking
    status TEXT NOT NULL DEFAULT 'open' 
        CHECK (status IN ('open', 'in_progress', 'filled', 'unfillable')),
    filled_at TIMESTAMPTZ,
    filled_by TEXT,
    fill_attempts INTEGER DEFAULT 0,
    last_error_message TEXT,
    
    -- Notes
    notes TEXT,
    
    -- Ensure gap_end is after gap_start
    CONSTRAINT valid_gap_range CHECK (gap_end > gap_start)
);

-- Indexes for gap queries
CREATE INDEX IF NOT EXISTS idx_data_gaps_status 
    ON data_gaps (status, detected_at);

CREATE INDEX IF NOT EXISTS idx_data_gaps_item 
    ON data_gaps (item_id, gap_start);

CREATE INDEX IF NOT EXISTS idx_data_gaps_time_range 
    ON data_gaps (gap_start, gap_end) 
    WHERE status = 'open';

COMMENT ON TABLE data_gaps IS 'Tracks missing 5-minute intervals in price data. Enables gap detection and recovery.';
COMMENT ON COLUMN data_gaps.status IS 'open=newly detected, in_progress=being filled, filled=resolved, unfillable=data not available in API';

-- =============================================================================
-- COLLECTION METRICS TABLE
-- =============================================================================
-- Tracks detailed metrics about data collection operations
-- Used for monitoring and alerting
-- =============================================================================

CREATE TABLE IF NOT EXISTS collection_metrics (
    id SERIAL,
    collector_name TEXT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    -- Operation metrics
    operation_type TEXT NOT NULL,  -- 'fetch', 'insert', 'error', etc.
    items_processed INTEGER,
    records_inserted INTEGER,
    records_updated INTEGER,
    records_skipped INTEGER,
    
    -- Performance metrics
    api_response_time_ms INTEGER,
    db_insert_time_ms INTEGER,
    total_operation_time_ms INTEGER,
    
    -- API metrics
    api_endpoint TEXT,
    api_status_code INTEGER,
    api_error_message TEXT,
    
    -- Data quality
    items_with_null_prices INTEGER,
    items_with_zero_volume INTEGER,
    timestamp_lag_minutes INTEGER,  -- How far behind real-time
    
    -- Metadata
    additional_data JSONB,
    
    -- Primary key includes timestamp for TimescaleDB hypertable compatibility
    PRIMARY KEY (timestamp, collector_name, operation_type)
);

-- Convert to hypertable for time-series metrics
SELECT create_hypertable(
    'collection_metrics', 
    'timestamp', 
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_collection_metrics_collector 
    ON collection_metrics (collector_name, timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_collection_metrics_operation 
    ON collection_metrics (operation_type, timestamp DESC);

COMMENT ON TABLE collection_metrics IS 'Time-series metrics for monitoring collector performance and health.';

-- =============================================================================
-- TIMESCALEDB CONFIGURATION
-- =============================================================================

-- Enable compression on prices table for storage efficiency
-- Compress chunks older than 7 days
ALTER TABLE prices SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'item_id',
    timescaledb.compress_orderby = 'timestamp DESC'
);

-- Add compression policy (compress after 7 days)
SELECT add_compression_policy('prices', INTERVAL '7 days', if_not_exists => TRUE);

-- Add retention policy (optional - remove comment to enable)
-- Uncomment the following line to automatically drop data older than 6 months
-- SELECT add_retention_policy('prices', INTERVAL '6 months', if_not_exists => TRUE);

-- =============================================================================
-- UTILITY FUNCTIONS
-- =============================================================================

-- Function to check database size
CREATE OR REPLACE FUNCTION get_database_size()
RETURNS TABLE (
    database_name TEXT,
    size_pretty TEXT,
    size_bytes BIGINT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        current_database()::TEXT,
        pg_size_pretty(pg_database_size(current_database())),
        pg_database_size(current_database());
END;
$$ LANGUAGE plpgsql;

-- Function to check prices table size by chunk
CREATE OR REPLACE FUNCTION get_prices_table_size()
RETURNS TABLE (
    chunk_name TEXT,
    chunk_size_pretty TEXT,
    chunk_size_bytes BIGINT,
    range_start TIMESTAMPTZ,
    range_end TIMESTAMPTZ
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        c.chunk_name::TEXT,
        pg_size_pretty(pg_total_relation_size(c.chunk_name))::TEXT,
        pg_total_relation_size(c.chunk_name),
        c.range_start,
        c.range_end
    FROM timescaledb_information.chunks c
    WHERE c.hypertable_name = 'prices'
    ORDER BY c.range_start DESC;
END;
$$ LANGUAGE plpgsql;

-- Function to detect data gaps for a specific time range
CREATE OR REPLACE FUNCTION detect_data_gaps(
    p_start_time TIMESTAMPTZ,
    p_end_time TIMESTAMPTZ,
    p_interval_minutes INTEGER DEFAULT 5
)
RETURNS TABLE (
    item_id INTEGER,
    gap_start TIMESTAMPTZ,
    gap_end TIMESTAMPTZ,
    missing_intervals BIGINT
) AS $$
BEGIN
    RETURN QUERY
    WITH time_series AS (
        SELECT generate_series(
            p_start_time,
            p_end_time,
            (p_interval_minutes || ' minutes')::INTERVAL
        ) AS expected_timestamp
    ),
    all_items AS (
        SELECT DISTINCT p.item_id 
        FROM prices p 
        WHERE p.timestamp BETWEEN p_start_time AND p_end_time
    ),
    expected_data AS (
        SELECT ai.item_id, ts.expected_timestamp
        FROM all_items ai
        CROSS JOIN time_series ts
    ),
    missing_data AS (
        SELECT 
            ed.item_id,
            ed.expected_timestamp,
            LEAD(ed.expected_timestamp) OVER (PARTITION BY ed.item_id ORDER BY ed.expected_timestamp) AS next_timestamp
        FROM expected_data ed
        LEFT JOIN prices p ON ed.item_id = p.item_id AND ed.expected_timestamp = p.timestamp
        WHERE p.item_id IS NULL
    )
    SELECT 
        md.item_id,
        md.expected_timestamp AS gap_start,
        COALESCE(md.next_timestamp, md.expected_timestamp + (p_interval_minutes || ' minutes')::INTERVAL) AS gap_end,
        COUNT(*) OVER (PARTITION BY md.item_id, md.expected_timestamp ORDER BY md.expected_timestamp) AS missing_intervals
    FROM missing_data md
    ORDER BY md.item_id, md.expected_timestamp;
END;
$$ LANGUAGE plpgsql;

-- Function to update collector state safely
CREATE OR REPLACE FUNCTION update_collector_state(
    p_collector_name TEXT,
    p_last_timestamp TIMESTAMPTZ,
    p_status TEXT,
    p_metadata JSONB DEFAULT NULL
)
RETURNS VOID AS $$
BEGIN
    INSERT INTO collector_state (
        collector_name, 
        last_processed_timestamp, 
        status, 
        metadata, 
        updated_at,
        started_at
    ) VALUES (
        p_collector_name, 
        p_last_timestamp, 
        p_status, 
        COALESCE(p_metadata, '{}'),
        NOW(),
        CASE WHEN p_status = 'running' THEN COALESCE(
            (SELECT started_at FROM collector_state WHERE collector_name = p_collector_name),
            NOW()
        ) ELSE NULL END
    )
    ON CONFLICT (collector_name) DO UPDATE SET
        last_processed_timestamp = EXCLUDED.last_processed_timestamp,
        status = EXCLUDED.status,
        metadata = CASE 
            WHEN EXCLUDED.metadata != '{}' THEN EXCLUDED.metadata 
            ELSE collector_state.metadata 
        END,
        updated_at = EXCLUDED.updated_at,
        started_at = COALESCE(collector_state.started_at, EXCLUDED.started_at),
        last_error_message = NULL,
        last_error_at = NULL,
        consecutive_errors = 0
    WHERE collector_state.status != 'error' OR EXCLUDED.status = 'running';
END;
$$ LANGUAGE plpgsql;

-- Function to record collector error
CREATE OR REPLACE FUNCTION record_collector_error(
    p_collector_name TEXT,
    p_error_message TEXT
)
RETURNS VOID AS $$
BEGIN
    UPDATE collector_state
    SET 
        last_error_message = p_error_message,
        last_error_at = NOW(),
        consecutive_errors = consecutive_errors + 1,
        status = CASE 
            WHEN consecutive_errors >= 5 THEN 'error'
            ELSE status 
        END,
        updated_at = NOW()
    WHERE collector_name = p_collector_name;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- INITIAL DATA
-- =============================================================================

-- Insert a marker record to verify database initialization
INSERT INTO collector_state (
    collector_name, 
    last_processed_timestamp, 
    status, 
    metadata
) VALUES (
    'database_init', 
    NOW(), 
    'stopped',
    jsonb_build_object(
        'init_version', '1.0',
        'init_timestamp', NOW(),
        'timezone', current_setting('timezone'),
        'init_completed', true
    )
)
ON CONFLICT (collector_name) DO UPDATE SET
    metadata = EXCLUDED.metadata,
    updated_at = NOW();

-- =============================================================================
-- GRANT PRIVILEGES
-- =============================================================================

-- Grant necessary permissions to the application user
-- Note: Run this as postgres superuser or adjust for your security model

-- GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO osrs_trader;
-- GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO osrs_trader;
-- GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO osrs_trader;

-- =============================================================================
-- END OF SCHEMA
-- =============================================================================
