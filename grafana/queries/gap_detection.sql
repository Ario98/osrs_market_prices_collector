-- Grafana Query: Detect Timestamp Gaps in prices table
-- This query shows all 5-minute intervals that should have data but don't

WITH expected_intervals AS (
    -- Generate all expected 5-minute intervals for the selected time range
    SELECT generate_series(
        $__timeFrom(),  -- Grafana time picker start
        $__timeTo(),    -- Grafana time picker end
        INTERVAL '5 minutes'
    ) as expected_time
),
actual_intervals AS (
    -- Get actual intervals that have data
    SELECT DISTINCT timestamp as actual_time
    FROM prices
    WHERE timestamp BETWEEN $__timeFrom() AND $__timeTo()
)
SELECT
    expected_time as "time",
    1 as "gap",
    'Missing Data' as "status"
FROM expected_intervals
LEFT JOIN actual_intervals ON expected_time = actual_time
WHERE actual_time IS NULL
ORDER BY expected_time;

-- Alternative: Show as continuous line with gaps highlighted
-- Use with Grafana "Null value" option set to "null as zero"

WITH time_series AS (
    SELECT 
        time_bucket('5 minutes', timestamp) as time,
        COUNT(*) as record_count
    FROM prices
    WHERE $__timeFilter(timestamp)
    GROUP BY time_bucket('5 minutes', timestamp)
    ORDER BY time
)
SELECT 
    time as "time",
    CASE 
        WHEN record_count > 0 THEN record_count 
        ELSE NULL  -- This creates the gap in the line
    END as "records",
    CASE 
        WHEN record_count = 0 OR record_count IS NULL THEN 1 
        ELSE 0 
    END as "gap_indicator"
FROM time_series;
