-- Silver Events with DQ fixes applied:
-- 1. Exclude events with NULL/empty user_id (17% of events — can't be used for user metrics)
-- 2. Deduplicate on user_id + event_type + event_ts (exclude session_id — unstable)
-- 3. Normalize timestamps to unified format
-- Note: Pre-signup event filtering happens in Gold layer (preserves data for potential analysis)

DROP TABLE IF EXISTS ornate-lead-479415-h3.product_analytics.silver_events;
CREATE TABLE ornate-lead-479415-h3.product_analytics.silver_events AS
WITH parsed_events AS (
    SELECT 
        event_id,
        user_id,
        event_type,
        session_id,
        
        -- Parse mixed timestamp formats (BigQuery compatible)
        CASE 
            -- Epoch milliseconds (13 digits, all numeric)
            WHEN LENGTH(event_ts) = 13 AND REGEXP_CONTAINS(event_ts, r'^[0-9]+$')
                THEN TIMESTAMP_MILLIS(CAST(event_ts AS INT64))
            -- Standard ISO format
            ELSE SAFE_CAST(event_ts AS TIMESTAMP)
        END AS event_ts
        
    FROM ornate-lead-479415-h3.product_analytics.bronze_events
    WHERE event_id IS NOT NULL 
      AND TRIM(event_id) != ''
      -- Exclude events with no user_id (can't be used for user-level metrics)
      AND user_id IS NOT NULL 
      AND TRIM(user_id) != ''
),

-- Deduplicate on user_id + event_type + event_ts (keep first occurrence)
deduplicated AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY user_id, event_type, event_ts 
            ORDER BY event_id  -- Deterministic tie-breaker
        ) AS row_num
    FROM parsed_events
    WHERE event_ts IS NOT NULL
      -- Filter impossible dates
      AND event_ts > '2010-01-01'
      AND event_ts <= CURRENT_TIMESTAMP()
)

SELECT 
    event_id,
    user_id,
    event_type,
    event_ts,
    session_id,
    DATE(event_ts) AS event_date
FROM deduplicated
WHERE row_num = 1;