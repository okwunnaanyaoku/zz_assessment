-- Bronze: Events (raw load)
DROP TABLE IF EXISTS ornate-lead-479415-h3.product_analytics.bronze_events;
CREATE TABLE ornate-lead-479415-h3.product_analytics.bronze_events AS
SELECT 
    event_id,
    user_id,
    event_type,
    event_ts,
    session_id,
    CURRENT_TIMESTAMP AS ingested_at
FROM ornate-lead-479415-h3.product_analytics.raw_events_csv;