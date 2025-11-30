-- =====================================================================
-- DATA QUALITY TEST SUITE
-- Purpose: Validate joinability, timestamps, attribution, and integrity
-- =====================================================================

-- 1. Orphan Events (events with user_id not found in users table)
SELECT COUNT(*) AS orphan_events
FROM ornate-lead-479415-h3.product_analytics.silver_events e
LEFT JOIN ornate-lead-479415-h3.product_analytics.silver_users u 
    ON e.user_id = u.user_id
WHERE e.user_id IS NOT NULL AND u.user_id IS NULL;

-- 2. Events with NULL or empty user_id in raw
SELECT COUNT(*) AS null_user_events
FROM ornate-lead-479415-h3.product_analytics.bronze_events
WHERE user_id IS NULL OR TRIM(user_id) = '';

-- 3. Events occurring before the user's signup timestamp
SELECT COUNT(*) AS events_before_signup
FROM ornate-lead-479415-h3.product_analytics.silver_events e
JOIN ornate-lead-479415-h3.product_analytics.silver_users u 
    ON e.user_id = u.user_id
WHERE e.event_ts < u.signup_at;

-- 4. Duplicate Events (same user_id + event_type + timestamp)
SELECT SUM(dup_count - 1) AS duplicate_event_rows
FROM (
    SELECT 
        user_id, event_type, event_ts,
        COUNT(*) AS dup_count
    FROM ornate-lead-479415-h3.product_analytics.silver_events
    WHERE user_id IS NOT NULL
    GROUP BY 1,2,3
    HAVING COUNT(*) > 1
) AS d;

-- 5. Feature Flag exposures before signup (QuickStart anomalies)
SELECT COUNT(*) AS flag_exposures_before_signup
FROM ornate-lead-479415-h3.product_analytics.silver_feature_flags ff
JOIN ornate-lead-479415-h3.product_analytics.silver_users u 
    ON ff.user_id = u.user_id
WHERE ff.exposed_at < u.signup_at;

-- 6. Subscription anomalies (start_date < signup_date)
SELECT COUNT(*) AS subscription_anomalies
FROM ornate-lead-479415-h3.product_analytics.silver_subscriptions s
JOIN ornate-lead-479415-h3.product_analytics.silver_users u
    ON s.user_id = u.user_id
WHERE s.start_date < u.signup_date;

-- 7. Unlinked marketing clicks (clicks not associated with any user)
SELECT COUNT(*) AS unlinked_marketing_clicks
FROM ornate-lead-479415-h3.product_analytics.silver_marketing_clicks
WHERE user_id IS NULL;

-- 8. Attribution completeness breakdown (user_table vs backfill vs unknown)
SELECT 
    utm_source_origin,
    COUNT(*) AS user_count
FROM ornate-lead-479415-h3.product_analytics.silver_users
GROUP BY utm_source_origin;
