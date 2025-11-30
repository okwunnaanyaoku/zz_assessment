DROP TABLE IF EXISTS ornate-lead-479415-h3.product_analytics.silver_users;
CREATE TABLE ornate-lead-479415-h3.product_analytics.silver_users AS
WITH 
-- Get earliest pre-signup click per user for attribution backfill
earliest_click AS (
    SELECT 
        mc.user_id,
        mc.utm_source AS click_utm_source,
        mc.clicked_at,
        ROW_NUMBER() OVER (
            PARTITION BY mc.user_id 
            ORDER BY mc.clicked_at ASC
        ) AS click_rank
    FROM ornate-lead-479415-h3.product_analytics.silver_marketing_clicks mc
    INNER JOIN ornate-lead-479415-h3.product_analytics.bronze_users bu ON mc.user_id = bu.user_id
    WHERE mc.user_id IS NOT NULL
      AND mc.clicked_at <= CAST(bu.signup_date AS TIMESTAMP)  -- Only pre-signup clicks
)

SELECT 
    bu.user_id,
    LOWER(TRIM(bu.email)) AS email,
    
    -- Parse signup_date (already in YYYY-MM-DD HH:MM:SS format)
    CAST(bu.signup_date AS TIMESTAMP) AS signup_at,
    DATE(bu.signup_date) AS signup_date,
    DATE_TRUNC(CAST(bu.signup_date AS TIMESTAMP), WEEK) AS signup_week,
    
    -- Attribution enrichment: user utm_source → earliest click → Unknown
    CASE 
        WHEN bu.utm_source IS NOT NULL AND TRIM(bu.utm_source) != '' 
            THEN TRIM(bu.utm_source)
        WHEN ec.click_utm_source IS NOT NULL 
            THEN ec.click_utm_source
        ELSE 'Unknown'
    END AS utm_source,
    
    -- Track attribution source for audit/debugging
    CASE 
        WHEN bu.utm_source IS NOT NULL AND TRIM(bu.utm_source) != '' 
            THEN 'user_table'
        WHEN ec.click_utm_source IS NOT NULL 
            THEN 'marketing_click_backfill'
        ELSE 'unknown'
    END AS utm_source_origin,
    
    bu.lifecycle_stage,
    
    -- Normalize country names
    CASE 
        WHEN UPPER(TRIM(bu.raw_country)) IN ('U.S.', 'US', 'USA', 'UNITED STATES', 'UNITED STATES ') 
            THEN 'United States'
        WHEN UPPER(TRIM(bu.raw_country)) IN ('CANADA') THEN 'Canada'
        WHEN UPPER(TRIM(bu.raw_country)) IN ('GERMANY') THEN 'Germany'
        WHEN UPPER(TRIM(bu.raw_country)) IN ('FRANCE') THEN 'France'
        WHEN UPPER(TRIM(bu.raw_country)) IN ('UK', 'UNITED KINGDOM') THEN 'United Kingdom'
        ELSE TRIM(bu.raw_country)
    END AS country,
    
    bu.external_id

FROM ornate-lead-479415-h3.product_analytics.bronze_users bu
LEFT JOIN earliest_click ec 
    ON bu.user_id = ec.user_id 
    AND ec.click_rank = 1  -- Only earliest click
WHERE bu.user_id IS NOT NULL AND TRIM(bu.user_id) != '';