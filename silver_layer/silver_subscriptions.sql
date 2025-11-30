-- Silver Subscriptions with DQ fix:
-- Flag subscriptions where start_date < signup_date (375 records)
-- These are kept for auditing but excluded from metrics in Gold layer
DROP TABLE IF EXISTS ornate-lead-479415-h3.product_analytics.silver_subscriptions;
CREATE TABLE ornate-lead-479415-h3.product_analytics.silver_subscriptions AS
SELECT 
    s.subscription_id,
    s.user_id,
    LOWER(TRIM(s.plan)) AS plan,
    CAST(s.amount AS NUMERIC) AS amount,
    UPPER(TRIM(s.currency)) AS currency,
    
    -- Parse start_date (YYYY-MM-DD format)
    CAST(s.start_date AS DATE) AS start_date,
    
    -- Parse end_date (MM/DD/YYYY format or NULL) - optional field
    CASE 
        WHEN s.end_date IS NULL OR TRIM(s.end_date) = '' THEN NULL
        ELSE SAFE.PARSE_DATE('%m/%d/%Y', s.end_date)
    END AS end_date,
    
    s.invoice_id,
    
    -- Flag anomalous subscriptions (start before signup)
    CASE 
        WHEN CAST(s.start_date AS DATE) < u.signup_date THEN 1 
        ELSE 0 
    END AS is_anomalous

FROM ornate-lead-479415-h3.product_analytics.bronze_subscriptions s
LEFT JOIN ornate-lead-479415-h3.product_analytics.silver_users u ON s.user_id = u.user_id
WHERE s.subscription_id IS NOT NULL AND TRIM(s.subscription_id) != '';