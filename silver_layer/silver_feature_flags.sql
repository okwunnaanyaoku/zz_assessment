-- Normalize flag names and deduplicate
DROP TABLE IF EXISTS ornate-lead-479415-h3.product_analytics.silver_feature_flags;
CREATE TABLE ornate-lead-479415-h3.product_analytics.silver_feature_flags AS
SELECT 
    user_id,
    -- Unify QuickStart variants
    CASE 
        WHEN flag_name IN ('qs_v1', 'quickstart_v1') THEN 'quickstart'
        ELSE LOWER(TRIM(flag_name))
    END AS flag_name,
    CAST(exposed_at AS TIMESTAMP) AS exposed_at

FROM ornate-lead-479415-h3.product_analytics.bronze_feature_flags
WHERE user_id IS NOT NULL AND TRIM(user_id) != '';

-- Helper table: First QuickStart exposure per user
DROP TABLE IF EXISTS ornate-lead-479415-h3.product_analytics.silver_quickstart_exposure;
CREATE TABLE ornate-lead-479415-h3.product_analytics.silver_quickstart_exposure AS
SELECT 
   user_id,
   MIN(exposed_at) AS first_exposed_at
FROM ornate-lead-479415-h3.product_analytics.silver_feature_flags
WHERE flag_name = 'quickstart'
GROUP BY user_id;