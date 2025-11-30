-- Bronze: Feature Flags (raw load)
DROP TABLE IF EXISTS ornate-lead-479415-h3.product_analytics.bronze_feature_flags;
CREATE TABLE ornate-lead-479415-h3.product_analytics.bronze_feature_flags AS
SELECT 
    user_id,
    flag_name,
    exposed_at,
    CURRENT_TIMESTAMP AS ingested_at
FROM ornate-lead-479415-h3.product_analytics.raw_feature_flags_csv;