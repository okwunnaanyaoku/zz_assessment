-- Bronze: Users (raw load)
DROP TABLE IF EXISTS ornate-lead-479415-h3.product_analytics.bronze_users;
CREATE TABLE ornate-lead-479415-h3.product_analytics.bronze_users AS
SELECT 
    user_id,
    email,
    signup_date,
    utm_source,
    lifecycle_stage,
    raw_country,
    created_at_str,
    external_id,
    CURRENT_TIMESTAMP AS ingested_at
FROM ornate-lead-479415-h3.product_analytics.raw_users_csv;
