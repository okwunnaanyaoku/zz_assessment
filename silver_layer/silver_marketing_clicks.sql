DROP TABLE IF EXISTS ornate-lead-479415-h3.product_analytics.silver_marketing_clicks;
CREATE TABLE ornate-lead-479415-h3.product_analytics.silver_marketing_clicks AS
SELECT 
    click_id,
    CASE WHEN TRIM(user_id) = '' THEN NULL ELSE user_id END AS user_id,
    TRIM(utm_source) AS utm_source,
    LOWER(TRIM(utm_campaign)) AS utm_campaign,
    CAST(clicked_at AS TIMESTAMP) AS clicked_at

FROM ornate-lead-479415-h3.product_analytics.bronze_marketing_clicks
WHERE click_id IS NOT NULL AND TRIM(click_id) != '';