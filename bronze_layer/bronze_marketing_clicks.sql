-- Bronze: Marketing Clicks (raw load)
DROP TABLE IF EXISTS ornate-lead-479415-h3.product_analytics.bronze_marketing_clicks;
CREATE TABLE ornate-lead-479415-h3.product_analytics.bronze_marketing_clicks AS
SELECT 
    click_id,
    user_id,
    utm_source,
    utm_campaign,
    clicked_at,
    CURRENT_TIMESTAMP AS ingested_at
FROM ornate-lead-479415-h3.product_analytics.raw_marketing_clicks_csv;