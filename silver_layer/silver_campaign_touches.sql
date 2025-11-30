-- Captures both first-touch and any-touch attribution in one table
DROP TABLE IF EXISTS ornate-lead-479415-h3.product_analytics.silver_campaign_touches;
CREATE TABLE ornate-lead-479415-h3.product_analytics.silver_campaign_touches AS
WITH first_touch AS (
    -- Get first click per user for first-touch attribution (standard SQL)
    SELECT 
        user_id,
        utm_campaign AS first_utm_campaign,
        utm_source AS first_utm_source,
        clicked_at AS first_click_at
    FROM (
        SELECT 
            user_id,
            utm_campaign,
            utm_source,
            clicked_at,
            ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY clicked_at ASC) AS rn
        FROM ornate-lead-479415-h3.product_analytics.silver_marketing_clicks
        WHERE user_id IS NOT NULL
    ) ranked
    WHERE rn = 1
)
SELECT
    mc.user_id,
    
    -- First-touch attribution (mutually exclusive, sums to 100%)
    ft.first_utm_campaign,
    ft.first_utm_source,
    ft.first_click_at,
    
    -- Any-touch flags (user counted in all campaigns they touched)
    MAX(CASE WHEN mc.utm_campaign = 'new_user' THEN 1 ELSE 0 END) AS touched_new_user,
    MAX(CASE WHEN mc.utm_campaign = 'retargeting' THEN 1 ELSE 0 END) AS touched_retargeting,
    MAX(CASE WHEN mc.utm_campaign = 'promo_q3' THEN 1 ELSE 0 END) AS touched_promo_q3,
    MAX(CASE WHEN mc.utm_campaign = 'summer_sale' THEN 1 ELSE 0 END) AS touched_summer_sale,
    
    -- Multi-touch count (for DQ/caveats)
    COUNT(DISTINCT mc.utm_campaign) AS campaigns_touched_count
    
FROM ornate-lead-479415-h3.product_analytics.silver_marketing_clicks mc
LEFT JOIN first_touch ft ON mc.user_id = ft.user_id
WHERE mc.user_id IS NOT NULL
GROUP BY mc.user_id, ft.first_utm_campaign, ft.first_utm_source, ft.first_click_at;