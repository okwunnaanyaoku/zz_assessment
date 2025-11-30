DROP TABLE IF EXISTS ornate-lead-479415-h3.product_analytics.gold_user_metrics;
CREATE TABLE ornate-lead-479415-h3.product_analytics.gold_user_metrics AS
WITH 
-- -------------------------
-- Activation
-- -------------------------
activations AS (
    SELECT 
        e.user_id,
        MIN(e.event_ts) AS activated_at
    FROM ornate-lead-479415-h3.product_analytics.silver_events e
    JOIN ornate-lead-479415-h3.product_analytics.silver_users u 
        ON e.user_id = u.user_id
    WHERE e.event_type = 'complete_quiz'
      AND e.event_ts >= u.signup_at
    GROUP BY e.user_id
),

-- -------------------------
-- Retention
-- -------------------------
retention AS (
    SELECT 
        e.user_id,
        1 AS retained_7d
    FROM ornate-lead-479415-h3.product_analytics.silver_events e
    JOIN ornate-lead-479415-h3.product_analytics.silver_users u 
        ON e.user_id = u.user_id
    WHERE e.event_ts > TIMESTAMP_ADD(u.signup_at, INTERVAL 1 DAY)
      AND e.event_ts <= TIMESTAMP_ADD(u.signup_at, INTERVAL 7 DAY)
    GROUP BY e.user_id
),

-- -------------------------
-- Event Depth
-- -------------------------
event_depth AS (
    SELECT 
        e.user_id,
        COUNT(*) AS events_48h
    FROM ornate-lead-479415-h3.product_analytics.silver_events e
    JOIN ornate-lead-479415-h3.product_analytics.silver_users u 
        ON e.user_id = u.user_id
    WHERE e.event_ts >= u.signup_at
      AND e.event_ts <= TIMESTAMP_ADD(u.signup_at, INTERVAL 48 HOUR)
    GROUP BY e.user_id
),

-- -------------------------
-- Subscriptions
-- -------------------------
subscriptions AS (
    SELECT 
        user_id,
        MIN(start_date) AS first_paid_at,
        1 AS converted_paid
    FROM ornate-lead-479415-h3.product_analytics.silver_subscriptions
    WHERE is_anomalous = 0
    GROUP BY user_id
),

-- -------------------------
-- Trials
-- -------------------------
trials AS (
    SELECT 
        e.user_id,
        MIN(e.event_ts) AS trial_started_at
    FROM ornate-lead-479415-h3.product_analytics.silver_events e
    JOIN ornate-lead-479415-h3.product_analytics.silver_users u 
        ON e.user_id = u.user_id
    WHERE e.event_type = 'start_trial'
      AND e.event_ts >= u.signup_at
    GROUP BY e.user_id
),

-- -------------------------
-- Base Users (DEFINE EXPOSURES HERE)
-- -------------------------
base_users AS (
    SELECT
        u.user_id,
        u.signup_at,
        u.signup_date,
        u.signup_week,
        u.country,
        u.utm_source AS acquisition_channel,

        -- QuickStart exposure
        CASE WHEN qs.user_id IS NOT NULL THEN 1 ELSE 0 END AS quickstart_exposed,
        qs.first_exposed_at AS quickstart_exposed_at,

        -- Campaign touches
        COALESCE(ct.first_utm_campaign, 'unknown') AS first_utm_campaign,
        COALESCE(ct.touched_new_user, 0) AS touched_new_user,
        COALESCE(ct.touched_retargeting, 0) AS touched_retargeting,
        COALESCE(ct.touched_promo_q3, 0) AS touched_promo_q3,
        COALESCE(ct.touched_summer_sale, 0) AS touched_summer_sale,
        COALESCE(ct.campaigns_touched_count, 0) AS campaigns_touched_count,

        -- Trial campaign = promo_q3 only
        CASE WHEN COALESCE(ct.touched_promo_q3, 0) = 1 THEN 1 ELSE 0 END AS trial_campaign_exposed

    FROM ornate-lead-479415-h3.product_analytics.silver_users u
    LEFT JOIN ornate-lead-479415-h3.product_analytics.silver_quickstart_exposure qs 
        ON u.user_id = qs.user_id
    LEFT JOIN ornate-lead-479415-h3.product_analytics.silver_campaign_touches ct 
        ON u.user_id = ct.user_id
)

-- -------------------------
-- FINAL TABLE
-- -------------------------
SELECT 
    bu.*,

    -- Exposure Group (NOW VALID)
    CASE
        WHEN bu.quickstart_exposed = 1 AND bu.trial_campaign_exposed = 1
            THEN 'quickstart_and_trial'
        WHEN bu.quickstart_exposed = 1 AND bu.trial_campaign_exposed = 0
            THEN 'quickstart_only'
        WHEN bu.quickstart_exposed = 0 AND bu.trial_campaign_exposed = 1
            THEN 'trial_campaign_only'
        ELSE 'control'
    END AS exposure_group,

    -- Activation
    CASE WHEN a.user_id IS NOT NULL THEN 1 ELSE 0 END AS activated,
    a.activated_at,
    TIMESTAMP_DIFF(a.activated_at, bu.signup_at, HOUR) AS hours_to_activation,
    TIMESTAMP_DIFF(a.activated_at, bu.signup_at, DAY) AS days_to_activation,

    -- QuickStart → Activation lag
    CASE 
        WHEN bu.quickstart_exposed = 1 AND a.user_id IS NOT NULL 
        THEN TIMESTAMP_DIFF(a.activated_at, bu.quickstart_exposed_at, HOUR)
        ELSE NULL
    END AS hours_exposure_to_activation,

    -- Retention
    COALESCE(r.retained_7d, 0) AS retained_7d,

    -- Event depth
    COALESCE(ed.events_48h, 0) AS event_depth_48h,

    -- Trials
    CASE WHEN t.user_id IS NOT NULL THEN 1 ELSE 0 END AS started_trial,
    t.trial_started_at,

    -- Paid conversion
    COALESCE(s.converted_paid, 0) AS paid,
    s.first_paid_at AS paid_at,

    -- Trial → Paid
    CASE 
        WHEN t.user_id IS NOT NULL AND s.user_id IS NOT NULL THEN 1
        WHEN t.user_id IS NOT NULL THEN 0
        ELSE NULL
    END AS trial_to_paid,

    -- Anomalies
    CASE 
        WHEN s.user_id IS NOT NULL AND a.user_id IS NULL THEN 'paid_without_activation'
        WHEN t.user_id IS NOT NULL AND a.user_id IS NULL THEN 'trial_without_activation'
        ELSE NULL
    END AS funnel_anomaly

FROM base_users bu
LEFT JOIN activations a USING(user_id)
LEFT JOIN retention r USING(user_id)
LEFT JOIN event_depth ed USING(user_id)
LEFT JOIN subscriptions s USING(user_id)
LEFT JOIN trials t USING(user_id);
