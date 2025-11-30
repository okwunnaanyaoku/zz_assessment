-- ============================================================================
-- PHASE 5: VALIDATION TESTS (Run after Gold layer)
-- Goal: Verify the integrity, consistency, and correctness of derived metrics
-- ============================================================================

-- Test 1: Row count consistency (Silver Users vs Gold Metrics)
SELECT 
    'Row Count Check' AS test_name,
    (SELECT COUNT(*) 
     FROM ornate-lead-479415-h3.product_analytics.silver_users) AS silver_users,
    (SELECT COUNT(*) 
     FROM ornate-lead-479415-h3.product_analytics.gold_user_metrics) AS gold_users,
    CASE 
        WHEN (SELECT COUNT(*) FROM ornate-lead-479415-h3.product_analytics.silver_users)
           = (SELECT COUNT(*) FROM ornate-lead-479415-h3.product_analytics.gold_user_metrics)
        THEN 'PASS' ELSE 'FAIL' 
    END AS status;

-- Test 2: Metric ranges (activated, retained_7d, paid should be in [0,1])
SELECT 
    'Metric Range Check' AS test_name,
    MIN(activated) AS min_activated,
    MAX(activated) AS max_activated,
    MIN(retained_7d) AS min_retained,
    MAX(retained_7d) AS max_retained,
    MIN(paid) AS min_paid,
    MAX(paid) AS max_paid,
    CASE 
        WHEN MIN(activated) >= 0 AND MAX(activated) <= 1
         AND MIN(retained_7d) >= 0 AND MAX(retained_7d) <= 1
         AND MIN(paid) >= 0 AND MAX(paid) <= 1
        THEN 'PASS' ELSE 'FAIL' 
    END AS status
FROM ornate-lead-479415-h3.product_analytics.gold_user_metrics;

-- Test 3: Required fields should never be NULL
SELECT 
    'NULL Check - Key Columns' AS test_name,
    SUM(CASE WHEN user_id IS NULL THEN 1 ELSE 0 END) AS null_user_id,
    SUM(CASE WHEN activated IS NULL THEN 1 ELSE 0 END) AS null_activated,
    SUM(CASE WHEN retained_7d IS NULL THEN 1 ELSE 0 END) AS null_retained,
    SUM(CASE WHEN paid IS NULL THEN 1 ELSE 0 END) AS null_paid,
    CASE 
        WHEN SUM(CASE WHEN user_id IS NULL THEN 1 END) IS NULL
         AND SUM(CASE WHEN activated IS NULL THEN 1 END) IS NULL
         AND SUM(CASE WHEN retained_7d IS NULL THEN 1 END) IS NULL
         AND SUM(CASE WHEN paid IS NULL THEN 1 END) IS NULL
        THEN 'PASS' ELSE 'FAIL' 
    END AS status
FROM ornate-lead-479415-h3.product_analytics.gold_user_metrics;

-- Test 4: Funnel monotonicity (Signup ≥ Activated ≥ Trial ≥ Paid)
SELECT 
    'Funnel Monotonicity Check' AS test_name,
    COUNT(*) AS total_signups,
    SUM(activated) AS activated,
    SUM(started_trial) AS trials,
    SUM(paid) AS paid,
    CASE 
        WHEN SUM(activated) <= COUNT(*)
         AND SUM(started_trial) <= SUM(activated)
         AND SUM(paid) <= SUM(started_trial)
        THEN 'PASS'
        ELSE 'CHECK — Users may skip steps (non-linear funnel)' 
    END AS status
FROM ornate-lead-479415-h3.product_analytics.gold_user_metrics;

-- Test 5: Distribution of funnel anomalies
SELECT 
    'Funnel Anomaly Check' AS test_name,
    funnel_anomaly,
    COUNT(*) AS user_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_of_users
FROM ornate-lead-479415-h3.product_analytics.gold_user_metrics
GROUP BY funnel_anomaly
ORDER BY user_count DESC;

-- Test 6: QuickStart exposure should be binary (0/1 only)
SELECT 
    'QuickStart Binary Check' AS test_name,
    COUNT(DISTINCT quickstart_exposed) AS distinct_values,
    MIN(quickstart_exposed) AS min_value,
    MAX(quickstart_exposed) AS max_value,
    CASE 
        WHEN COUNT(DISTINCT quickstart_exposed) <= 2
         AND MIN(quickstart_exposed) >= 0
         AND MAX(quickstart_exposed) <= 1
        THEN 'PASS' ELSE 'FAIL' 
    END AS status
FROM ornate-lead-479415-h3.product_analytics.gold_user_metrics;

-- Test 7: user_id must be unique in Gold metrics
SELECT 
    'Duplicate User Check' AS test_name,
    COUNT(*) AS total_rows,
    COUNT(DISTINCT user_id) AS distinct_users,
    CASE WHEN COUNT(*) = COUNT(DISTINCT user_id)
         THEN 'PASS' ELSE 'FAIL' 
    END AS status
FROM ornate-lead-479415-h3.product_analytics.gold_user_metrics;

-- Test 8: Activation timing must be non-negative
SELECT 
    'Activation Timing Check' AS test_name,
    MIN(hours_to_activation) AS min_hours,
    MAX(hours_to_activation) AS max_hours,
    CASE 
        WHEN MIN(hours_to_activation) >= 0 OR MIN(hours_to_activation) IS NULL
        THEN 'PASS' ELSE 'FAIL (Negative activation times detected)' 
    END AS status
FROM ornate-lead-479415-h3.product_analytics.gold_user_metrics;

-- Test 9: Event depth must be non-negative
SELECT 
    'Event Depth Check' AS test_name,
    MIN(event_depth_48h) AS min_depth,
    MAX(event_depth_48h) AS max_depth,
    CASE 
        WHEN MIN(event_depth_48h) >= 0 THEN 'PASS' ELSE 'FAIL' 
    END AS status
FROM ornate-lead-479415-h3.product_analytics.gold_user_metrics;
