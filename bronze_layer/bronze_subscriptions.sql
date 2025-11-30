-- Bronze: Subscriptions (raw load)
DROP TABLE IF EXISTS ornate-lead-479415-h3.product_analytics.bronze_subscriptions;
CREATE TABLE ornate-lead-479415-h3.product_analytics.bronze_subscriptions AS
SELECT 
    subscription_id,
    user_id,
    plan,
    amount,
    currency,
    start_date,
    end_date,
    invoice_id,
    CURRENT_TIMESTAMP AS ingested_at
FROM ornate-lead-479415-h3.product_analytics.raw_subscription_csv;