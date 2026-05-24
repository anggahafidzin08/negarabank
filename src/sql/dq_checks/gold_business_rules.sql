-- Gold Layer: Business Rule Validation
-- Check fraud scores, event counts, and other business logic

SELECT
    'fraud_score_range' as rule_name,
    COUNT(CASE WHEN fraud_score < 0 OR fraud_score > 1 THEN 1 END) as violating_records,
    'fraud_score must be between 0.0 and 1.0' as rule_description,
    CASE
        WHEN COUNT(CASE WHEN fraud_score < 0 OR fraud_score > 1 THEN 1 END) = 0 THEN 'pass'
        ELSE 'fail'
    END as status
FROM gold.fact_fraud_transaction_alert

UNION ALL

SELECT
    'alert_status_mapping' as rule_name,
    COUNT(CASE WHEN fraud_alert_status NOT IN ('HIGH_RISK', 'MEDIUM_RISK', 'LOW_RISK') THEN 1 END),
    'alert_status must be valid enum',
    CASE
        WHEN COUNT(CASE WHEN fraud_alert_status NOT IN ('HIGH_RISK', 'MEDIUM_RISK', 'LOW_RISK') THEN 1 END) = 0 THEN 'pass'
        ELSE 'fail'
    END
FROM gold.fact_fraud_transaction_alert

UNION ALL

SELECT
    'event_count_reasonableness' as rule_name,
    COUNT(CASE WHEN event_count_24h > 10000 THEN 1 END),
    'event_count_24h > 10000 is suspicious',
    'warn'
FROM gold.fact_fraud_transaction_alert;
