-- =============================================================================
-- fact_fraud_risk: daily customer fraud risk snapshot
-- incremental_merge — watermark ${watermark_ts} scopes new Silver rows
-- Grain: one row per (customer_id, snapshot_date)
-- Joins daily transaction metrics with the latest credit score per customer.
-- =============================================================================
WITH
daily_txn_metrics AS (
  SELECT
    customer_id,
    CAST(DATE(txn_date) AS DATE)                              AS snapshot_date,
    COUNT(*)                                                  AS daily_txn_count,
    ROUND(SUM(amount), 2)                                     AS daily_total_amount,
    COUNT(CASE WHEN UPPER(status) = 'FAILED' THEN 1 END)     AS daily_failed_count
  FROM negarabank.silver.transactions
  WHERE is_current     = true
    AND load_timestamp > '${watermark_ts}'
  GROUP BY customer_id, CAST(DATE(txn_date) AS DATE)
),

latest_credit_score AS (
  SELECT
    customer_id,
    CAST(score AS INT)                AS credit_score,
    probability_of_default
  FROM negarabank.bronze.credit_scores
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY customer_id
    ORDER BY load_timestamp DESC
  ) = 1
)

MERGE INTO negarabank.gold.fact_fraud_risk AS target
USING (
  SELECT
    d.customer_id,
    d.snapshot_date,
    c.credit_score,
    c.probability_of_default,
    d.daily_txn_count,
    d.daily_total_amount,
    d.daily_failed_count,
    CASE
      WHEN c.probability_of_default > 0.5 OR d.daily_failed_count >= 3 THEN 'HIGH'
      WHEN c.probability_of_default > 0.3 OR d.daily_failed_count >= 1 THEN 'MEDIUM'
      ELSE 'LOW'
    END                               AS risk_level,
    CURRENT_TIMESTAMP()               AS computed_at
  FROM daily_txn_metrics d
  LEFT JOIN latest_credit_score c ON d.customer_id = c.customer_id
) AS source
ON  target.customer_id   = source.customer_id
AND target.snapshot_date = source.snapshot_date

WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
