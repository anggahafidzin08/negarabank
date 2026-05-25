-- =============================================================================
-- Q2e: Fraud Detection Alert Patterns
-- incremental_merge — watermark ${watermark_ts} scopes new Silver rows
-- Grain: one row per (customer_id, alert_type, alert_date)
-- Two alert types:
--   VELOCITY_SPIKE — 5+ transactions within any single clock hour
--   AMOUNT_SPIKE   — single transaction > 3× the customer's own 30-day average
--
-- Note: GEO_ANOMALY excluded — no location data in Silver transactions schema.
--
-- Incremental scope:
--   affected_customers — customer_ids with new Silver rows since watermark
--   affected_txns      — ALL 90-day transactions for those customers, so:
--     • VELOCITY_SPIKE bucket COUNTs include all txns in the hour, not just new
--     • AMOUNT_SPIKE 30d AVG baseline has prior-day rows via RANGE BETWEEN
-- =============================================================================
WITH

affected_customers AS (
  SELECT DISTINCT customer_id
  FROM negarabank.silver.transactions
  WHERE is_current     = true
    AND load_timestamp > '${watermark_ts}'
    AND txn_date      >= DATEADD(DAY, -90, CURRENT_DATE())
),

affected_txns AS (
  SELECT
    customer_id,
    transaction_id,
    amount,
    txn_date,
    status
  FROM negarabank.silver.transactions
  WHERE is_current   = true
    AND customer_id  IN (SELECT customer_id FROM affected_customers)
    AND txn_date    >= DATEADD(DAY, -90, CURRENT_DATE())
    AND UPPER(status) NOT IN ('FAILED', 'REVERSED')
),

velocity_alerts AS (
  SELECT
    customer_id,
    DATE_TRUNC('hour', txn_date)         AS window_start,
    CAST(DATE(txn_date) AS DATE)         AS alert_date,
    COUNT(*)                             AS txn_count,
    ROUND(SUM(amount), 2)                AS window_total_amount,
    ROUND(MIN(amount), 2)                AS min_amount,
    ROUND(MAX(amount), 2)                AS max_amount
  FROM affected_txns
  GROUP BY customer_id, DATE_TRUNC('hour', txn_date), CAST(DATE(txn_date) AS DATE)
  HAVING COUNT(*) >= 5
),

customer_30d_avg AS (
  SELECT
    customer_id,
    CAST(DATE(txn_date) AS DATE)         AS txn_day,
    AVG(amount) OVER (
      PARTITION BY customer_id
      ORDER BY CAST(DATE(txn_date) AS DATE)
      RANGE BETWEEN 30 PRECEDING AND 1 PRECEDING
    )                                    AS avg_30d_amount
  FROM affected_txns
),

amount_spike_raw AS (
  SELECT
    t.customer_id,
    CAST(DATE(t.txn_date) AS DATE)       AS alert_date,
    t.amount,
    ca.avg_30d_amount,
    t.amount / NULLIF(ca.avg_30d_amount, 0) AS spike_ratio
  FROM affected_txns t
  JOIN customer_30d_avg ca
    ON  t.customer_id                    = ca.customer_id
    AND CAST(DATE(t.txn_date) AS DATE)   = ca.txn_day
  WHERE t.amount      > ca.avg_30d_amount * 3
    AND ca.avg_30d_amount IS NOT NULL
),

amount_alerts AS (
  SELECT
    customer_id,
    alert_date,
    COUNT(*)                             AS spike_txn_count,
    ROUND(MAX(amount), 2)                AS max_spike_amount,
    ROUND(MAX(spike_ratio), 2)           AS max_spike_ratio,
    ROUND(AVG(avg_30d_amount), 2)        AS baseline_avg_amount
  FROM amount_spike_raw
  GROUP BY customer_id, alert_date
)

MERGE INTO negarabank.gold.fraud_detection_alerts AS target
USING (
  SELECT
    customer_id,
    'VELOCITY_SPIKE'                     AS alert_type,
    alert_date,
    TO_JSON(NAMED_STRUCT(
      'window_start',        CAST(window_start AS STRING),
      'txn_count',           txn_count,
      'window_total_amount', window_total_amount,
      'min_amount',          min_amount,
      'max_amount',          max_amount
    ))                                   AS details_json,
    CURRENT_TIMESTAMP()                  AS computed_at
  FROM velocity_alerts

  UNION ALL

  SELECT
    customer_id,
    'AMOUNT_SPIKE'                       AS alert_type,
    alert_date,
    TO_JSON(NAMED_STRUCT(
      'spike_txn_count',     spike_txn_count,
      'max_spike_amount',    max_spike_amount,
      'max_spike_ratio',     max_spike_ratio,
      'baseline_avg_amount', baseline_avg_amount
    ))                                   AS details_json,
    CURRENT_TIMESTAMP()                  AS computed_at
  FROM amount_alerts
) AS source
ON  target.customer_id = source.customer_id
AND target.alert_type  = source.alert_type
AND target.alert_date  = source.alert_date

WHEN MATCHED THEN UPDATE SET
  details_json = source.details_json,
  computed_at  = source.computed_at

WHEN NOT MATCHED THEN INSERT *
