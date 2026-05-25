-- =============================================================================
-- Q2e: Fraud Detection Alert Patterns
-- OJK regulatory report: flag suspicious customers for review
-- Three alert types (evaluated independently, all three can fire per customer):
--
--   VELOCITY_SPIKE   — 5+ transactions within any single clock hour
--   GEO_ANOMALY      — transactions in 3+ distinct cities on the same calendar day
--   AMOUNT_SPIKE     — single transaction > 3× the customer's own 30-day average
--
-- Performance notes:
--   - DATE_TRUNC session windows (not sliding RANGE BETWEEN) — O(n) not O(n²)
--   - Partition pruning: filter transactions to a configurable look-back window
--   - Each alert CTE is independent; UNION ALL avoids cross-join fan-out
--   - Output partitioned by alert_date for incremental refresh patterns (Q2f)
-- =============================================================================

CREATE OR REPLACE TABLE negarabank.gold.fraud_detection_alerts
USING DELTA
PARTITIONED BY (alert_date)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
)
AS

WITH

-- ── Base: recent transactions (configurable look-back, default 90 days) ───────
recent_txns AS (
  SELECT
    t.customer_id,
    t.transaction_id,
    t.amount,
    t.txn_date,
    t.status,
    -- city derived from account's branch metadata (join to accounts)
    a.branch_city
  FROM negarabank.silver.transactions t
  LEFT JOIN negarabank.silver.accounts a
    ON  t.account_id  = a.account_id
    AND a.is_current  = true
  WHERE t.is_current  = true
    AND t.txn_date   >= DATEADD(DAY, -90, CURRENT_DATE())
    AND UPPER(t.status) NOT IN ('FAILED', 'REVERSED')
),

-- ── 1. VELOCITY_SPIKE: 5+ transactions in any 1-hour bucket ──────────────────
velocity_alerts AS (
  SELECT
    customer_id,
    DATE_TRUNC('hour', txn_date)          AS window_start,
    CAST(DATE(txn_date) AS DATE)          AS alert_date,
    COUNT(*)                              AS txn_count,
    SUM(amount)                           AS window_total_amount,
    MIN(amount)                           AS min_amount,
    MAX(amount)                           AS max_amount
  FROM recent_txns
  GROUP BY customer_id, DATE_TRUNC('hour', txn_date), CAST(DATE(txn_date) AS DATE)
  HAVING COUNT(*) >= 5
),

-- ── 2. GEO_ANOMALY: 3+ distinct cities in a single calendar day ──────────────
geo_alerts AS (
  SELECT
    customer_id,
    CAST(DATE(txn_date) AS DATE)          AS alert_date,
    COUNT(DISTINCT branch_city)           AS distinct_cities,
    COLLECT_LIST(DISTINCT branch_city)    AS cities_list
  FROM recent_txns
  WHERE branch_city IS NOT NULL
  GROUP BY customer_id, CAST(DATE(txn_date) AS DATE)
  HAVING COUNT(DISTINCT branch_city) >= 3
),

-- ── 3. AMOUNT_SPIKE: single txn > 3× rolling 30-day personal average ─────────
-- Step 3a: compute each customer's 30-day rolling average (exclude outlier day)
customer_30d_avg AS (
  SELECT
    customer_id,
    CAST(DATE(txn_date) AS DATE)          AS txn_day,
    AVG(amount) OVER (
      PARTITION BY customer_id
      ORDER BY CAST(DATE(txn_date) AS DATE)
      RANGE BETWEEN 30 PRECEDING AND 1 PRECEDING  -- exclude same-day (self-referential)
    )                                             AS avg_30d_amount
  FROM recent_txns
),

-- Step 3b: join back to flag spiking transactions
amount_spike_raw AS (
  SELECT
    t.customer_id,
    t.transaction_id,
    t.amount,
    CAST(DATE(t.txn_date) AS DATE)        AS alert_date,
    ca.avg_30d_amount,
    t.amount / NULLIF(ca.avg_30d_amount, 0) AS spike_ratio
  FROM recent_txns t
  JOIN customer_30d_avg ca
    ON  t.customer_id = ca.customer_id
    AND CAST(DATE(t.txn_date) AS DATE) = ca.txn_day
  WHERE t.amount > ca.avg_30d_amount * 3
    AND ca.avg_30d_amount IS NOT NULL
),

amount_alerts AS (
  SELECT
    customer_id,
    alert_date,
    COUNT(*)                              AS spike_txn_count,
    MAX(amount)                           AS max_spike_amount,
    MAX(spike_ratio)                      AS max_spike_ratio,
    AVG(avg_30d_amount)                   AS baseline_avg_amount
  FROM amount_spike_raw
  GROUP BY customer_id, alert_date
)

-- ── Final assembly: UNION ALL three alert types ────────────────────────────────
SELECT
  v.customer_id,
  'VELOCITY_SPIKE'                        AS alert_type,
  v.alert_date,
  TO_JSON(NAMED_STRUCT(
    'window_start',       CAST(v.window_start AS STRING),
    'txn_count',          v.txn_count,
    'window_total_amount',ROUND(v.window_total_amount, 2),
    'min_amount',         ROUND(v.min_amount, 2),
    'max_amount',         ROUND(v.max_amount, 2)
  ))                                      AS details_json,
  CURRENT_TIMESTAMP()                     AS computed_at

FROM velocity_alerts v

UNION ALL

SELECT
  g.customer_id,
  'GEO_ANOMALY'                           AS alert_type,
  g.alert_date,
  TO_JSON(NAMED_STRUCT(
    'distinct_cities',  g.distinct_cities,
    'cities',           CAST(g.cities_list AS STRING)
  ))                                      AS details_json,
  CURRENT_TIMESTAMP()                     AS computed_at

FROM geo_alerts g

UNION ALL

SELECT
  a.customer_id,
  'AMOUNT_SPIKE'                          AS alert_type,
  a.alert_date,
  TO_JSON(NAMED_STRUCT(
    'spike_txn_count',    a.spike_txn_count,
    'max_spike_amount',   ROUND(a.max_spike_amount, 2),
    'max_spike_ratio',    ROUND(a.max_spike_ratio, 2),
    'baseline_avg_amount',ROUND(a.baseline_avg_amount, 2)
  ))                                      AS details_json,
  CURRENT_TIMESTAMP()                     AS computed_at

FROM amount_alerts a
