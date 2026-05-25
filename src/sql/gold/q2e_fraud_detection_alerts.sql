-- =============================================================================
-- Q2e: Fraud Detection Alert Patterns — Incremental MERGE
-- OJK regulatory report: flag suspicious customers for review
-- Three alert types (evaluated independently, all three can fire per customer):
--
--   VELOCITY_SPIKE   — 5+ transactions within any single clock hour
--   GEO_ANOMALY      — transactions in 3+ distinct cities on the same calendar day
--   AMOUNT_SPIKE     — single transaction > 3× the customer's own 30-day average
--
-- Incremental strategy (watermark-driven):
--   1. Find customers who have new Silver transactions since the last Gold run
--      (load_timestamp > ${watermark_ts}).
--   2. For those customers, read ALL their transactions within the 90-day
--      look-back window — not just the new ones. This is required because:
--        - VELOCITY_SPIKE and GEO_ANOMALY group by time bucket: a new txn in
--          an existing hour/day must recompute the whole bucket's COUNT.
--        - AMOUNT_SPIKE uses a 30-day rolling AVG baseline: the window needs
--          prior transactions to produce a non-NULL avg_30d_amount.
--   3. Recompute all three alert types for the affected customer set.
--   4. MERGE INTO Gold on (customer_id, alert_type, alert_date):
--        MATCHED     → UPDATE details_json + computed_at
--        NOT MATCHED → INSERT (new alert)
--
-- Why this scope expansion is safe:
--   Typically < 5% of customers transact on any given day. Expanding from
--   "new rows only" to "all rows for affected customers in 90 days" is still
--   O(active_customers × avg_txns_per_90d) vs O(all_customers × all_txns).
-- =============================================================================

-- ── Step 1: Identify customers with new Silver transactions ────────────────
-- Scoped to the 90-day look-back window so we don't chase ancient watermarks.
CREATE OR REPLACE TEMP VIEW affected_customers AS
SELECT DISTINCT customer_id
FROM negarabank.silver.transactions
WHERE is_current      = true
  AND load_timestamp  > '${watermark_ts}'
  AND txn_date       >= DATEADD(DAY, -90, CURRENT_DATE());


-- ── Step 2: Full transaction context for affected customers ────────────────
-- Pull all non-failed transactions for these customers within the look-back
-- window. The broader window feeds the 30-day AVG baseline and bucket counts.
CREATE OR REPLACE TEMP VIEW affected_txns AS
SELECT
  t.customer_id,
  t.transaction_id,
  t.amount,
  t.txn_date,
  t.status,
  a.branch_city
FROM negarabank.silver.transactions t
LEFT JOIN negarabank.silver.accounts a
  ON  t.account_id = a.account_id
  AND a.is_current = true
WHERE t.is_current = true
  AND t.customer_id IN (SELECT customer_id FROM affected_customers)
  AND t.txn_date   >= DATEADD(DAY, -90, CURRENT_DATE())
  AND UPPER(t.status) NOT IN ('FAILED', 'REVERSED');


-- ── Step 3: Recompute all three alert types for affected customers ──────────
CREATE OR REPLACE TEMP VIEW fraud_alerts_incremental AS

WITH

-- 1. VELOCITY_SPIKE: 5+ transactions in any 1-hour bucket
velocity_alerts AS (
  SELECT
    customer_id,
    DATE_TRUNC('hour', txn_date)          AS window_start,
    CAST(DATE(txn_date) AS DATE)          AS alert_date,
    COUNT(*)                              AS txn_count,
    SUM(amount)                           AS window_total_amount,
    MIN(amount)                           AS min_amount,
    MAX(amount)                           AS max_amount
  FROM affected_txns
  GROUP BY customer_id, DATE_TRUNC('hour', txn_date), CAST(DATE(txn_date) AS DATE)
  HAVING COUNT(*) >= 5
),

-- 2. GEO_ANOMALY: 3+ distinct cities on a single calendar day
geo_alerts AS (
  SELECT
    customer_id,
    CAST(DATE(txn_date) AS DATE)          AS alert_date,
    COUNT(DISTINCT branch_city)           AS distinct_cities,
    COLLECT_LIST(DISTINCT branch_city)    AS cities_list
  FROM affected_txns
  WHERE branch_city IS NOT NULL
  GROUP BY customer_id, CAST(DATE(txn_date) AS DATE)
  HAVING COUNT(DISTINCT branch_city) >= 3
),

-- 3a. AMOUNT_SPIKE baseline: 30-day rolling avg per customer per day
-- Using all 90 days of affected_txns so the window has prior-day context.
-- RANGE BETWEEN 30 PRECEDING AND 1 PRECEDING excludes same-day transactions
-- to avoid the spiking txn inflating its own baseline.
customer_30d_avg AS (
  SELECT
    customer_id,
    CAST(DATE(txn_date) AS DATE)          AS txn_day,
    AVG(amount) OVER (
      PARTITION BY customer_id
      ORDER BY CAST(DATE(txn_date) AS DATE)
      RANGE BETWEEN 30 PRECEDING AND 1 PRECEDING
    )                                     AS avg_30d_amount
  FROM affected_txns
),

-- 3b. Flag transactions that exceed 3× the customer's own baseline
amount_spike_raw AS (
  SELECT
    t.customer_id,
    t.transaction_id,
    t.amount,
    CAST(DATE(t.txn_date) AS DATE)        AS alert_date,
    ca.avg_30d_amount,
    t.amount / NULLIF(ca.avg_30d_amount, 0) AS spike_ratio
  FROM affected_txns t
  JOIN customer_30d_avg ca
    ON  t.customer_id                      = ca.customer_id
    AND CAST(DATE(t.txn_date) AS DATE)     = ca.txn_day
  WHERE t.amount      > ca.avg_30d_amount * 3
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

-- UNION ALL three types into a single result set
SELECT
  v.customer_id,
  'VELOCITY_SPIKE'                        AS alert_type,
  v.alert_date,
  TO_JSON(NAMED_STRUCT(
    'window_start',        CAST(v.window_start AS STRING),
    'txn_count',           v.txn_count,
    'window_total_amount', ROUND(v.window_total_amount, 2),
    'min_amount',          ROUND(v.min_amount, 2),
    'max_amount',          ROUND(v.max_amount, 2)
  ))                                      AS details_json,
  CURRENT_TIMESTAMP()                     AS computed_at
FROM velocity_alerts v

UNION ALL

SELECT
  g.customer_id,
  'GEO_ANOMALY'                           AS alert_type,
  g.alert_date,
  TO_JSON(NAMED_STRUCT(
    'distinct_cities', g.distinct_cities,
    'cities',          CAST(g.cities_list AS STRING)
  ))                                      AS details_json,
  CURRENT_TIMESTAMP()                     AS computed_at
FROM geo_alerts g

UNION ALL

SELECT
  a.customer_id,
  'AMOUNT_SPIKE'                          AS alert_type,
  a.alert_date,
  TO_JSON(NAMED_STRUCT(
    'spike_txn_count',     a.spike_txn_count,
    'max_spike_amount',    ROUND(a.max_spike_amount, 2),
    'max_spike_ratio',     ROUND(a.max_spike_ratio, 2),
    'baseline_avg_amount', ROUND(a.baseline_avg_amount, 2)
  ))                                      AS details_json,
  CURRENT_TIMESTAMP()                     AS computed_at
FROM amount_alerts a;


-- ── Step 4: MERGE incremental result into the Gold table ───────────────────
-- Composite key: (customer_id, alert_type, alert_date)
-- A customer can trigger the same alert type on different days → NOT MATCHED.
-- If today's run recomputes an existing (customer, type, day) → MATCHED UPDATE
-- to refresh details_json (e.g. txn_count grew within the same hour bucket).

MERGE INTO negarabank.gold.fraud_detection_alerts AS target
USING fraud_alerts_incremental AS source
ON  target.customer_id = source.customer_id
AND target.alert_type  = source.alert_type
AND target.alert_date  = source.alert_date

WHEN MATCHED THEN UPDATE SET
  details_json = source.details_json,
  computed_at  = source.computed_at

WHEN NOT MATCHED THEN INSERT *;
