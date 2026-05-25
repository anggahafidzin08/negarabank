-- =============================================================================
-- Q2d: Monthly Customer Health Scorecard — Incremental MERGE
-- OJK regulatory report: one row per customer per month
-- Optimized for Databricks Delta Lake on 8M accounts / 60M+ transactions
--
-- Incremental strategy:
--   1. Detect which (customer_id, report_month) pairs have new Silver data
--      since the last Gold run by reading MAX(computed_at) from the target table.
--   2. Recompute ONLY those pairs (partition pruning via load_timestamp filter).
--   3. MERGE the recomputed rows into the Gold table:
--        - MATCHED → UPDATE all metrics columns
--        - NOT MATCHED → INSERT (new customer or new month)
--
-- Why not full rebuild?
--   Full CTAS scans all Silver history every night. For 8M accounts across
--   24+ months of history that is 192M+ Silver rows read to produce the same
--   Gold rows that did not change. The incremental filter reduces the nightly
--   scan to only rows touched in the last load window (typically < 1% of data).
--
-- Performance notes:
--   - Silver tables Z-ordered by customer_id; load_timestamp filter prunes
--     partitions (Silver is partitioned by load_date).
--   - NULLIF guards prevent divide-by-zero on credit_limit / prev_balance.
--   - Conditional aggregation replaces PIVOT for portability.
-- =============================================================================

-- ── Step 1: Resolve the incremental watermark ──────────────────────────────
-- MAX(computed_at) from the existing Gold table tells us the last time any row
-- was written. We reprocess all Silver rows newer than that timestamp.
-- On first run the table does not exist, so the watermark falls back to epoch
-- (1970-01-01) which triggers a full backfill — equivalent to the old CTAS.

-- Step 1 is handled in the notebook wrapper (08_build_customer_health_scorecard.py)
-- which calls spark.sql() with the watermark injected as a parameter.
-- The SQL below uses ${watermark_ts} as a placeholder; the notebook substitutes it.
--
-- For manual / ad-hoc runs set: SET watermark_ts = '1970-01-01 00:00:00';

-- ── Step 2: Identify affected (customer_id, report_month) pairs ────────────
-- Any Silver account row updated after the watermark means the whole
-- report_month for that customer must be recomputed (balance aggregates change).

CREATE OR REPLACE TEMP VIEW affected_keys AS
SELECT DISTINCT
  a.customer_id,
  DATE_TRUNC('month', COALESCE(a.effective_start_date, a.load_date)) AS report_month
FROM negarabank.silver.accounts a
WHERE a.load_timestamp > '${watermark_ts}'   -- only rows new since last Gold run

UNION

SELECT DISTINCT
  t.customer_id,
  DATE_TRUNC('month', t.txn_date)            AS report_month
FROM negarabank.silver.transactions t
WHERE t.load_timestamp > '${watermark_ts}';


-- ── Step 3: Recompute metrics for affected keys only ───────────────────────
CREATE OR REPLACE TEMP VIEW scorecard_incremental AS

WITH

account_monthly AS (
  SELECT
    a.customer_id,
    DATE_TRUNC('month', COALESCE(a.effective_start_date, a.load_date)) AS report_month,
    SUM(a.balance)                                                       AS total_balance,
    SUM(CASE WHEN UPPER(a.account_type) = 'CREDIT' THEN a.balance      ELSE 0    END) AS credit_balance,
    SUM(CASE WHEN UPPER(a.account_type) = 'CREDIT' THEN a.credit_limit ELSE NULL END) AS total_credit_limit
  FROM negarabank.silver.accounts a
  -- Semi-join: only compute months that were touched by new Silver data
  WHERE a.is_current = true
    AND EXISTS (
      SELECT 1 FROM affected_keys k
      WHERE k.customer_id  = a.customer_id
        AND k.report_month = DATE_TRUNC('month', COALESCE(a.effective_start_date, a.load_date))
    )
  GROUP BY a.customer_id, DATE_TRUNC('month', COALESCE(a.effective_start_date, a.load_date))
),

mom_balance AS (
  SELECT
    customer_id,
    report_month,
    total_balance,
    credit_balance,
    total_credit_limit,
    LAG(total_balance) OVER (
      PARTITION BY customer_id
      ORDER BY report_month
    ) AS prev_month_balance
  FROM (
    -- Include the prior month's balance even if it wasn't in affected_keys,
    -- so LAG() has a value to compare against.
    SELECT
      a.customer_id,
      DATE_TRUNC('month', COALESCE(a.effective_start_date, a.load_date)) AS report_month,
      SUM(a.balance)                                                       AS total_balance,
      SUM(CASE WHEN UPPER(a.account_type) = 'CREDIT' THEN a.balance      ELSE 0    END) AS credit_balance,
      SUM(CASE WHEN UPPER(a.account_type) = 'CREDIT' THEN a.credit_limit ELSE NULL END) AS total_credit_limit
    FROM negarabank.silver.accounts a
    WHERE a.is_current = true
      AND a.customer_id IN (SELECT DISTINCT customer_id FROM affected_keys)
    GROUP BY a.customer_id, DATE_TRUNC('month', COALESCE(a.effective_start_date, a.load_date))
  )
),

txn_monthly AS (
  SELECT
    t.customer_id,
    DATE_TRUNC('month', t.txn_date)                                     AS report_month,
    COUNT(CASE WHEN UPPER(t.status) = 'DEBIT'   THEN 1 END)            AS debit_count,
    COUNT(CASE WHEN UPPER(t.status) = 'CREDIT'  THEN 1 END)            AS credit_count,
    COUNT(CASE WHEN UPPER(t.status) = 'PENDING' THEN 1 END)            AS pending_count,
    COUNT(CASE WHEN UPPER(t.status) = 'FAILED'  THEN 1 END)            AS failed_count,
    COUNT(*)                                                             AS total_txn_count,
    AVG(CASE WHEN UPPER(t.channel) = 'ONLINE'  THEN t.amount END)      AS avg_online_amount,
    AVG(CASE WHEN UPPER(t.channel) = 'BRANCH'  THEN t.amount END)      AS avg_branch_amount,
    AVG(CASE WHEN UPPER(t.channel) = 'ATM'     THEN t.amount END)      AS avg_atm_amount,
    AVG(CASE WHEN UPPER(t.channel) = 'MOBILE'  THEN t.amount END)      AS avg_mobile_amount,
    AVG(t.amount)                                                        AS avg_txn_amount
  FROM negarabank.silver.transactions t
  WHERE t.is_current = true
    AND t.customer_id IN (SELECT DISTINCT customer_id FROM affected_keys)
  GROUP BY t.customer_id, DATE_TRUNC('month', t.txn_date)
),

latest_credit_score AS (
  SELECT
    customer_id,
    score                    AS credit_score,
    probability_of_default
  FROM negarabank.bronze.credit_scores
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY customer_id
    ORDER BY load_timestamp DESC
  ) = 1
)

SELECT
  mb.customer_id,
  mb.report_month,
  ROUND(mb.total_balance,      2)                                        AS total_balance,
  ROUND(mb.prev_month_balance, 2)                                        AS prev_month_balance,
  ROUND(
    (mb.total_balance - mb.prev_month_balance)
      / NULLIF(mb.prev_month_balance, 0) * 100,
    2
  )                                                                       AS mom_balance_change_pct,
  COALESCE(mt.debit_count,   0)                                          AS debit_count,
  COALESCE(mt.credit_count,  0)                                          AS credit_count,
  COALESCE(mt.pending_count, 0)                                          AS pending_count,
  COALESCE(mt.failed_count,  0)                                          AS failed_count,
  COALESCE(mt.total_txn_count, 0)                                        AS total_txn_count,
  ROUND(mt.avg_online_amount,  2)                                        AS avg_online_amount,
  ROUND(mt.avg_branch_amount,  2)                                        AS avg_branch_amount,
  ROUND(mt.avg_atm_amount,     2)                                        AS avg_atm_amount,
  ROUND(mt.avg_mobile_amount,  2)                                        AS avg_mobile_amount,
  ROUND(
    mb.credit_balance / NULLIF(mb.total_credit_limit, 0) * 100,
    2
  )                                                                       AS credit_utilization_pct,
  cs.credit_score,
  cs.probability_of_default,
  CASE WHEN
       (mb.credit_balance / NULLIF(mb.total_credit_limit, 0)) > 0.80
    OR  cs.probability_of_default > 0.3
    OR  (mb.total_balance - mb.prev_month_balance)
          / NULLIF(mb.prev_month_balance, 0) < -0.30
  THEN TRUE ELSE FALSE
  END                                                                     AS risk_flag,
  CURRENT_TIMESTAMP()                                                     AS computed_at

FROM mom_balance mb
-- Filter back to only the affected keys (the LAG window needed the broader set above)
JOIN affected_keys ak
  ON  mb.customer_id  = ak.customer_id
  AND mb.report_month = ak.report_month
LEFT JOIN txn_monthly        mt ON mb.customer_id = mt.customer_id
                                AND mb.report_month = mt.report_month
LEFT JOIN latest_credit_score cs ON mb.customer_id = cs.customer_id;


-- ── Step 4: MERGE incremental result into the Gold table ───────────────────
-- Target table is created on first run by the notebook; subsequent runs MERGE.

MERGE INTO negarabank.gold.customer_health_scorecard AS target
USING scorecard_incremental AS source
ON  target.customer_id  = source.customer_id
AND target.report_month = source.report_month

WHEN MATCHED THEN UPDATE SET
  total_balance           = source.total_balance,
  prev_month_balance      = source.prev_month_balance,
  mom_balance_change_pct  = source.mom_balance_change_pct,
  debit_count             = source.debit_count,
  credit_count            = source.credit_count,
  pending_count           = source.pending_count,
  failed_count            = source.failed_count,
  total_txn_count         = source.total_txn_count,
  avg_online_amount       = source.avg_online_amount,
  avg_branch_amount       = source.avg_branch_amount,
  avg_atm_amount          = source.avg_atm_amount,
  avg_mobile_amount       = source.avg_mobile_amount,
  credit_utilization_pct  = source.credit_utilization_pct,
  credit_score            = source.credit_score,
  probability_of_default  = source.probability_of_default,
  risk_flag               = source.risk_flag,
  computed_at             = source.computed_at

WHEN NOT MATCHED THEN INSERT *;
