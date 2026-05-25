-- account_daily_snapshot: daily transaction summary per account
-- Output of the Q1 optimized pipeline; used as feature input for credit scoring
-- Grain: one row per (account_id, snapshot_date)
--
-- Incremental: only processes accounts that have new transactions since last run.
-- Joins fact_transactions (activity) with dim_account (current account state).
WITH new_activity AS (
  SELECT
    t.account_id,
    CAST(DATE(t.txn_date) AS DATE)                          AS snapshot_date,
    COUNT(*)                                                 AS txn_count,
    COUNT(CASE WHEN UPPER(t.status) = 'DEBIT'  THEN 1 END) AS debit_count,
    COUNT(CASE WHEN UPPER(t.status) = 'CREDIT' THEN 1 END) AS credit_count,
    COUNT(CASE WHEN UPPER(t.status) = 'FAILED' THEN 1 END) AS failed_count,
    ROUND(SUM(CASE WHEN UPPER(t.status) = 'DEBIT'  THEN t.amount ELSE 0 END), 2) AS total_debit_amount,
    ROUND(SUM(CASE WHEN UPPER(t.status) = 'CREDIT' THEN t.amount ELSE 0 END), 2) AS total_credit_amount,
    ROUND(SUM(t.amount), 2)                                  AS total_amount,
    ROUND(AVG(t.amount), 2)                                  AS avg_txn_amount,
    ROUND(MAX(t.amount), 2)                                  AS max_txn_amount
  FROM negarabank.gold.fact_transactions t
  WHERE t.computed_at > '${watermark_ts}'
  GROUP BY t.account_id, CAST(DATE(t.txn_date) AS DATE)
)

MERGE INTO negarabank.gold.account_daily_snapshot AS target
USING (
  SELECT
    a.account_id,
    a.customer_id,
    a.account_type,
    a.status                  AS account_status,
    a.credit_limit,
    n.snapshot_date,
    n.txn_count,
    n.debit_count,
    n.credit_count,
    n.failed_count,
    n.total_debit_amount,
    n.total_credit_amount,
    n.total_amount,
    n.avg_txn_amount,
    n.max_txn_amount,
    -- net flow for the day: positive = net inflow, negative = net outflow
    ROUND(n.total_credit_amount - n.total_debit_amount, 2)  AS net_flow,
    -- utilization only meaningful for credit accounts
    ROUND(
      CASE WHEN UPPER(a.account_type) = 'CREDIT' AND a.credit_limit > 0
           THEN n.total_debit_amount / a.credit_limit * 100
           ELSE NULL
      END, 2
    )                                                        AS daily_utilization_pct,
    CURRENT_TIMESTAMP()                                      AS computed_at
  FROM new_activity n
  JOIN negarabank.gold.dim_account a ON n.account_id = a.account_id
) AS source
ON  target.account_id    = source.account_id
AND target.snapshot_date = source.snapshot_date

WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
