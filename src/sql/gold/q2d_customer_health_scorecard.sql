-- Q2d: Monthly customer health scorecard, one row per (customer_id, report_month)
-- Incremental: only recomputes months where Silver has new rows since last run
-- LAG window needs all months per affected customer, so we scope by customer_id first
-- then re-join to affected_keys before the MERGE to avoid writing unchanged rows
WITH

affected_keys AS (
  SELECT DISTINCT customer_id, DATE_TRUNC('month', COALESCE(effective_start_date, load_date)) AS report_month
  FROM negarabank.silver.accounts
  WHERE load_timestamp > '${watermark_ts}'
  UNION
  SELECT DISTINCT customer_id, DATE_TRUNC('month', txn_date) AS report_month
  FROM negarabank.silver.transactions
  WHERE load_timestamp > '${watermark_ts}'
),

account_monthly AS (
  SELECT
    a.customer_id,
    DATE_TRUNC('month', COALESCE(a.effective_start_date, a.load_date))  AS report_month,
    SUM(a.balance)                                                        AS total_balance,
    SUM(CASE WHEN UPPER(a.account_type) = 'CREDIT' THEN a.balance      ELSE 0    END) AS credit_balance,
    SUM(CASE WHEN UPPER(a.account_type) = 'CREDIT' THEN a.credit_limit ELSE NULL END) AS total_credit_limit
  FROM negarabank.silver.accounts a
  WHERE a.is_current  = true
    AND a.customer_id IN (SELECT DISTINCT customer_id FROM affected_keys)
  GROUP BY a.customer_id, DATE_TRUNC('month', COALESCE(a.effective_start_date, a.load_date))
),

mom_balance AS (
  SELECT
    customer_id,
    report_month,
    total_balance,
    credit_balance,
    total_credit_limit,
    LAG(total_balance) OVER (PARTITION BY customer_id ORDER BY report_month) AS prev_month_balance
  FROM account_monthly
),

txn_monthly AS (
  SELECT
    t.customer_id,
    DATE_TRUNC('month', t.txn_date)                                    AS report_month,
    COUNT(CASE WHEN UPPER(t.status) = 'DEBIT'   THEN 1 END)           AS debit_count,
    COUNT(CASE WHEN UPPER(t.status) = 'CREDIT'  THEN 1 END)           AS credit_count,
    COUNT(CASE WHEN UPPER(t.status) = 'PENDING' THEN 1 END)           AS pending_count,
    COUNT(CASE WHEN UPPER(t.status) = 'FAILED'  THEN 1 END)           AS failed_count,
    COUNT(*)                                                            AS total_txn_count,
    AVG(CASE WHEN UPPER(t.channel) = 'ONLINE' THEN t.amount END)      AS avg_online_amount,
    AVG(CASE WHEN UPPER(t.channel) = 'BRANCH' THEN t.amount END)      AS avg_branch_amount,
    AVG(CASE WHEN UPPER(t.channel) = 'ATM'    THEN t.amount END)      AS avg_atm_amount,
    AVG(CASE WHEN UPPER(t.channel) = 'MOBILE' THEN t.amount END)      AS avg_mobile_amount
  FROM negarabank.silver.transactions t
  WHERE t.is_current  = true
    AND t.customer_id IN (SELECT DISTINCT customer_id FROM affected_keys)
  GROUP BY t.customer_id, DATE_TRUNC('month', t.txn_date)
),

latest_credit_score AS (
  SELECT customer_id, score AS credit_score, probability_of_default
  FROM negarabank.bronze.credit_scores
  QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY load_timestamp DESC) = 1
)

MERGE INTO negarabank.gold.customer_health_scorecard AS target
USING (
  SELECT
    mb.customer_id,
    mb.report_month,
    ROUND(mb.total_balance, 2)                                                        AS total_balance,
    ROUND(mb.prev_month_balance, 2)                                                   AS prev_month_balance,
    ROUND((mb.total_balance - mb.prev_month_balance) / NULLIF(mb.prev_month_balance, 0) * 100, 2) AS mom_balance_change_pct,
    COALESCE(mt.debit_count,     0)                                                   AS debit_count,
    COALESCE(mt.credit_count,    0)                                                   AS credit_count,
    COALESCE(mt.pending_count,   0)                                                   AS pending_count,
    COALESCE(mt.failed_count,    0)                                                   AS failed_count,
    COALESCE(mt.total_txn_count, 0)                                                   AS total_txn_count,
    ROUND(mt.avg_online_amount, 2)                                                    AS avg_online_amount,
    ROUND(mt.avg_branch_amount, 2)                                                    AS avg_branch_amount,
    ROUND(mt.avg_atm_amount, 2)                                                       AS avg_atm_amount,
    ROUND(mt.avg_mobile_amount, 2)                                                    AS avg_mobile_amount,
    ROUND(mb.credit_balance / NULLIF(mb.total_credit_limit, 0) * 100, 2)             AS credit_utilization_pct,
    cs.credit_score,
    cs.probability_of_default,
    CASE WHEN
         (mb.credit_balance / NULLIF(mb.total_credit_limit, 0)) > 0.80
      OR  cs.probability_of_default > 0.3
      OR  (mb.total_balance - mb.prev_month_balance) / NULLIF(mb.prev_month_balance, 0) < -0.30
    THEN true ELSE false END                                                           AS risk_flag,
    CURRENT_TIMESTAMP()                                                                AS computed_at
  FROM mom_balance mb
  JOIN affected_keys ak  ON mb.customer_id = ak.customer_id AND mb.report_month = ak.report_month
  LEFT JOIN txn_monthly mt ON mb.customer_id = mt.customer_id AND mb.report_month = mt.report_month
  LEFT JOIN latest_credit_score cs ON mb.customer_id = cs.customer_id
) AS source
ON  target.customer_id  = source.customer_id
AND target.report_month = source.report_month

WHEN MATCHED THEN UPDATE SET
  total_balance = source.total_balance, prev_month_balance = source.prev_month_balance,
  mom_balance_change_pct = source.mom_balance_change_pct,
  debit_count = source.debit_count, credit_count = source.credit_count,
  pending_count = source.pending_count, failed_count = source.failed_count,
  total_txn_count = source.total_txn_count,
  avg_online_amount = source.avg_online_amount, avg_branch_amount = source.avg_branch_amount,
  avg_atm_amount = source.avg_atm_amount, avg_mobile_amount = source.avg_mobile_amount,
  credit_utilization_pct = source.credit_utilization_pct,
  credit_score = source.credit_score, probability_of_default = source.probability_of_default,
  risk_flag = source.risk_flag, computed_at = source.computed_at

WHEN NOT MATCHED THEN INSERT *
