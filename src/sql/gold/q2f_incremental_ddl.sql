-- =============================================================================
-- Q2f: Incremental Refresh Optimization Strategy
-- Demonstrates three production-grade incremental patterns for the Gold layer
-- targeting an 8M-account / 60M+ transaction dataset on Databricks.
--
-- PATTERN SELECTION GUIDE
-- ─────────────────────────────────────────────────────────────────────────────
-- | Pattern              | Latency   | Complexity | Best for                 |
-- |----------------------|-----------|------------|--------------------------|
-- | A. dbt incremental   | Batch     | Low        | Batch ETL, team knows SQL|
-- | B. DLT APPLY CHANGES | Near-RT   | Medium     | CDC / append-only Bronze |
-- | C. Materialized View | On-demand | Very low   | BI dashboards, ad hoc    |
-- ─────────────────────────────────────────────────────────────────────────────
--
-- For the NegaraBank nightly batch pipeline, PATTERN A is recommended:
--   - Aligns with existing 2 AM batch window
--   - dbt handles MERGE logic, lineage docs, and DQ tests
--   - Zero streaming infrastructure overhead
-- =============================================================================


-- ═══════════════════════════════════════════════════════════════════════════════
-- PATTERN A: dbt Incremental Model (recommended for batch ETL)
-- File: models/gold/customer_health_scorecard.sql
-- Config block tells dbt to generate a MERGE statement instead of full refresh
-- ═══════════════════════════════════════════════════════════════════════════════

-- dbt model config (JINJA block — not plain SQL, shown for reference)
--
-- {{ config(
--     materialized   = 'incremental',
--     unique_key     = ['customer_id', 'report_month'],
--     incremental_strategy = 'merge',
--     merge_update_columns = [
--         'total_balance', 'prev_month_balance', 'mom_balance_change_pct',
--         'debit_count', 'credit_count', 'pending_count', 'failed_count',
--         'total_txn_count', 'credit_utilization_pct',
--         'credit_score', 'probability_of_default', 'risk_flag', 'computed_at'
--     ],
--     partition_by   = {'field': 'report_month', 'data_type': 'date'},
--     file_format    = 'delta',
--     post_hook      = "OPTIMIZE {{ this }} ZORDER BY (customer_id)"
-- ) }}

-- Core query — dbt injects the WHERE clause when running incrementally:
--
-- WITH account_monthly AS (
--   SELECT ...
--   FROM {{ ref('silver_accounts') }}
--   {% if is_incremental() %}
--   WHERE DATE_TRUNC('month', COALESCE(effective_start_date, load_date))
--         >= DATE_TRUNC('month', DATEADD(MONTH, -1, CURRENT_DATE()))
--   {% endif %}
-- ), ...

-- Plain-SQL equivalent of the incremental MERGE dbt generates at run time:
MERGE INTO negarabank.gold.customer_health_scorecard AS target
USING (
  -- Recompute only the last two report months (current + prior for MoM accuracy)
  SELECT
    mb.customer_id,
    mb.report_month,
    ROUND(mb.total_balance, 2)                                         AS total_balance,
    ROUND(mb.prev_month_balance, 2)                                    AS prev_month_balance,
    ROUND((mb.total_balance - mb.prev_month_balance)
          / NULLIF(mb.prev_month_balance, 0) * 100, 2)                AS mom_balance_change_pct,
    COALESCE(mt.debit_count,   0)                                      AS debit_count,
    COALESCE(mt.credit_count,  0)                                      AS credit_count,
    COALESCE(mt.pending_count, 0)                                      AS pending_count,
    COALESCE(mt.failed_count,  0)                                      AS failed_count,
    COALESCE(mt.total_txn_count, 0)                                    AS total_txn_count,
    ROUND(mb.credit_balance / NULLIF(mb.total_credit_limit, 0) * 100, 2) AS credit_utilization_pct,
    cs.credit_score,
    cs.probability_of_default,
    CASE WHEN
         (mb.credit_balance / NULLIF(mb.total_credit_limit, 0)) > 0.80
      OR  cs.probability_of_default > 0.3
      OR  (mb.total_balance - mb.prev_month_balance)
            / NULLIF(mb.prev_month_balance, 0) < -0.30
    THEN TRUE ELSE FALSE END                                           AS risk_flag,
    CURRENT_TIMESTAMP()                                                AS computed_at
  FROM (
    SELECT
      customer_id,
      DATE_TRUNC('month', COALESCE(effective_start_date, load_date)) AS report_month,
      SUM(balance)                                                    AS total_balance,
      SUM(CASE WHEN UPPER(account_type) = 'CREDIT' THEN balance      ELSE 0    END) AS credit_balance,
      SUM(CASE WHEN UPPER(account_type) = 'CREDIT' THEN credit_limit ELSE NULL END) AS total_credit_limit,
      LAG(SUM(balance)) OVER (
        PARTITION BY customer_id
        ORDER BY DATE_TRUNC('month', COALESCE(effective_start_date, load_date))
      )                                                               AS prev_month_balance
    FROM negarabank.silver.accounts
    WHERE is_current = true
      -- Incremental filter: only accounts active in the last 2 months
      AND COALESCE(effective_start_date, load_date)
          >= DATEADD(MONTH, -2, DATE_TRUNC('month', CURRENT_DATE()))
    GROUP BY customer_id,
             DATE_TRUNC('month', COALESCE(effective_start_date, load_date))
  ) mb
  LEFT JOIN (
    SELECT
      customer_id,
      DATE_TRUNC('month', txn_date)                                   AS report_month,
      COUNT(CASE WHEN UPPER(status) = 'DEBIT'   THEN 1 END)          AS debit_count,
      COUNT(CASE WHEN UPPER(status) = 'CREDIT'  THEN 1 END)          AS credit_count,
      COUNT(CASE WHEN UPPER(status) = 'PENDING' THEN 1 END)          AS pending_count,
      COUNT(CASE WHEN UPPER(status) = 'FAILED'  THEN 1 END)          AS failed_count,
      COUNT(*)                                                         AS total_txn_count
    FROM negarabank.silver.transactions
    WHERE is_current = true
      AND txn_date >= DATEADD(MONTH, -2, DATE_TRUNC('month', CURRENT_DATE()))
    GROUP BY customer_id, DATE_TRUNC('month', txn_date)
  ) mt ON mb.customer_id = mt.customer_id AND mb.report_month = mt.report_month
  LEFT JOIN (
    SELECT customer_id, score AS credit_score, probability_of_default
    FROM negarabank.bronze.credit_scores
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY load_timestamp DESC) = 1
  ) cs ON mb.customer_id = cs.customer_id
) AS source

ON  target.customer_id  = source.customer_id
AND target.report_month = source.report_month

WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;


-- ═══════════════════════════════════════════════════════════════════════════════
-- PATTERN B: Delta Live Tables (DLT) — APPLY CHANGES INTO for CDC
-- Suited for near-real-time refresh when Bronze receives CDC from Debezium/Kafka
-- ═══════════════════════════════════════════════════════════════════════════════

-- DLT pipeline definition (Python API — shown as SQL-style pseudocode)
-- Requires: DLT pipeline in Databricks workspace, not a standalone SQL script

/*
CREATE OR REFRESH STREAMING TABLE silver_accounts_cdc
COMMENT 'SCD1 view of accounts — latest state only (DLT CDC pattern)'
AS APPLY CHANGES INTO negarabank.silver.accounts_cdc
FROM STREAM(negarabank.bronze.accounts_raw_cdc)
KEYS (account_id)
SEQUENCE BY load_timestamp
STORED AS SCD TYPE 1;
*/

-- For SCD2 (full history), use TYPE 2:
/*
CREATE OR REFRESH STREAMING TABLE silver_accounts_history
COMMENT 'SCD2 full history of accounts'
AS APPLY CHANGES INTO negarabank.silver.accounts_history
FROM STREAM(negarabank.bronze.accounts_raw_cdc)
KEYS (account_id)
SEQUENCE BY load_timestamp
STORED AS SCD TYPE 2;
*/


-- ═══════════════════════════════════════════════════════════════════════════════
-- PATTERN C: Materialized View (Databricks SQL — on-demand refresh for BI)
-- Best for analyst-facing dashboards queried via Databricks SQL warehouses
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE MATERIALIZED VIEW negarabank.gold.mv_customer_risk_summary
COMMENT 'Pre-aggregated risk summary for BI dashboards — refreshed nightly'
AS
SELECT
  cs.customer_id,
  c.full_name,
  c.segment,
  c.risk_category,
  DATE_TRUNC('month', CURRENT_DATE())      AS snapshot_month,
  cs.credit_score,
  cs.probability_of_default,
  hs.total_balance,
  hs.credit_utilization_pct,
  hs.mom_balance_change_pct,
  hs.risk_flag,
  COUNT(fa.alert_type)                     AS open_alert_count,
  COLLECT_SET(fa.alert_type)               AS alert_types
FROM negarabank.gold.customer_health_scorecard hs
JOIN negarabank.silver.customers              c  ON hs.customer_id = c.customer_id
                                                 AND c.is_current = true
JOIN negarabank.bronze.credit_scores          cs ON hs.customer_id = cs.customer_id
LEFT JOIN negarabank.gold.fraud_detection_alerts fa
  ON  hs.customer_id  = fa.customer_id
  AND fa.alert_date  >= DATEADD(DAY, -30, CURRENT_DATE())
WHERE hs.report_month = DATE_TRUNC('month', DATEADD(MONTH, -1, CURRENT_DATE()))
GROUP BY
  cs.customer_id, c.full_name, c.segment, c.risk_category,
  cs.credit_score, cs.probability_of_default,
  hs.total_balance, hs.credit_utilization_pct,
  hs.mom_balance_change_pct, hs.risk_flag;


-- ═══════════════════════════════════════════════════════════════════════════════
-- PARTITION MAINTENANCE: OPTIMIZE + ZORDER (run after incremental loads)
-- Keep in a post-hook or a separate maintenance task in batch_etl_job.yml
-- ═══════════════════════════════════════════════════════════════════════════════

-- Run monthly to compact small files and re-order data for query pruning:
OPTIMIZE negarabank.gold.customer_health_scorecard
  ZORDER BY (customer_id);

OPTIMIZE negarabank.gold.fraud_detection_alerts
  ZORDER BY (customer_id, alert_type);

-- Vacuum old Delta versions after 7-day retention window:
VACUUM negarabank.gold.customer_health_scorecard RETAIN 168 HOURS;
VACUUM negarabank.gold.fraud_detection_alerts    RETAIN 168 HOURS;
