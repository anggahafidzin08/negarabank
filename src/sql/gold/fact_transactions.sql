-- =============================================================================
-- fact_transactions: transaction-level fact table
-- incremental_merge — watermark ${watermark_ts} scopes new Silver rows
-- Grain: one row per transaction_id (immutable after settlement)
-- Joins Silver transactions to Gold dim tables for analytical keys.
-- =============================================================================
WITH
new_transactions AS (
  SELECT
    t.transaction_id,
    t.customer_id,
    t.account_id,
    ROUND(t.amount, 2)                  AS amount,
    CAST(DATE(t.txn_date) AS DATE)      AS txn_date,
    t.status,
    t.reconciled,
    CURRENT_TIMESTAMP()                 AS computed_at
  FROM negarabank.silver.transactions t
  WHERE t.is_current     = true
    AND t.load_timestamp > '${watermark_ts}'
)

MERGE INTO negarabank.gold.fact_transactions AS target
USING new_transactions AS source
ON target.transaction_id = source.transaction_id

-- Status can change (e.g. PENDING → DEBIT) via Silver SCD2; update when it does
WHEN MATCHED AND target.status != source.status THEN UPDATE SET
  status      = source.status,
  reconciled  = source.reconciled,
  computed_at = source.computed_at

WHEN NOT MATCHED THEN INSERT *
