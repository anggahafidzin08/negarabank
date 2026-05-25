-- fact_transactions: one row per transaction, incremental merge on transaction_id
-- Status updates are possible (PENDING -> DEBIT) so we update on status change
WITH new_transactions AS (
  SELECT
    transaction_id,
    customer_id,
    account_id,
    ROUND(amount, 2)               AS amount,
    CAST(DATE(txn_date) AS DATE)   AS txn_date,
    status,
    reconciled,
    CURRENT_TIMESTAMP()            AS computed_at
  FROM negarabank.silver.transactions
  WHERE is_current     = true
    AND load_timestamp > '${watermark_ts}'
)

MERGE INTO negarabank.gold.fact_transactions AS target
USING new_transactions AS source
ON target.transaction_id = source.transaction_id

WHEN MATCHED AND target.status != source.status THEN UPDATE SET
  status      = source.status,
  reconciled  = source.reconciled,
  computed_at = source.computed_at

WHEN NOT MATCHED THEN INSERT *
