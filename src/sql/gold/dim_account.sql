-- dim_account: current account snapshot from Silver
CREATE OR REPLACE TABLE negarabank.gold.dim_account
USING DELTA
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
)
AS
SELECT
  account_id,
  customer_id,
  account_type,
  status,
  CAST(open_date AS DATE)             AS open_date,
  CAST(credit_limit AS DECIMAL(15,2)) AS credit_limit,
  effective_start_date,
  load_date,
  load_timestamp
FROM negarabank.silver.accounts
WHERE is_current = true
