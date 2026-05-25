-- dim_customer: current customer snapshot, one row per customer
CREATE OR REPLACE TABLE negarabank.gold.dim_customer
USING DELTA
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
)
AS
SELECT
  CAST(customer_id AS BIGINT)    AS customer_id,
  UPPER(TRIM(full_name))         AS full_name,
  LOWER(TRIM(email))             AS email,
  TRIM(phone)                    AS phone,
  UPPER(TRIM(segment))           AS segment,
  UPPER(TRIM(risk_category))     AS risk_category,
  CAST(date_of_birth AS DATE)    AS date_of_birth,
  UPPER(TRIM(nationality))       AS nationality,
  UPPER(TRIM(kyc_status))        AS kyc_status,
  load_date,
  load_timestamp
FROM negarabank.bronze.customers
QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY load_timestamp DESC) = 1
