-- =============================================================================
-- dim_date: synthetic calendar dimension
-- full_refresh — generates 2018-01-01 through 2030-12-31 on every run
-- No source table dependency; derived purely from a date sequence.
-- =============================================================================
CREATE OR REPLACE TABLE negarabank.gold.dim_date
USING DELTA
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
)
AS
SELECT
  d                                                         AS date_key,
  YEAR(d)                                                   AS year,
  QUARTER(d)                                                AS quarter,
  MONTH(d)                                                  AS month,
  DAYOFMONTH(d)                                             AS day,
  DAYOFWEEK(d)                                              AS day_of_week,    -- 1=Sun … 7=Sat
  DATE_FORMAT(d, 'EEEE')                                    AS day_name,
  DATE_FORMAT(d, 'MMMM')                                    AS month_name,
  DATE_TRUNC('week',  d)                                    AS week_start,
  DATE_TRUNC('month', d)                                    AS month_start,
  DATE_TRUNC('quarter', d)                                  AS quarter_start,
  CASE WHEN DAYOFWEEK(d) IN (1, 7) THEN TRUE ELSE FALSE END AS is_weekend,
  CONCAT(YEAR(d), '-Q', QUARTER(d))                         AS year_quarter,
  DATE_FORMAT(d, 'yyyy-MM')                                 AS year_month
FROM (
  SELECT EXPLODE(SEQUENCE(
    DATE '2018-01-01',
    DATE '2030-12-31',
    INTERVAL 1 DAY
  )) AS d
)
