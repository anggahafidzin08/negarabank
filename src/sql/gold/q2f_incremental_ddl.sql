-- =============================================================================
-- Q2f: Incremental Silver → Gold Transformation Strategy
--
-- Problem statement:
--   The Gold layer (customer_health_scorecard, fraud_detection_alerts) must
--   be refreshed nightly without scanning all Silver history on every run.
--   8M accounts × 24+ months = 192M+ Silver rows if done as a full rebuild.
--
-- Design decision: watermark-driven incremental MERGE
-- ─────────────────────────────────────────────────────────────────────────────
-- Silver tables carry three metadata columns written by the SCD2 transformer:
--
--   load_timestamp        TIMESTAMP  — when the row entered Silver (set by
--                                      bronze_extractor._add_metadata)
--   effective_start_date  DATE       — business date the version became active
--   is_current            BOOLEAN    — TRUE for the live version of a record
--
-- These are the control columns we use to detect what is new since the last
-- Gold run, avoiding a full table scan.
-- =============================================================================


-- ══════════════════════════════════════════════════════════════════════════════
-- WATERMARK PATTERN
--
-- Each Gold table self-documents when it was last computed via computed_at.
-- The notebook reads this value before running SQL and passes it as a parameter.
-- On first run (table does not exist) the notebook substitutes '1970-01-01',
-- triggering a full backfill through the same code path — no special-case logic.
-- ══════════════════════════════════════════════════════════════════════════════

-- Notebook reads watermark (Python, in 08_build_customer_health_scorecard.py):
--
--   if spark.catalog.tableExists("negarabank.gold.customer_health_scorecard"):
--       row = spark.sql(
--           "SELECT MAX(computed_at) FROM negarabank.gold.customer_health_scorecard"
--       ).collect()[0][0]
--       watermark_ts = str(row) if row else "1970-01-01 00:00:00"
--   else:
--       watermark_ts = "1970-01-01 00:00:00"   -- full backfill on first run
--   spark.sql(f"SET watermark_ts = '{watermark_ts}'")

-- The SQL then filters Silver using ${watermark_ts}:
--
--   WHERE load_timestamp > '${watermark_ts}'


-- ══════════════════════════════════════════════════════════════════════════════
-- WHY load_timestamp AND NOT load_date?
--
-- load_date is a partition column (DATE precision). Using it as a watermark
-- would force reprocessing an entire day's partitions even for a 1-row change.
-- load_timestamp has microsecond precision and is written by _add_metadata()
-- in the Bronze extractor at extraction time, then propagated through Silver
-- unchanged. It accurately reflects when each row entered the pipeline.
-- ══════════════════════════════════════════════════════════════════════════════


-- ══════════════════════════════════════════════════════════════════════════════
-- MERGE SEMANTICS FOR GOLD TABLES
--
-- Gold tables are not append-only: a customer's health score for March 2025
-- might change if a retroactive Silver correction arrives in May 2025.
-- We therefore MERGE rather than append:
--
--   MATCHED     → UPDATE all metric columns (recomputed value replaces old one)
--   NOT MATCHED → INSERT  (new customer or new month first seen)
--
-- This keeps the Gold table correct under late-arriving data without any manual
-- DELETE + re-INSERT logic.
-- ══════════════════════════════════════════════════════════════════════════════


-- ══════════════════════════════════════════════════════════════════════════════
-- AFFECTED-KEY SCOPING (why we can't just filter the final SELECT)
--
-- The health scorecard uses LAG() to compute MoM balance change.
-- If we filter account_monthly to load_timestamp > watermark before the LAG,
-- the prior month's balance is missing and prev_month_balance is always NULL.
--
-- Solution (see q2d_customer_health_scorecard.sql):
--   Step 1 — build affected_keys: (customer_id, report_month) pairs that have
--             new Silver rows since the watermark.
--   Step 2 — expand to full customer scope for the LAG window:
--             WHERE customer_id IN (SELECT customer_id FROM affected_keys)
--             This reads all months for those customers, giving LAG() its context.
--   Step 3 — filter back to affected_keys in the final JOIN before MERGE,
--             so we only write rows that actually changed.
--
-- This pattern reads O(customers_with_changes × months_per_customer) rows
-- rather than O(all_customers × all_months).
-- ══════════════════════════════════════════════════════════════════════════════


-- ══════════════════════════════════════════════════════════════════════════════
-- PARTITION MAINTENANCE (run as post-task in batch_etl_job.yml)
-- ══════════════════════════════════════════════════════════════════════════════

-- After each nightly MERGE, compact small files and re-order for query pruning.
-- Databricks autoOptimize handles write-time compaction; ZORDER improves reads:
OPTIMIZE negarabank.gold.customer_health_scorecard
  ZORDER BY (customer_id);

OPTIMIZE negarabank.gold.fraud_detection_alerts
  ZORDER BY (customer_id, alert_type);

-- Retain 7 days of Delta history (enough for rollback; reduces storage cost):
VACUUM negarabank.gold.customer_health_scorecard RETAIN 168 HOURS;
VACUUM negarabank.gold.fraud_detection_alerts    RETAIN 168 HOURS;


-- ══════════════════════════════════════════════════════════════════════════════
-- SUMMARY: incremental flow per nightly run
--
--   Bronze extractor  → writes new rows to Silver partitions (load_date = today)
--                        with load_timestamp = now()
--   Silver transformer → SCD2 MERGE: new/changed records get is_current = true,
--                        load_timestamp carried through from Bronze
--   Gold notebook     → reads MAX(computed_at) from Gold table  [watermark]
--                        builds affected_keys WHERE load_timestamp > watermark
--                        recomputes metrics for those keys
--                        MERGE INTO Gold (UPDATE existing, INSERT new)
--                        OPTIMIZE ZORDER (post-hook)
-- ══════════════════════════════════════════════════════════════════════════════
