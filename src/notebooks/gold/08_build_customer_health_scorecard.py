"""
Gold layer: Monthly Customer Health Scorecard (Q2d) — incremental MERGE.

Watermark pattern:
  1. Read MAX(computed_at) from the existing Gold table.
  2. Inject as ${watermark_ts} SQL variable — Silver filter uses it to scope
     only rows that arrived after the last successful Gold run.
  3. On first run (table absent) watermark falls back to epoch → full backfill.
  4. Execute incremental SQL (affected_keys → recompute → MERGE INTO Gold).
"""
from pathlib import Path

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

GOLD_TABLE = "negarabank.gold.customer_health_scorecard"

# ── 1. Resolve watermark ───────────────────────────────────────────────────
if spark.catalog.tableExists(GOLD_TABLE):
    row = spark.sql(f"SELECT MAX(computed_at) FROM {GOLD_TABLE}").collect()[0][0]
    watermark_ts = str(row) if row else "1970-01-01 00:00:00"
else:
    # First run: create the table structure via a bootstrapped empty MERGE target
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {GOLD_TABLE} (
            customer_id             BIGINT,
            report_month            DATE,
            total_balance           DECIMAL(15,2),
            prev_month_balance      DECIMAL(15,2),
            mom_balance_change_pct  DECIMAL(10,2),
            debit_count             BIGINT,
            credit_count            BIGINT,
            pending_count           BIGINT,
            failed_count            BIGINT,
            total_txn_count         BIGINT,
            avg_online_amount       DECIMAL(15,2),
            avg_branch_amount       DECIMAL(15,2),
            avg_atm_amount          DECIMAL(15,2),
            avg_mobile_amount       DECIMAL(15,2),
            credit_utilization_pct  DECIMAL(10,2),
            credit_score            INT,
            probability_of_default  DECIMAL(5,4),
            risk_flag               BOOLEAN,
            computed_at             TIMESTAMP
        )
        USING DELTA
        PARTITIONED BY (report_month)
        TBLPROPERTIES (
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact'   = 'true'
        )
    """)
    watermark_ts = "1970-01-01 00:00:00"

print(f"Watermark: {watermark_ts}")

# ── 2. Inject watermark as SQL variable ────────────────────────────────────
spark.sql(f"SET watermark_ts = '{watermark_ts}'")

# ── 3. Execute incremental SQL ─────────────────────────────────────────────
sql_path = Path(__file__).parent.parent.parent / "sql" / "gold" / "q2d_customer_health_scorecard.sql"
spark.sql(sql_path.read_text())

# ── 4. Report and optimize ─────────────────────────────────────────────────
count = spark.table(GOLD_TABLE).count()
print(f"{GOLD_TABLE}: {count:,} total rows after MERGE")

spark.sql(f"OPTIMIZE {GOLD_TABLE} ZORDER BY (customer_id)")
print("OPTIMIZE complete")
