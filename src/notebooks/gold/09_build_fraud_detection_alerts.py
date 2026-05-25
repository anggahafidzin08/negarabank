"""
Gold layer: Fraud Detection Alerts (Q2e) — incremental MERGE.

Watermark pattern:
  1. Read MAX(computed_at) from the existing Gold table.
  2. Inject as ${watermark_ts} SQL variable — Silver filter scopes to customers
     with new transactions since the last successful Gold run.
  3. On first run (table absent) watermark falls back to epoch → full backfill.
  4. Execute incremental SQL (affected_customers → full context → MERGE INTO Gold).
"""
from pathlib import Path

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

GOLD_TABLE = "negarabank.gold.fraud_detection_alerts"

# ── 1. Resolve watermark ───────────────────────────────────────────────────
if spark.catalog.tableExists(GOLD_TABLE):
    row = spark.sql(f"SELECT MAX(computed_at) FROM {GOLD_TABLE}").collect()[0][0]
    watermark_ts = str(row) if row else "1970-01-01 00:00:00"
else:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {GOLD_TABLE} (
            customer_id  BIGINT,
            alert_type   STRING,
            alert_date   DATE,
            details_json STRING,
            computed_at  TIMESTAMP
        )
        USING DELTA
        PARTITIONED BY (alert_date)
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
sql_path = Path(__file__).parent.parent.parent / "sql" / "gold" / "q2e_fraud_detection_alerts.sql"
spark.sql(sql_path.read_text())

# ── 4. Report and optimize ─────────────────────────────────────────────────
count = spark.table(GOLD_TABLE).count()
print(f"{GOLD_TABLE}: {count:,} total rows after MERGE")

spark.sql(f"OPTIMIZE {GOLD_TABLE} ZORDER BY (customer_id, alert_type)")
print("OPTIMIZE complete")
