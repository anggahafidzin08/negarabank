"""
Gold layer: Fraud Detection Alerts (Q2e)
Reads from negarabank.silver.{transactions,accounts}.
Writes to negarabank.gold.fraud_detection_alerts (partitioned by alert_date).
"""
from pathlib import Path

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

sql_path = Path(__file__).parent.parent.parent / "sql" / "gold" / "q2e_fraud_detection_alerts.sql"
sql = sql_path.read_text()

spark.sql(sql)

count = spark.table("negarabank.gold.fraud_detection_alerts").count()
print(f"fraud_detection_alerts: {count:,} rows written")

spark.sql(
    "OPTIMIZE negarabank.gold.fraud_detection_alerts ZORDER BY (customer_id, alert_type)"
)
print("OPTIMIZE complete")
