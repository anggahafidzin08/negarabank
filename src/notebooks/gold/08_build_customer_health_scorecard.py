"""
Gold layer: Monthly Customer Health Scorecard (Q2d)
Reads from negarabank.silver.{accounts,transactions} and negarabank.bronze.credit_scores.
Writes to negarabank.gold.customer_health_scorecard (partitioned by report_month).
"""
import os
from pathlib import Path

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

sql_path = Path(__file__).parent.parent.parent / "sql" / "gold" / "q2d_customer_health_scorecard.sql"
sql = sql_path.read_text()

spark.sql(sql)

count = spark.table("negarabank.gold.customer_health_scorecard").count()
print(f"customer_health_scorecard: {count:,} rows written")

spark.sql(
    "OPTIMIZE negarabank.gold.customer_health_scorecard ZORDER BY (customer_id)"
)
print("OPTIMIZE complete")
