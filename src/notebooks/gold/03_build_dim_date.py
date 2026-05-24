# Databricks notebook source
# Gold Layer: Dimension - DATE

from pyspark.sql.functions import col, lit, year, month, dayofweek, to_date
from datetime import datetime, timedelta
from src.python.config import Paths
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("GoldDimDate")

# COMMAND ----------

# Generate date range (past 10 years + future 2 years)
start_date = datetime(2016, 1, 1)
end_date = datetime(2028, 12, 31)

# Create date dimension
dates = []
current_date = start_date
while current_date <= end_date:
    dates.append({
        "date": current_date,
        "date_key": int(current_date.strftime("%Y%m%d")),
        "year": current_date.year,
        "month": current_date.month,
        "day": current_date.day,
        "quarter": (current_date.month - 1) // 3 + 1,
        "is_weekend": 1 if current_date.weekday() >= 5 else 0,
    })
    current_date += timedelta(days=1)

dates_df = spark.createDataFrame(dates)

logger.info(f"Generated {dates_df.count()} date records")

# COMMAND ----------

# Write to Gold
output_path = Paths.gold_table("dim_date")
dates_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(output_path)

logger.info(f"✓ Dimension DATE written: {dates_df.count()} records")
print(f"✓ DIM_DATE built")

# COMMAND ----------

dates_df.filter(col("date_key") >= 20260501).limit(10).show()
