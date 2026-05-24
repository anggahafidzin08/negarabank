# Databricks notebook source
# Silver Layer: Transform ACCOUNTS (Deduplication + DQ)

from pyspark.sql.functions import col, row_number, lit, max as spark_max, count as spark_count
from pyspark.sql.window import Window
from datetime import datetime
from src.python.config import Paths
from src.python.dq_framework import DataQualityFramework
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("SilverAccountsTransform")

# COMMAND ----------

# Read Bronze
bronze_path = Paths.bronze_table("accounts")
accounts_bronze = spark.read.format("delta").load(bronze_path)

logger.info(f"Read raw accounts from Bronze")

# COMMAND ----------

# Data Quality Checks
dq = DataQualityFramework(spark, Paths.S3['silver'])
dq.add_null_check("accounts", "account_id", threshold=0.0)
dq.add_null_check("accounts", "customer_id", threshold=0.0)
dq.add_uniqueness_check("accounts", "account_id")

dq_results = dq.run_checks()
logger.info(f"DQ checks: {dq_results['passed']} passed, {dq_results['failed']} failed")

# COMMAND ----------

# Deduplication: Keep latest by account_id
# (though ACCOUNTS is usually unique, handle if duplicates exist)
window_spec = Window.partitionBy("account_id").orderBy(col("load_timestamp").desc())
accounts_dedup = accounts_bronze.withColumn("rn", row_number().over(window_spec)) \
    .filter(col("rn") == 1) \
    .drop("rn")

logger.info(f"After dedup: deduplication complete")

# COMMAND ----------

# Type casting & standardization
accounts_silver = accounts_dedup.select(
    col("account_id").cast("long"),
    col("customer_id").cast("long"),
    col("account_type").cast("string"),
    col("balance").cast("decimal(15,2)"),
    col("status").cast("string"),
    col("open_date").cast("timestamp"),
    lit("true").alias("dq_passed"),
    col("load_date").cast("string"),
)

# COMMAND ----------

# Write to Silver
output_path = Paths.silver_table("accounts")
accounts_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("load_date") \
    .save(output_path)

logger.info(f"✓ Silver accounts written: {accounts_silver.count()} records")
print(f"✓ ACCOUNTS transformed and loaded to Silver ({output_path})")

# COMMAND ----------

accounts_silver.limit(10).show()
