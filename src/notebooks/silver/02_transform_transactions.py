# Databricks notebook source
# Silver Layer: Transform TRANSACTIONS (Reconciliation + DQ)

from pyspark.sql.functions import col, row_number, lit, when, coalesce
from pyspark.sql.window import Window
from datetime import datetime
from src.python.config import Paths
from src.python.dq_framework import DataQualityFramework
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("SilverTransactionsTransform")

# COMMAND ----------

# Read Bronze
bronze_path = Paths.bronze_table("transactions")
txns_bronze = spark.read.format("delta").load(bronze_path)

logger.info(f"Read {txns_bronze.count()} raw transactions from Bronze")

# COMMAND ----------

# Read Bronze accounts for FK validation
accounts_bronze = spark.read.format("delta").load(Paths.bronze_table("accounts"))

# COMMAND ----------

# Data Quality Checks
dq = DataQualityFramework(spark, Paths.S3['silver'])
dq.add_null_check("transactions", "transaction_id", threshold=0.0)
dq.add_null_check("transactions", "amount", threshold=0.0)
dq.add_uniqueness_check("transactions", "transaction_id")

dq_results = dq.run_checks()
logger.info(f"DQ checks: {dq_results['passed']} passed, {dq_results['failed']} failed")

# COMMAND ----------

# Reconciliation: Check for orphaned records (FK validation)
txns_with_fk = txns_bronze.join(
    accounts_bronze.select("account_id").distinct(),
    on="account_id",
    how="left"
)

# Mark orphaned
txns_reconciled = txns_bronze.withColumn(
    "reconciled",
    when(
        col("account_id").isin(accounts_bronze.select("account_id").rdd.flatMap(lambda x: x).collect()),
        "true"
    ).otherwise("false")
)

orphan_count = txns_reconciled.filter(col("reconciled") == "false").count()
logger.info(f"Orphaned records detected: {orphan_count}")

# COMMAND ----------

# Deduplication: Keep latest by transaction_id
window_spec = Window.partitionBy("transaction_id").orderBy(col("load_timestamp").desc())
txns_dedup = txns_reconciled.withColumn("rn", row_number().over(window_spec)) \
    .filter(col("rn") == 1) \
    .drop("rn")

logger.info(f"After dedup: {txns_dedup.count()} unique transactions")

# COMMAND ----------

# Type casting & standardization
txns_silver = txns_dedup.select(
    col("transaction_id").cast("long"),
    col("account_id").cast("long"),
    col("customer_id").cast("long"),
    col("amount").cast("decimal(15,2)"),
    col("txn_date").cast("timestamp"),
    col("status").cast("string"),
    col("reconciled").cast("string"),
    col("load_date").cast("string"),
)

# COMMAND ----------

# Write to Silver (append mode for incremental loads)
output_path = Paths.silver_table("transactions")
txns_silver.write \
    .format("delta") \
    .mode("append") \
    .partitionBy("load_date") \
    .save(output_path)

logger.info(f"✓ Silver transactions written: {txns_silver.count()} records")
print(f"✓ TRANSACTIONS transformed and loaded to Silver ({output_path})")

# COMMAND ----------

txns_silver.filter(col("reconciled") == "false").show(5)
