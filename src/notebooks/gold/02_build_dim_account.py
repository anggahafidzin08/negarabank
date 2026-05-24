# Databricks notebook source
# Gold Layer: Dimension - ACCOUNT

from pyspark.sql.functions import col, row_number, lit
from pyspark.sql.window import Window
from src.python.config import Paths
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("GoldDimAccount")

# COMMAND ----------

# Read Silver accounts
accounts_silver = spark.read.format("delta").load(Paths.silver_table("accounts"))

# COMMAND ----------

# Build dimension (no SCD needed, accounts are relatively static)
dim_account = accounts_silver.select(
    col("account_id"),
    col("customer_id"),
    col("account_type"),
    col("status"),
    col("open_date"),
).distinct()

dim_account = dim_account.withColumn(
    "account_key",
    row_number().over(Window.orderBy("account_id"))
)

# COMMAND ----------

# Write to Gold
output_path = Paths.gold_table("dim_account")
dim_account.write \
    .format("delta") \
    .mode("overwrite") \
    .save(output_path)

logger.info(f"✓ Dimension ACCOUNT written: {dim_account.count()} records")
print(f"✓ DIM_ACCOUNT built")

# COMMAND ----------

dim_account.limit(10).show()
