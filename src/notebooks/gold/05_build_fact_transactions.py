# Databricks notebook source
# Gold Layer: Fact - TRANSACTIONS

from pyspark.sql.functions import col, to_date, when
from src.python.config import Paths
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("GoldFactTransactions")

# COMMAND ----------

# Read dimensions and silver transactions
dim_customer = spark.read.format("delta").load(Paths.gold_table("dim_customer")) \
    .filter(col("is_current") == "true").select("customer_key", "customer_id")
dim_account = spark.read.format("delta").load(Paths.gold_table("dim_account"))
dim_date = spark.read.format("delta").load(Paths.gold_table("dim_date"))
txns_silver = spark.read.format("delta").load(Paths.silver_table("transactions"))

# COMMAND ----------

# Build fact table (join dimensions)
fact_txns = txns_silver.join(
    dim_customer, on="customer_id", how="left"
).join(
    dim_account, on=["customer_id", "account_id"], how="left"
).join(
    dim_date,
    col("dim_date.date_key") == to_date(col("txn_date")).cast("string"),
    how="left"
)

fact_txns = fact_txns.select(
    col("transaction_id"),
    col("customer_key"),
    col("account_key"),
    col("date_key").alias("txn_date_key"),
    col("amount"),
    col("status"),
    col("txn_date").alias("created_at"),
    col("load_date"),
)

logger.info(f"Built fact_transactions: {fact_txns.count()} records")

# COMMAND ----------

# Write to Gold
output_path = Paths.gold_table("fact_transactions")
fact_txns.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("load_date") \
    .save(output_path)

logger.info(f"✓ Fact TRANSACTIONS written to Gold")
print(f"✓ FACT_TRANSACTIONS built")

# COMMAND ----------

fact_txns.limit(10).show()
