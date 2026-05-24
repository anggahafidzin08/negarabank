# Databricks notebook source
# Bronze Layer: Extract ACCOUNTS from Oracle

from datetime import datetime
from pyspark.sql.functions import col, sum as _sum, count as _count, when, isnull, lit, Window
from src.python.config import get_oracle_credentials, Paths
from src.python.jdbc_extractor import JDBCExtractor
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("BronzeAccountsLoad")

# COMMAND ----------

# Load credentials
creds = get_oracle_credentials()
jdbc_url = f"jdbc:oracle:thin:@{creds['host']}:{creds['port']}/banking"

# Initialize extractor
extractor = JDBCExtractor(spark, jdbc_url, creds)

logger.info("Starting ACCOUNTS extraction from Oracle...")

# COMMAND ----------

# Extract full ACCOUNTS table (static master)
accounts_df = extractor.extract_full_table("ACCOUNTS")

logger.info(f"Extracted {accounts_df.count()} account records")

# COMMAND ----------

# Add load metadata
load_date = datetime.now().strftime("%Y-%m-%d")
accounts_df = accounts_df.withColumn("load_date", lit(load_date))
accounts_df = accounts_df.withColumn("load_timestamp", lit(datetime.now().isoformat()))

# Write to Bronze layer (partitioned by load_date)
output_path = Paths.bronze_table("accounts")
accounts_df.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("load_date") \
    .save(output_path)

logger.info(f"✓ Accounts loaded to: {output_path}")
print(f"✓ ACCOUNTS: {accounts_df.count()} records written to Bronze")

# COMMAND ----------

# Display schema
accounts_df.printSchema()

# COMMAND ----------

# Quick data quality check
print(f"Null counts:")
accounts_df.select([(count(when(isnull(c), 1)).over(Window.partitionBy())).alias(c) for c in accounts_df.columns]).show()
