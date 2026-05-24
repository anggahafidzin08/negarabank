# Databricks notebook source
# Gold Layer: Dimension - CUSTOMER (SCD Type 2)
# Tracks customer changes over time

from pyspark.sql.functions import col, row_number, lit, current_date, to_date, when, coalesce, max as spark_max
from pyspark.sql.window import Window
from datetime import datetime
from src.python.config import Paths
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("GoldDimCustomer")

# COMMAND ----------

# Read Silver accounts (source of truth for customers)
accounts_silver = spark.read.format("delta").load(Paths.silver_table("accounts"))

# COMMAND ----------

# Capture timestamp once for consistency
current_timestamp = datetime.now().strftime("%Y-%m-%d")

# Prepare new records
new_customers = accounts_silver.select(
    col("customer_id"),
    lit(None).cast("string").alias("name"),  # Would come from CRM
    lit(None).cast("string").alias("email"),  # Would come from CRM
    lit("standard").alias("segment"),  # Derived from account info
    col("balance").alias("risk_score"),
    lit(current_timestamp).alias("effective_date"),
    lit(None).cast("string").alias("end_date"),
    lit("true").alias("is_current"),
).distinct()

logger.info(f"Processing customers from Silver")

# COMMAND ----------

# Try reading existing dim_customer
try:
    existing_customers = spark.read.format("delta").load(Paths.gold_table("dim_customer"))
    has_existing = True
except:
    has_existing = False
    existing_customers = None

logger.info(f"Existing dimension data: {has_existing}")

# COMMAND ----------

if has_existing:
    # SCD Type 2 Merge: Mark old records as expired, insert new records
    # Records that changed: compare new vs existing (is_current=true)

    current_records = existing_customers.filter(col("is_current") == "true")

    # Identify changed records
    changed = new_customers.join(
        current_records,
        on="customer_id",
        how="inner"
    ).filter(
        # Compare key fields (name, email, segment, risk_score)
        (col("new_customers.name") != col("current_records.name")) |
        (col("new_customers.email") != col("current_records.email")) |
        (col("new_customers.segment") != col("current_records.segment"))
    )

    # Close old records
    expired_records = current_records.select(
        col("customer_key"),
        col("customer_id"),
        col("name"),
        col("email"),
        col("segment"),
        col("risk_score"),
        col("effective_date"),
        lit(current_timestamp).alias("end_date"),
        lit("false").alias("is_current"),
    )

    # Insert new versions of changed records
    changed_updates = changed.select(
        col("current_records.customer_key"),  # Reuse existing key
        col("customer_id"),
        col("new_customers.name"),
        col("new_customers.email"),
        col("new_customers.segment"),
        col("new_customers.risk_score"),
        lit(current_timestamp).alias("effective_date"),
        lit(None).cast("string").alias("end_date"),
        lit("true").alias("is_current"),
    )

    # Combine: existing non-changed + expired + new changes
    # Use anti-join to avoid .collect() scalability risk
    unchanged_records = current_records.join(
        changed.select(col("customer_id")).distinct(),
        on="customer_id",
        how="left_anti"  # Keeps rows from left that don't match right
    )

    final_dim = unchanged_records.union(expired_records).union(changed_updates)

    # Handle new customers not in existing table
    all_customer_ids = new_customers.select("customer_id")
    existing_customer_ids = current_records.select("customer_id")
    new_only = all_customer_ids.join(
        existing_customer_ids,
        on="customer_id",
        how="left_anti"
    )

    if new_only.count() > 0:
        new_customers_only = new_customers.join(
            new_only,
            on="customer_id",
            how="inner"
        )
        # Assign new customer_keys
        max_key = final_dim.agg(spark_max("customer_key")).collect()[0][0] or 0
        new_with_keys = new_customers_only.withColumn(
            "customer_key",
            row_number().over(Window.orderBy("customer_id")) + max_key
        )
        final_dim = final_dim.union(new_with_keys)

else:
    # First load: assign customer_keys
    new_customers_with_keys = new_customers.withColumn(
        "customer_key",
        row_number().over(Window.orderBy("customer_id"))
    )
    final_dim = new_customers_with_keys

# COMMAND ----------

# Write to Gold (overwrite or append based on SCD strategy)
output_path = Paths.gold_table("dim_customer")
final_dim.write \
    .format("delta") \
    .mode("overwrite") \
    .save(output_path)

logger.info(f"✓ Dimension CUSTOMER written: {final_dim.count()} records (SCD Type 2)")
print(f"✓ DIM_CUSTOMER built with SCD Type 2 tracking")

# COMMAND ----------

# Show active records
spark.read.format("delta").load(output_path).filter(col("is_current") == "true").show(5)
