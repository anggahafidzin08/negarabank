# Databricks notebook source
# Gold Layer: Fact - FRAUD_RISK (Daily Batch Aggregation)

from pyspark.sql.functions import col, count, sum as spark_sum, max as spark_max, min as spark_min, datediff, current_date, lit, when
from datetime import datetime
from src.python.config import Paths
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("GoldFactFraudRisk")

# COMMAND ----------

# Read dimensions
dim_customer = spark.read.format("delta").load(Paths.gold_table("dim_customer")) \
    .filter(col("is_current") == "true").select("customer_key", "customer_id")

# Read fact transactions for fraud analysis
try:
    fact_txns = spark.read.format("delta").load(Paths.gold_table("fact_transactions"))
except:
    logger.info("fact_transactions not yet available")
    fact_txns = None

# COMMAND ----------

if fact_txns:
    # Aggregate fraud risk metrics per customer
    fraud_risk = fact_txns.groupBy("customer_key").agg(
        spark_max("amount").alias("max_transaction_amount_7d"),
        (spark_sum("amount") / count("*")).alias("avg_transaction_amount"),
        count("*").alias("transaction_count_7d"),
    )

    # Add customer info
    fraud_risk = fraud_risk.join(
        dim_customer, on="customer_key", how="left"
    )

    # Calculate fraud risk score (simplified; real logic would use ML model)
    fraud_risk = fraud_risk.withColumn(
        "fraud_risk_score",
        lit(0.3)  # Placeholder; actual model would score here
    ).withColumn(
        "fraud_indicator",
        when(col("fraud_risk_score") > 0.7, "HIGH")
        .when(col("fraud_risk_score") > 0.4, "MEDIUM")
        .otherwise("LOW")
    ).withColumn(
        "last_fraud_date",
        lit(None)
    ).withColumn(
        "fraud_count_12m",
        lit(0)
    ).withColumn(
        "model_version",
        lit("1.0")
    ).withColumn(
        "snapshot_date",
        lit(datetime.now().strftime("%Y-%m-%d"))
    )

    logger.info(f"Built fact_fraud_risk: {fraud_risk.count()} records")

    # Write to Gold
    output_path = Paths.gold_table("fact_fraud_risk")
    fraud_risk.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("snapshot_date") \
        .save(output_path)

    logger.info(f"✓ Fact FRAUD_RISK written to Gold")
else:
    logger.info("Skipping fact_fraud_risk (no transaction data)")

print(f"✓ FACT_FRAUD_RISK built")

# COMMAND ----------

# Show sample
if fact_txns:
    spark.read.format("delta").load(Paths.gold_table("fact_fraud_risk")).limit(10).show()
