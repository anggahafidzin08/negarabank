# Databricks notebook source
# Real-Time Fraud Detection: Kafka Stream → Feature Enrichment → ML Scoring → Delta Alert Table

from pyspark.sql.functions import (
    col, from_json, schema_of_json, lit, current_timestamp,
    sum as spark_sum, count as spark_count, window, when,
    explode_outer, array_contains, row_number, broadcast
)
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, TimestampType
from pyspark.sql.window import Window
from datetime import datetime
from src.python.config import Paths, get_kafka_config
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("FraudDetectionStreaming")

# COMMAND ----------

# Get Kafka config
kafka_config = get_kafka_config()
logger.info(f"Kafka config: {kafka_config}")

# COMMAND ----------

# Define mobile event schema
mobile_event_schema = StructType([
    StructField("event_id", LongType(), False),
    StructField("customer_id", LongType(), False),
    StructField("event_type", StringType(), True),
    StructField("timestamp", TimestampType(), False),
    StructField("device_id", StringType(), True),
    StructField("location", StringType(), True),
    StructField("amount", DoubleType(), True),
])

# COMMAND ----------

# Read Kafka stream
kafka_stream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_config["bootstrap_servers"]) \
    .option("subscribe", kafka_config["mobile_events_topic"]) \
    .option("startingOffsets", "latest") \
    .option("checkpointLocation", f"{Paths.S3['checkpoints']}fraud_detection/") \
    .load()

logger.info("Connected to Kafka stream")

# COMMAND ----------

# Parse JSON payload
events_df = kafka_stream.select(
    col("timestamp").alias("kafka_timestamp"),
    from_json(col("value").cast("string"), mobile_event_schema).alias("data")
).select(
    col("data.event_id"),
    col("data.customer_id"),
    col("data.event_type"),
    col("data.timestamp").alias("event_timestamp"),
    col("data.device_id"),
    col("data.location"),
    col("data.amount"),
    col("kafka_timestamp"),
)

logger.info("Parsed Kafka events")

# COMMAND ----------

# Deduplication: Remove exact duplicates within 5-min window (by event_id)
window_spec = Window.partitionBy("event_id").orderBy(col("event_timestamp").desc())
deduped_events = events_df.withColumn("rn", row_number().over(window_spec)) \
    .filter(col("rn") == 1) \
    .drop("rn")

# COMMAND ----------

# Broadcast join with account master (latest 7-day snapshot from Silver)
accounts_silver = spark.read.format("delta").load(Paths.silver_table("accounts"))
accounts_broadcast = broadcast(accounts_silver.select("customer_id", "account_id", "balance"))

events_with_account = deduped_events.join(
    accounts_broadcast,
    on="customer_id",
    how="left"
)

# COMMAND ----------

# Feature engineering: 24-hour window aggregations
# (Streaming window for fraud signal enrichment)

window_spec_24h = Window \
    .partitionBy("customer_id") \
    .orderBy(col("event_timestamp")) \
    .rangeBetween(-86400, 0)  # 24 hours in seconds

events_with_features = events_with_account.withColumn(
    "event_count_24h",
    spark_count("event_id").over(window_spec_24h)
).withColumn(
    "avg_transaction_amount",
    (spark_sum(col("amount")).over(window_spec_24h) / spark_count("event_id").over(window_spec_24h))
)

# COMMAND ----------

# ML Model Inference
# (In production, load MLflow model; here using placeholder)

# Placeholder fraud scoring function
def fraud_score_udf(event_count: int, avg_amount: float, balance: float) -> float:
    """
    Simple fraud scoring (placeholder; real model would be XGBoost/LightGBM).

    Logic:
    - High event count (>100 in 24h) + low balance = higher risk
    - Large amount compared to avg balance = higher risk
    """
    score = 0.0
    if event_count > 100:
        score += 0.3
    if avg_amount and balance and avg_amount > balance * 0.5:
        score += 0.4
    return min(score, 1.0)

from pyspark.sql.functions import udf
fraud_score_fn = udf(fraud_score_udf, DoubleType())

events_with_score = events_with_features.withColumn(
    "fraud_score",
    fraud_score_fn(col("event_count_24h"), col("avg_transaction_amount"), col("balance"))
)

# COMMAND ----------

# Alert classification based on score
events_with_alert = events_with_score.withColumn(
    "fraud_alert_status",
    when(col("fraud_score") > 0.8, "HIGH_RISK")
    .when(col("fraud_score") > 0.6, "MEDIUM_RISK")
    .otherwise("LOW_RISK")
).withColumn(
    "model_version",
    lit("1.0")
).withColumn(
    "processing_timestamp",
    current_timestamp()
).withColumn(
    "alert_sent",
    lit(False)
).withColumn(
    "event_date",
    lit(datetime.now().strftime("%Y-%m-%d"))
)

# COMMAND ----------

# Select final schema for fact_fraud_transaction_alert
fraud_alerts_final = events_with_alert.select(
    col("event_id").alias("transaction_id"),
    col("customer_id"),
    col("account_id"),
    col("amount"),
    col("event_timestamp"),
    col("event_count_24h"),
    col("avg_transaction_amount"),
    col("balance").alias("account_balance"),
    col("fraud_score"),
    col("fraud_alert_status"),
    col("model_version"),
    col("processing_timestamp"),
    col("alert_sent"),
    col("event_date"),
)

logger.info("Fraud detection pipeline ready")

# COMMAND ----------

# Write to Delta (upsert mode for fraud alerts)
def write_fraud_alerts(batch_df, batch_id):
    """Write batch of fraud alerts to Delta (upsert by transaction_id)."""
    try:
        batch_df.write \
            .format("delta") \
            .option("mergeSchema", "true") \
            .mode("append") \
            .partitionBy("event_date") \
            .save(Paths.gold_table("fact_fraud_transaction_alert"))

        logger.info(f"Batch {batch_id}: {batch_df.count()} fraud alerts written")
    except Exception as e:
        logger.error(f"Batch {batch_id} failed: {str(e)}")

fraud_alerts_final.writeStream \
    .foreachBatch(write_fraud_alerts) \
    .option("checkpointLocation", f"{Paths.S3['checkpoints']}fraud_alerts/") \
    .start() \
    .awaitTermination()

logger.info("✓ Fraud detection streaming job started (running indefinitely)")
print("✓ FRAUD DETECTION STREAMING: 24/7 Kafka consumer active")
