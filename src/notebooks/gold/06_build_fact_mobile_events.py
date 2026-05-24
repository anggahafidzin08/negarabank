# Databricks notebook source
# Gold Layer: Fact - MOBILE_EVENTS

from pyspark.sql.functions import col, to_date, explode, arrays_zip, rank, lit
from pyspark.sql.window import Window
from src.python.config import Paths
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("GoldFactMobileEvents")

# COMMAND ----------

# Read dimensions and silver mobile events
dim_customer = spark.read.format("delta").load(Paths.gold_table("dim_customer")) \
    .filter(col("is_current") == "true").select("customer_key", "customer_id")
dim_event_type = spark.read.format("delta").load(Paths.gold_table("dim_event_type"))
dim_date = spark.read.format("delta").load(Paths.gold_table("dim_date"))

# Read mobile events (would come from streaming or batch)
try:
    events_silver = spark.read.format("delta").load(Paths.silver_table("mobile_events"))
except:
    logger.info("Mobile events not yet loaded (streaming will populate)")
    events_silver = spark.createDataFrame([], "event_id long, customer_id long, event_type string, event_timestamp timestamp")

# COMMAND ----------

# Build fact table if events exist
has_events = events_silver.count() > 0  # Single count check

if has_events:
    fact_events = events_silver.join(
        dim_customer, on="customer_id", how="left"
    ).join(
        dim_event_type, on="event_type", how="left"
    ).join(
        dim_date,
        col("dim_date.date_key") == to_date(col("event_timestamp")).cast("string"),
        how="left"
    )

    fact_events = fact_events.select(
        col("event_id"),
        col("customer_key"),
        col("date_key").alias("event_date_key"),
        col("event_type_id"),
        lit(1).alias("event_count"),
        col("event_timestamp"),
        col("load_date"),
    )

    logger.info(f"Built fact_mobile_events: {fact_events.count()} records")

    # Write to Gold
    output_path = Paths.gold_table("fact_mobile_events")
    fact_events.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("load_date") \
        .save(output_path)

    logger.info(f"✓ Fact MOBILE_EVENTS written to Gold")
else:
    logger.info("Skipping fact_mobile_events (no data yet)")

print(f"✓ FACT_MOBILE_EVENTS prepared (streaming will populate)")

# COMMAND ----------

# Show sample if available
if has_events:
    spark.read.format("delta").load(Paths.gold_table("fact_mobile_events")).limit(10).show()
