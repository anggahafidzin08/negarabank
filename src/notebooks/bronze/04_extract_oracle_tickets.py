# Databricks notebook source
# Bronze Layer: Extract SUPPORT_TICKETS from Oracle

from datetime import datetime
from pyspark.sql.functions import lit
from src.python.config import get_oracle_credentials, Paths
from src.python.jdbc_extractor import JDBCExtractor
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("BronzeSupportTicketsLoad")

# COMMAND ----------

creds = get_oracle_credentials()
jdbc_url = f"jdbc:oracle:thin:@{creds['host']}:{creds['port']}/banking"
extractor = JDBCExtractor(spark, jdbc_url, creds)

logger.info("Starting SUPPORT_TICKETS extraction...")

# COMMAND ----------

# Extract support tickets (batch daily load)
tickets_df = extractor.extract_full_table("SUPPORT_TICKETS")

record_count = tickets_df.count()
logger.info(f"Extracted {record_count} support ticket records")

# COMMAND ----------

# Add load metadata (capture once, reuse)
load_timestamp = datetime.now()
load_date = load_timestamp.strftime("%Y-%m-%d")
load_timestamp_iso = load_timestamp.isoformat()

tickets_df = tickets_df.withColumn("load_date", lit(load_date))
tickets_df = tickets_df.withColumn("load_timestamp", lit(load_timestamp_iso))

# Write to Bronze
output_path = Paths.bronze_table("support_tickets")
tickets_df.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("load_date") \
    .save(output_path)

logger.info(f"✓ Support tickets loaded to: {output_path}")
print(f"✓ SUPPORT_TICKETS: {record_count} records written to Bronze")

# COMMAND ----------

tickets_df.limit(10).show()
