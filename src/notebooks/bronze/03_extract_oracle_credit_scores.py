# Databricks notebook source
# Bronze Layer: Extract CREDIT_SCORES from Oracle

from datetime import datetime
from pyspark.sql.functions import lit
from src.python.config import get_oracle_credentials, Paths
from src.python.jdbc_extractor import JDBCExtractor
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("BronzeCreditScoresLoad")

# COMMAND ----------

creds = get_oracle_credentials()
jdbc_url = f"jdbc:oracle:thin:@{creds['host']}:{creds['port']}/banking"
extractor = JDBCExtractor(spark, jdbc_url, creds)

logger.info("Starting CREDIT_SCORES extraction...")

# COMMAND ----------

# Extract full credit scores snapshot (daily)
scores_df = extractor.extract_full_table("CREDIT_SCORES")

logger.info(f"Extracted {scores_df.count()} credit score records")

# COMMAND ----------

# Add load metadata
load_date = datetime.now().strftime("%Y-%m-%d")
scores_df = scores_df.withColumn("load_date", lit(load_date))
scores_df = scores_df.withColumn("load_timestamp", lit(datetime.now().isoformat()))

# Write to Bronze
output_path = Paths.bronze_table("credit_scores")
scores_df.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("load_date") \
    .save(output_path)

logger.info(f"✓ Credit scores loaded to: {output_path}")
print(f"✓ CREDIT_SCORES: {scores_df.count()} records written to Bronze")

# COMMAND ----------

scores_df.limit(10).show()
