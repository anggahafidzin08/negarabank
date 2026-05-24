# Databricks notebook source
# Bronze Layer: Extract TRANSACTIONS from Oracle (Delta Load)

from datetime import datetime, timedelta
from pyspark.sql.functions import lit, col, max as spark_max
from src.python.config import get_oracle_credentials, Paths
from src.python.jdbc_extractor import JDBCExtractor
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("BronzeTransactionsLoad")

# COMMAND ----------

# Initialize extractor and credentials
creds = get_oracle_credentials()
jdbc_url = f"jdbc:oracle:thin:@{creds['host']}:{creds['port']}/banking"
extractor = JDBCExtractor(spark, jdbc_url, creds)

# COMMAND ----------

# Determine date range for delta load
# For daily batch job at 2 AM UTC, load yesterday's full day
try:
    if spark.catalog.tableExists("bronze", "transactions"):
        last_load_df = spark.table("bronze.transactions")
        last_load_date_str = last_load_df.select(spark_max(col("load_date"))).collect()[0][0]
        # Safely parse date, handling both DATE and TIMESTAMP columns
        last_load_dt = datetime.fromisoformat(str(last_load_date_str).split()[0])
    else:
        # First load: go back 30 days
        last_load_dt = datetime.now() - timedelta(days=30)
        logger.info("bronze.transactions table does not exist yet, defaulting to 30-day lookback")
except Exception as e:
    logger.error(f"Error reading last load date: {e}, defaulting to 30-day lookback")
    last_load_dt = datetime.now() - timedelta(days=30)

# For this daily job (2 AM UTC), extract yesterday's transactions
target_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
logger.info(f"Delta load: extracting transactions from {last_load_dt} to {target_date}")

# COMMAND ----------

# Extract transactions incrementally (JDBC with predicate slicing)
# Predicate slicing: 4 x 6-hour windows for parallel JDBC connections
# This loads yesterday's full day in parallel
predicates = [
    f"txn_date >= TO_DATE('{target_date} 00:00:00', 'YYYY-MM-DD HH24:MI:SS') AND txn_date < TO_DATE('{target_date} 06:00:00', 'YYYY-MM-DD HH24:MI:SS')",
    f"txn_date >= TO_DATE('{target_date} 06:00:00', 'YYYY-MM-DD HH24:MI:SS') AND txn_date < TO_DATE('{target_date} 12:00:00', 'YYYY-MM-DD HH24:MI:SS')",
    f"txn_date >= TO_DATE('{target_date} 12:00:00', 'YYYY-MM-DD HH24:MI:SS') AND txn_date < TO_DATE('{target_date} 18:00:00', 'YYYY-MM-DD HH24:MI:SS')",
    f"txn_date >= TO_DATE('{target_date} 18:00:00', 'YYYY-MM-DD HH24:MI:SS') AND txn_date < TO_DATE('{target_date} 23:59:59', 'YYYY-MM-DD HH24:MI:SS')",
]

txns_df = extractor.extract_with_predicate("TRANSACTIONS", predicates)

# COMMAND ----------

# Count records for logging (perform once, reuse)
record_count = txns_df.count()
logger.info(f"Extracted {record_count} transaction records")

# COMMAND ----------

# Add load metadata (capture once, reuse across columns)
load_timestamp = datetime.now()
load_date = load_timestamp.strftime("%Y-%m-%d")
load_timestamp_iso = load_timestamp.isoformat()

txns_df = txns_df.withColumn("load_date", lit(load_date))
txns_df = txns_df.withColumn("load_timestamp", lit(load_timestamp_iso))

# Write to Bronze (append mode for delta load)
output_path = Paths.bronze_table("transactions")
txns_df.write \
    .format("delta") \
    .mode("append") \
    .partitionBy("load_date") \
    .save(output_path)

logger.info(f"✓ Transactions appended to: {output_path}")
print(f"✓ TRANSACTIONS: {record_count} records appended to Bronze")

# COMMAND ----------

# Display sample
txns_df.limit(10).show()
