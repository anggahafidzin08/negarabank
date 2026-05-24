# Databricks notebook source
# Bronze Layer: Extract TRANSACTIONS from Oracle (Delta Load)

from datetime import datetime, timedelta
from pyspark.sql.functions import lit, col
from src.python.config import get_oracle_credentials, Paths
from src.python.jdbc_extractor import JDBCExtractor
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("BronzeTransactionsLoad")

# COMMAND ----------

creds = get_oracle_credentials()
jdbc_url = f"jdbc:oracle:thin:@{creds['host']}:{creds['port']}/banking"
extractor = JDBCExtractor(spark, jdbc_url, creds)

# COMMAND ----------

# Get last successful load date
try:
    last_load_df = spark.table("bronze.transactions")
    last_load_date = last_load_df.select(max(col("load_date"))).collect()[0][0]
    last_load_dt = datetime.strptime(str(last_load_date), "%Y-%m-%d")
except:
    # First load: go back 30 days
    last_load_dt = datetime.now() - timedelta(days=30)

logger.info(f"Delta load: extracting transactions from {last_load_dt} onwards")

# COMMAND ----------

# Extract transactions incrementally (JDBC with predicate slicing)
# Predicate slicing: 4 x 6-hour windows for parallel JDBC connections
target_date = datetime.now().strftime("%Y-%m-%d")
predicates = [
    f"txn_date >= TO_DATE('{target_date} 00:00:00', 'YYYY-MM-DD HH24:MI:SS') AND txn_date < TO_DATE('{target_date} 06:00:00', 'YYYY-MM-DD HH24:MI:SS')",
    f"txn_date >= TO_DATE('{target_date} 06:00:00', 'YYYY-MM-DD HH24:MI:SS') AND txn_date < TO_DATE('{target_date} 12:00:00', 'YYYY-MM-DD HH24:MI:SS')",
    f"txn_date >= TO_DATE('{target_date} 12:00:00', 'YYYY-MM-DD HH24:MI:SS') AND txn_date < TO_DATE('{target_date} 18:00:00', 'YYYY-MM-DD HH24:MI:SS')",
    f"txn_date >= TO_DATE('{target_date} 18:00:00', 'YYYY-MM-DD HH24:MI:SS') AND txn_date < TO_DATE('{target_date + 1} 00:00:00', 'YYYY-MM-DD HH24:MI:SS')",
]

txns_df = extractor.extract_with_predicate("TRANSACTIONS", predicates)

logger.info(f"Extracted {txns_df.count()} transaction records")

# COMMAND ----------

# Add load metadata
load_date = datetime.now().strftime("%Y-%m-%d")
txns_df = txns_df.withColumn("load_date", lit(load_date))
txns_df = txns_df.withColumn("load_timestamp", lit(datetime.now().isoformat()))

# Write to Bronze (append mode for delta load)
output_path = Paths.bronze_table("transactions")
txns_df.write \
    .format("delta") \
    .mode("append") \
    .partitionBy("load_date") \
    .save(output_path)

logger.info(f"✓ Transactions appended to: {output_path}")
print(f"✓ TRANSACTIONS: {txns_df.count()} records appended to Bronze")

# COMMAND ----------

# Display sample
txns_df.limit(10).show()
