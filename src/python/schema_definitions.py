from pyspark.sql.types import StructType, StructField, StringType, LongType, DecimalType, TimestampType, IntegerType

# Silver Layer Schemas (for validation)

accounts_silver_schema = StructType([
    StructField("account_id", LongType(), False),
    StructField("customer_id", LongType(), False),
    StructField("account_type", StringType(), True),
    StructField("balance", DecimalType(15, 2), True),
    StructField("status", StringType(), True),
    StructField("open_date", TimestampType(), True),
    StructField("dq_passed", StringType(), True),
    StructField("load_date", StringType(), False),
])

transactions_silver_schema = StructType([
    StructField("transaction_id", LongType(), False),
    StructField("account_id", LongType(), False),
    StructField("customer_id", LongType(), False),
    StructField("amount", DecimalType(15, 2), False),
    StructField("txn_date", TimestampType(), False),
    StructField("status", StringType(), True),
    StructField("reconciled", StringType(), True),
    StructField("load_date", StringType(), False),
])

# Gold Layer Schemas

dim_customer_schema = StructType([
    StructField("customer_key", LongType(), False),
    StructField("customer_id", LongType(), False),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("segment", StringType(), True),
    StructField("risk_score", DecimalType(5, 2), True),
    StructField("effective_date", StringType(), False),
    StructField("end_date", StringType(), True),
    StructField("is_current", StringType(), False),
])

fact_fraud_alert_schema = StructType([
    StructField("transaction_id", LongType(), False),
    StructField("customer_id", LongType(), False),
    StructField("account_id", LongType(), False),
    StructField("amount", DecimalType(15, 2), False),
    StructField("event_timestamp", TimestampType(), False),
    StructField("event_count_24h", IntegerType(), True),
    StructField("avg_transaction_amount", DecimalType(15, 2), True),
    StructField("account_balance", DecimalType(15, 2), True),
    StructField("fraud_score", DecimalType(3, 2), False),
    StructField("fraud_alert_status", StringType(), False),
    StructField("model_version", StringType(), True),
    StructField("processing_timestamp", TimestampType(), False),
    StructField("alert_sent", StringType(), True),
    StructField("event_date", StringType(), False),
])
