import pytest
from pyspark.sql.functions import col

def test_accounts_null_check(sample_accounts_data):
    """Test that required account columns have no nulls."""
    null_count = sample_accounts_data.filter(col("account_id").isNull()).count()
    assert null_count == 0, "account_id should not have nulls"

def test_accounts_deduplication(spark, sample_accounts_data):
    """Test deduplication logic removes exact duplicates."""
    from pyspark.sql.functions import row_number
    from pyspark.sql.window import Window

    # Add duplicates
    with_dupes = sample_accounts_data.union(
        sample_accounts_data.filter(col("account_id") == 1).limit(1)
    )

    # Deduplicate
    window = Window.partitionBy("account_id").orderBy(col("account_id"))
    deduped = with_dupes.withColumn("rn", row_number().over(window)) \
        .filter(col("rn") == 1)

    assert deduped.count() == sample_accounts_data.count(), "Dedup should remove duplicates"

def test_transactions_fk_validation(spark, sample_accounts_data, sample_transactions_data):
    """Test orphaned records detection."""
    # Add transaction with non-existent account
    from pyspark.sql.types import StructType, StructField, LongType, DecimalType, TimestampType
    from datetime import datetime

    orphan_schema = StructType([
        StructField("transaction_id", LongType(), False),
        StructField("account_id", LongType(), False),
        StructField("amount", DecimalType(15, 2), False),
        StructField("txn_date", TimestampType(), False),
    ])

    orphan_data = [
        (2001, 999, 500.00, datetime(2026, 5, 24, 13, 0)),  # Non-existent account
    ]

    orphan_txn = spark.createDataFrame(orphan_data, orphan_schema)
    all_txns = sample_transactions_data.union(orphan_txn)

    # Check for orphans
    valid_accounts = sample_accounts_data.select("account_id").rdd.flatMap(lambda x: x).collect()
    orphans = all_txns.filter(~col("account_id").isin(valid_accounts))

    assert orphans.count() == 1, "Should detect 1 orphaned record"

def test_fraud_score_range(spark):
    """Test fraud scores are within valid range (0.0 - 1.0)."""
    from pyspark.sql.types import StructType, StructField, DoubleType

    schema = StructType([
        StructField("fraud_score", DoubleType(), False),
    ])

    data = [(0.0,), (0.5,), (0.99,), (1.0,)]
    scores_df = spark.createDataFrame(data, schema)

    invalid = scores_df.filter((col("fraud_score") < 0) | (col("fraud_score") > 1))
    assert invalid.count() == 0, "All fraud scores should be in [0, 1]"
