import pytest
from datetime import datetime
from pyspark.sql.functions import lit, col, row_number
from pyspark.sql.window import Window

@pytest.mark.integration
def test_bronze_to_silver_accounts_pipeline(spark, sample_accounts_data):
    """Test full Bronze → Silver transformation for accounts."""
    # Simulate bronze load
    bronze_accounts = sample_accounts_data.withColumn(
        "load_date",
        lit(datetime.now().strftime("%Y-%m-%d"))
    )

    # Simulate silver transformation
    silver_accounts = bronze_accounts.select(
        col("account_id").cast("long"),
        col("customer_id").cast("long"),
        col("account_type").cast("string"),
        col("balance").cast("decimal(15,2)"),
        lit("true").alias("dq_passed"),
        col("load_date").cast("string"),
    )

    # Validate
    assert silver_accounts.count() == sample_accounts_data.count()
    assert silver_accounts.schema.fieldNames() == [
        "account_id", "customer_id", "account_type", "balance", "dq_passed", "load_date"
    ]

@pytest.mark.integration
def test_silver_to_gold_dimension_pipeline(spark, sample_accounts_data):
    """Test Silver → Gold dimension building."""
    # Add load_date for silver
    silver = sample_accounts_data.withColumn(
        "load_date",
        lit(datetime.now().strftime("%Y-%m-%d"))
    )

    # Build gold dimension
    gold_dim = silver.select(
        col("account_id"),
        col("customer_id"),
        col("account_type"),
    ).distinct().withColumn(
        "account_key",
        row_number().over(Window.orderBy("account_id"))
    )

    assert gold_dim.count() == sample_accounts_data.select("account_id").distinct().count()
    assert "account_key" in gold_dim.columns
