import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, StringType, DecimalType

@pytest.fixture(scope="session")
def spark():
    """Create a SparkSession for testing."""
    return SparkSession.builder \
        .appName("negarabank-tests") \
        .config("spark.sql.shuffle.partitions", 4) \
        .config("spark.default.parallelism", 4) \
        .getOrCreate()

@pytest.fixture
def sample_accounts_data(spark):
    """Create sample accounts data for testing."""
    schema = StructType([
        StructField("account_id", LongType(), False),
        StructField("customer_id", LongType(), False),
        StructField("account_type", StringType(), True),
        StructField("balance", DecimalType(15, 2), True),
    ])

    data = [
        (1, 100, "CHECKING", 5000.00),
        (2, 101, "SAVINGS", 10000.00),
        (3, 102, "CHECKING", 2500.00),
    ]

    return spark.createDataFrame(data, schema)

@pytest.fixture
def sample_transactions_data(spark):
    """Create sample transactions data for testing."""
    from pyspark.sql.types import TimestampType
    from datetime import datetime

    schema = StructType([
        StructField("transaction_id", LongType(), False),
        StructField("account_id", LongType(), False),
        StructField("amount", DecimalType(15, 2), False),
        StructField("txn_date", TimestampType(), False),
    ])

    data = [
        (1001, 1, 500.00, datetime(2026, 5, 24, 10, 0)),
        (1002, 1, 300.00, datetime(2026, 5, 24, 11, 0)),
        (1003, 2, 1000.00, datetime(2026, 5, 24, 12, 0)),
    ]

    return spark.createDataFrame(data, schema)
