import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime
from src.python.jdbc_extractor import JDBCExtractor

@pytest.fixture
def mock_spark():
    """Create a mock SparkSession."""
    return Mock()

@pytest.fixture
def jdbc_extractor(mock_spark):
    """Create a JDBCExtractor instance with mocked Spark."""
    credentials = {"user": "etl_user", "password": "etl_pass"}
    jdbc_url = "jdbc:oracle:thin:@core-db:1521/banking"
    return JDBCExtractor(mock_spark, jdbc_url, credentials)

def test_extract_full_table(jdbc_extractor, mock_spark):
    """Test extracting an entire table."""
    # Arrange
    mock_spark.read.format.return_value.option.return_value.option.return_value.option.return_value.option.return_value.load.return_value = MagicMock()

    # Act
    result = jdbc_extractor.extract_full_table("ACCOUNTS")

    # Assert
    mock_spark.read.format.assert_called_with("jdbc")
    assert result is not None

def test_extract_incremental(jdbc_extractor, mock_spark):
    """Test incremental extraction with partitioning."""
    # Arrange
    mock_spark.read.format.return_value.option.return_value.load.return_value = MagicMock()

    # Act
    result = jdbc_extractor.extract_incremental(
        "TRANSACTIONS",
        partition_column="transaction_id",
        lower_bound="1",
        upper_bound="1000000",
        num_partitions=4
    )

    # Assert
    assert result is not None

def test_extract_delta_load(jdbc_extractor, mock_spark):
    """Test delta load (incremental extraction)."""
    # Arrange
    last_load = datetime(2026, 5, 23, 0, 0, 0)
    mock_spark.read.format.return_value.option.return_value.load.return_value = MagicMock()

    # Act
    result = jdbc_extractor.extract_delta_load(
        "TRANSACTIONS",
        last_load_date=last_load,
        date_column="txn_date"
    )

    # Assert
    assert result is not None
