from typing import Optional
from datetime import datetime, timedelta
from pyspark.sql import SparkSession, DataFrame

class JDBCExtractor:
    """Utility for extracting data from Oracle via JDBC."""

    def __init__(self, spark: SparkSession, jdbc_url: str, credentials: dict):
        """
        Initialize JDBC extractor.

        Args:
            spark: SparkSession
            jdbc_url: JDBC connection string (e.g., jdbc:oracle:thin:@host:1521/db)
            credentials: Dict with 'user' and 'password'
        """
        self.spark = spark
        self.jdbc_url = jdbc_url
        self.credentials = credentials

    def extract_full_table(self, table_name: str) -> DataFrame:
        """Extract entire table from Oracle."""
        return self.spark.read.format("jdbc") \
            .option("url", self.jdbc_url) \
            .option("dbtable", table_name) \
            .option("user", self.credentials["user"]) \
            .option("password", self.credentials["password"]) \
            .option("driver", "oracle.jdbc.driver.OracleDriver") \
            .load()

    def extract_incremental(
        self,
        table_name: str,
        partition_column: str,
        lower_bound: str,
        upper_bound: str,
        num_partitions: int = 4,
    ) -> DataFrame:
        """
        Extract data with partitioned read (parallel JDBC connections).

        Args:
            table_name: Oracle table name
            partition_column: Column to partition on (must be numeric)
            lower_bound: Lower bound of partition column
            upper_bound: Upper bound of partition column
            num_partitions: Number of parallel JDBC connections

        Returns:
            DataFrame with data
        """
        return self.spark.read.format("jdbc") \
            .option("url", self.jdbc_url) \
            .option("dbtable", table_name) \
            .option("user", self.credentials["user"]) \
            .option("password", self.credentials["password"]) \
            .option("driver", "oracle.jdbc.driver.OracleDriver") \
            .option("partitionColumn", partition_column) \
            .option("lowerBound", lower_bound) \
            .option("upperBound", upper_bound) \
            .option("numPartitions", num_partitions) \
            .load()

    def extract_with_predicate(self, table_name: str, predicates: list) -> DataFrame:
        """
        Extract data using custom SQL predicates (for time-sliced parallel loading).

        Args:
            table_name: Oracle table name
            predicates: List of WHERE clause conditions

        Returns:
            Union of DataFrames from each predicate
        """
        dfs = []
        for predicate in predicates:
            df = self.spark.read.format("jdbc") \
                .option("url", self.jdbc_url) \
                .option("dbtable", f"({table_name} WHERE {predicate}) t") \
                .option("user", self.credentials["user"]) \
                .option("password", self.credentials["password"]) \
                .option("driver", "oracle.jdbc.driver.OracleDriver") \
                .load()
            dfs.append(df)

        return dfs[0] if len(dfs) == 1 else dfs[0].union(*dfs[1:])

    def extract_delta_load(
        self,
        table_name: str,
        last_load_date: datetime,
        date_column: str = "modified_date",
    ) -> DataFrame:
        """
        Extract only rows modified since last load (incremental/delta loading).

        Args:
            table_name: Oracle table name
            last_load_date: Last successful load timestamp
            date_column: Column name for filtering (default: modified_date)

        Returns:
            DataFrame with new/modified rows only
        """
        # Cast to proper timestamp format for Oracle
        formatted_date = last_load_date.strftime("%Y-%m-%d %H:%M:%S")
        predicate = f"{date_column} >= TO_DATE('{formatted_date}', 'YYYY-MM-DD HH24:MI:SS')"

        return self.spark.read.format("jdbc") \
            .option("url", self.jdbc_url) \
            .option("dbtable", f"({table_name} WHERE {predicate}) t") \
            .option("user", self.credentials["user"]) \
            .option("password", self.credentials["password"]) \
            .option("driver", "oracle.jdbc.driver.OracleDriver") \
            .load()
