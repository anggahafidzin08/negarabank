from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, when, isnull, max as spark_max, min as spark_min
from typing import List, Dict, Any
from datetime import datetime
import json

class DataQualityCheck:
    """Represents a single DQ check."""

    def __init__(self, name: str, description: str, table: str, sql_query: str):
        self.name = name
        self.description = description
        self.table = table
        self.sql_query = sql_query
        self.passed = False
        self.record_count = 0
        self.timestamp = datetime.now().isoformat()

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "description": self.description,
            "table": self.table,
            "passed": self.passed,
            "record_count": self.record_count,
            "timestamp": self.timestamp,
        }

class DataQualityFramework:
    """Framework for running DQ checks and logging results."""

    def __init__(self, spark: SparkSession, output_path: str):
        self.spark = spark
        self.output_path = output_path
        self.checks: List[DataQualityCheck] = []
        self.results = []

    def add_null_check(self, table: str, column: str, threshold: float = 0.05) -> DataQualityCheck:
        """
        Check that null % does not exceed threshold.

        Args:
            table: Table name
            column: Column to check
            threshold: Max allowed null % (default 5%)
        """
        sql = f"""
        SELECT
            '{column}' as column_name,
            ROUND(COUNT(CASE WHEN {column} IS NULL THEN 1 END) / COUNT(*), 4) as null_pct,
            COUNT(*) as total_rows
        FROM {table}
        """

        check = DataQualityCheck(
            name=f"{table}__{column}__null_check",
            description=f"Null check: {column} in {table} (threshold: {threshold*100}%)",
            table=table,
            sql_query=sql
        )
        self.checks.append(check)
        return check

    def add_uniqueness_check(self, table: str, column: str) -> DataQualityCheck:
        """Check for duplicate values (should be unique)."""
        sql = f"""
        SELECT
            '{column}' as column_name,
            COUNT(*) as total_rows,
            COUNT(DISTINCT {column}) as distinct_rows,
            COUNT(*) - COUNT(DISTINCT {column}) as duplicate_count
        FROM {table}
        """

        check = DataQualityCheck(
            name=f"{table}__{column}__uniqueness_check",
            description=f"Uniqueness check: {column} in {table}",
            table=table,
            sql_query=sql
        )
        self.checks.append(check)
        return check

    def add_referential_integrity_check(
        self,
        child_table: str,
        child_column: str,
        parent_table: str,
        parent_column: str,
    ) -> DataQualityCheck:
        """Check for orphaned records (FK references non-existent parent)."""
        sql = f"""
        SELECT
            COUNT(*) as orphan_count,
            ROUND(COUNT(*) / (SELECT COUNT(*) FROM {child_table}), 4) as orphan_pct
        FROM {child_table} c
        WHERE c.{child_column} NOT IN (SELECT {parent_column} FROM {parent_table})
        """

        check = DataQualityCheck(
            name=f"{child_table}__{child_column}__fk_check",
            description=f"FK check: {child_table}.{child_column} → {parent_table}.{parent_column}",
            table=child_table,
            sql_query=sql
        )
        self.checks.append(check)
        return check

    def run_checks(self, fail_fast: bool = False) -> Dict[str, Any]:
        """
        Execute all registered checks.

        Args:
            fail_fast: Stop on first failure (for critical checks)

        Returns:
            Summary dict with pass/fail counts
        """
        summary = {
            "total_checks": len(self.checks),
            "passed": 0,
            "failed": 0,
            "timestamp": datetime.now().isoformat(),
            "results": []
        }

        for check in self.checks:
            try:
                result_df = self.spark.sql(check.sql_query)
                result_rows = result_df.collect()

                check.passed = True
                check.record_count = len(result_rows)

                summary["passed"] += 1
                summary["results"].append(check.to_dict())

                print(f"✓ {check.name}")

            except Exception as e:
                check.passed = False
                summary["failed"] += 1
                summary["results"].append({
                    **check.to_dict(),
                    "error": str(e)
                })

                print(f"✗ {check.name}: {str(e)}")

                if fail_fast:
                    break

        # Log results
        self._log_results(summary)

        return summary

    def _log_results(self, summary: Dict[str, Any]):
        """Save DQ results to Delta table."""
        from pyspark.sql import functions as F

        results_df = self.spark.createDataFrame(
            [(r["name"], r["description"], r["passed"], r["timestamp"])
             for r in summary["results"]],
            ["check_name", "description", "passed", "check_timestamp"]
        )

        results_df.write \
            .format("delta") \
            .mode("append") \
            .save(f"{self.output_path}/dq_check_results/")

        print(f"\n✓ DQ results logged to {self.output_path}/dq_check_results/")
