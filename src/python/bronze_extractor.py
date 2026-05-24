"""
Reusable Bronze layer extractor.

Usage (spark_python_task or CLI):
    python bronze_extractor.py --table ACCOUNTS
    python bronze_extractor.py --all
    python bronze_extractor.py --config /path/to/bronze_tables.yml --table TRANSACTIONS
"""
import argparse
import logging
import os
from datetime import datetime, timedelta
from typing import Optional

import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, max as spark_max

from src.python.config import get_oracle_credentials, Paths
from src.python.jdbc_extractor import JDBCExtractor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("BronzeExtractor")

DEFAULT_CONFIG = os.path.join(os.path.dirname(__file__), "../config/bronze_tables.yml")


class BronzeExtractor:
    def __init__(self, spark: SparkSession, config_path: str = DEFAULT_CONFIG):
        self.spark = spark
        self.config = self._load_config(config_path)
        creds = get_oracle_credentials()
        jdbc_url = f"jdbc:oracle:thin:@{creds['host']}:{creds['port']}/banking"
        self.extractor = JDBCExtractor(spark, jdbc_url, creds)

    def _load_config(self, path: str) -> dict:
        with open(path, "r") as f:
            return yaml.safe_load(f)

    def _get_last_load_date(self, bronze_table: str, lookback_days: int) -> datetime:
        """Return last successful load date, or fallback to N-day lookback."""
        try:
            if self.spark.catalog.tableExists("bronze", bronze_table):
                row = (
                    self.spark.table(f"bronze.{bronze_table}")
                    .select(spark_max(col("load_date")))
                    .collect()[0][0]
                )
                return datetime.fromisoformat(str(row).split()[0])
        except Exception as e:
            logger.warning(f"Could not read last load date for {bronze_table}: {e}")
        logger.info(f"Defaulting to {lookback_days}-day lookback for {bronze_table}")
        return datetime.now() - timedelta(days=lookback_days)

    def _build_predicates(self, date_column: str, target_date: str, slices: int) -> list[str]:
        """Split a single day into N equal time-window predicates for parallel JDBC reads."""
        hours_per_slice = 24 // slices
        predicates = []
        for i in range(slices):
            start_h = i * hours_per_slice
            end_h = (i + 1) * hours_per_slice
            start = f"{target_date} {start_h:02d}:00:00"
            # Last slice ends at 23:59:59 to avoid overlap
            end = f"{target_date} 23:59:59" if end_h == 24 else f"{target_date} {end_h:02d}:00:00"
            fmt = "YYYY-MM-DD HH24:MI:SS"
            op = "<=" if end_h == 24 else "<"
            predicates.append(
                f"{date_column} >= TO_DATE('{start}', '{fmt}') "
                f"AND {date_column} {op} TO_DATE('{end}', '{fmt}')"
            )
        return predicates

    def _add_metadata(self, df, load_timestamp: datetime):
        return (
            df.withColumn("load_date", lit(load_timestamp.strftime("%Y-%m-%d")))
              .withColumn("load_timestamp", lit(load_timestamp.isoformat()))
        )

    def _extract_full_snapshot(self, table_cfg: dict) -> None:
        name = table_cfg["name"]
        bronze_table = table_cfg["bronze_table"]
        write_mode = table_cfg.get("write_mode", "overwrite")

        logger.info(f"[{name}] full_snapshot extraction starting")
        df = self.extractor.extract_full_table(name)
        df = self._add_metadata(df, datetime.now())
        record_count = df.count()

        df.write.format("delta").mode(write_mode).partitionBy("load_date").save(
            Paths.bronze_table(bronze_table)
        )
        logger.info(f"[{name}] ✓ {record_count} records → bronze.{bronze_table} ({write_mode})")

    def _extract_delta_load(self, table_cfg: dict) -> None:
        name = table_cfg["name"]
        bronze_table = table_cfg["bronze_table"]
        date_column = table_cfg["date_column"]
        lookback_days = table_cfg.get("lookback_days", 30)
        slices = table_cfg.get("predicate_slices", 4)
        write_mode = table_cfg.get("write_mode", "append")

        last_load_dt = self._get_last_load_date(bronze_table, lookback_days)
        target_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        logger.info(f"[{name}] delta_load: {last_load_dt.date()} → {target_date} ({slices} slices)")

        predicates = self._build_predicates(date_column, target_date, slices)
        df = self.extractor.extract_with_predicate(name, predicates)

        load_ts = datetime.now()
        df = self._add_metadata(df, load_ts)
        record_count = df.count()

        df.write.format("delta").mode(write_mode).partitionBy("load_date").save(
            Paths.bronze_table(bronze_table)
        )
        logger.info(f"[{name}] ✓ {record_count} records → bronze.{bronze_table} ({write_mode})")

    def extract(self, table_name: str) -> None:
        """Extract a single table by its Oracle name (case-insensitive)."""
        matches = [
            t for t in self.config["tables"]
            if t["name"].upper() == table_name.upper()
        ]
        if not matches:
            raise ValueError(
                f"Table '{table_name}' not found in config. "
                f"Available: {[t['name'] for t in self.config['tables']]}"
            )
        self._dispatch(matches[0])

    def extract_all(self) -> None:
        """Extract all tables defined in config, in order."""
        for table_cfg in self.config["tables"]:
            self._dispatch(table_cfg)

    def _dispatch(self, table_cfg: dict) -> None:
        method = table_cfg["method"]
        if method == "full_snapshot":
            self._extract_full_snapshot(table_cfg)
        elif method == "delta_load":
            self._extract_delta_load(table_cfg)
        else:
            raise ValueError(f"Unknown method '{method}' for table {table_cfg['name']}")


def main():
    parser = argparse.ArgumentParser(description="Bronze layer extractor")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--table", help="Extract a single Oracle table by name")
    group.add_argument("--all", action="store_true", help="Extract all configured tables")
    parser.add_argument("--config", default=DEFAULT_CONFIG, help="Path to bronze_tables.yml")
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()
    extractor = BronzeExtractor(spark, config_path=args.config)

    if args.all:
        extractor.extract_all()
    else:
        extractor.extract(args.table)


if __name__ == "__main__":
    main()
