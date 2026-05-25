"""
Reusable Gold layer builder.

Usage (spark_python_task or CLI):
    python gold_builder.py --table dim_customer
    python gold_builder.py --all
    python gold_builder.py --config /path/to/gold_tables.yml --table fact_transactions

Two refresh methods per table (declared in gold_tables.yml):

  full_refresh:
    Executes a single CREATE OR REPLACE TABLE ... AS SELECT statement.
    Used for dimension tables derived from a current Silver/Bronze snapshot.

  incremental_merge:
    1. Read MAX(computed_at) from the existing Gold table as the watermark.
       Falls back to epoch ('1970-01-01 00:00:00') on first run → full backfill.
    2. CREATE TABLE IF NOT EXISTS from the columns declared in YAML config.
    3. Substitute ${watermark_ts} in the SQL string (Python-level, no SQL vars).
    4. Execute a single WITH ... MERGE INTO statement.

Both methods run OPTIMIZE ZORDER after each successful write.
"""
import argparse
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Optional

import yaml
from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("GoldBuilder")

DEFAULT_CONFIG = os.path.join(os.path.dirname(__file__), "../config/gold_tables.yml")
SQL_BASE = Path(__file__).parent.parent / "sql"


class GoldBuilder:
    def __init__(self, spark: SparkSession, config_path: str = DEFAULT_CONFIG):
        self.spark = spark
        with open(config_path) as f:
            self.config = yaml.safe_load(f)

    # ------------------------------------------------------------------
    # Watermark helpers
    # ------------------------------------------------------------------

    def _resolve_watermark(self, target_table: str) -> str:
        """Return MAX(computed_at) from the Gold table, or epoch if absent."""
        if not self.spark.catalog.tableExists(target_table):
            return "1970-01-01 00:00:00"
        row = self.spark.sql(
            f"SELECT MAX(computed_at) FROM {target_table}"
        ).collect()[0][0]
        return str(row) if row else "1970-01-01 00:00:00"

    # ------------------------------------------------------------------
    # Table creation (incremental_merge first run)
    # ------------------------------------------------------------------

    def _ensure_table(self, cfg: dict) -> None:
        """CREATE TABLE IF NOT EXISTS from the columns declared in YAML."""
        cols_ddl = ",\n      ".join(
            f"{c['name']} {c['type']}" for c in cfg["columns"]
        )
        partition_clause = (
            f"PARTITIONED BY ({cfg['partition_by']})"
            if cfg.get("partition_by")
            else ""
        )
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {cfg['target_table']} (
              {cols_ddl}
            )
            USING DELTA
            {partition_clause}
            TBLPROPERTIES (
              'delta.autoOptimize.optimizeWrite' = 'true',
              'delta.autoOptimize.autoCompact'   = 'true'
            )
        """)

    # ------------------------------------------------------------------
    # SQL execution
    # ------------------------------------------------------------------

    def _load_sql(self, sql_file: str, watermark_ts: Optional[str] = None) -> str:
        sql = (SQL_BASE / sql_file).read_text()
        if watermark_ts is not None:
            sql = sql.replace("'${watermark_ts}'", f"'{watermark_ts}'")
        return sql

    # ------------------------------------------------------------------
    # Refresh strategies
    # ------------------------------------------------------------------

    def _full_refresh(self, cfg: dict) -> None:
        sql = self._load_sql(cfg["sql_file"])
        self.spark.sql(sql)

    def _incremental_merge(self, cfg: dict) -> None:
        watermark_ts = self._resolve_watermark(cfg["target_table"])
        logger.info(f"[{cfg['name']}] watermark: {watermark_ts}")
        self._ensure_table(cfg)
        sql = self._load_sql(cfg["sql_file"], watermark_ts)
        self.spark.sql(sql)

    # ------------------------------------------------------------------
    # Post-load optimization
    # ------------------------------------------------------------------

    def _optimize(self, cfg: dict) -> None:
        table = cfg["target_table"]
        zorder_cols = cfg.get("optimize_zorder", [])
        if zorder_cols:
            cols = ", ".join(zorder_cols)
            self.spark.sql(f"OPTIMIZE {table} ZORDER BY ({cols})")
        else:
            self.spark.sql(f"OPTIMIZE {table}")

    # ------------------------------------------------------------------
    # Dispatch
    # ------------------------------------------------------------------

    def _dispatch(self, cfg: dict) -> None:
        name   = cfg["name"]
        method = cfg["method"]
        logger.info(f"[{name}] starting Gold build ({method})")

        if method == "full_refresh":
            self._full_refresh(cfg)
        elif method == "incremental_merge":
            self._incremental_merge(cfg)
        else:
            raise ValueError(f"[{name}] unknown method '{method}'")

        self._optimize(cfg)

        count = self.spark.table(cfg["target_table"]).count()
        logger.info(f"[{name}] ✓ {count:,} rows total")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def build(self, table_name: str) -> None:
        """Build a single Gold table by name (matches name: in YAML)."""
        matches = [t for t in self.config["tables"] if t["name"].lower() == table_name.lower()]
        if not matches:
            available = [t["name"] for t in self.config["tables"]]
            raise ValueError(f"Table '{table_name}' not found. Available: {available}")
        self._dispatch(matches[0])

    def build_all(self) -> None:
        """Build all Gold tables in declaration order."""
        for cfg in self.config["tables"]:
            self._dispatch(cfg)


def main():
    parser = argparse.ArgumentParser(description="Gold layer builder")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--table", help="Build a single Gold table by name")
    group.add_argument("--all", action="store_true", help="Build all configured tables")
    parser.add_argument("--config", default=DEFAULT_CONFIG, help="Path to gold_tables.yml")
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()
    builder = GoldBuilder(spark, config_path=args.config)

    if args.all:
        builder.build_all()
    else:
        builder.build(args.table)


if __name__ == "__main__":
    main()
