"""
Gold layer builder. Config-driven via gold_tables.yml.

Usage:
    python gold_builder.py --table dim_customer
    python gold_builder.py --all
"""
import argparse
import logging
import os
from pathlib import Path

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

    def _watermark(self, target_table: str) -> str:
        if not self.spark.catalog.tableExists(target_table):
            return "1970-01-01 00:00:00"
        row = self.spark.sql(f"SELECT MAX(computed_at) FROM {target_table}").collect()[0][0]
        return str(row) if row else "1970-01-01 00:00:00"

    def _ensure_table(self, cfg: dict) -> None:
        cols = ",\n  ".join(f"{c['name']} {c['type']}" for c in cfg["columns"])
        partition = f"PARTITIONED BY ({cfg['partition_by']})" if cfg.get("partition_by") else ""
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {cfg['target_table']} (
              {cols}
            )
            USING DELTA
            {partition}
            TBLPROPERTIES (
              'delta.autoOptimize.optimizeWrite' = 'true',
              'delta.autoOptimize.autoCompact' = 'true'
            )
        """)

    def _sql(self, sql_file: str, watermark_ts: str = None) -> str:
        content = (SQL_BASE / sql_file).read_text()
        if watermark_ts is not None:
            content = content.replace("'${watermark_ts}'", f"'{watermark_ts}'")
        return content

    def _optimize(self, cfg: dict) -> None:
        table = cfg["target_table"]
        cols = cfg.get("optimize_zorder", [])
        if cols:
            self.spark.sql(f"OPTIMIZE {table} ZORDER BY ({', '.join(cols)})")
        else:
            self.spark.sql(f"OPTIMIZE {table}")

    def _dispatch(self, cfg: dict) -> None:
        name = cfg["name"]
        method = cfg["method"]
        logger.info(f"[{name}] building ({method})")

        if method == "full_refresh":
            self.spark.sql(self._sql(cfg["sql_file"]))

        elif method == "incremental_merge":
            wm = self._watermark(cfg["target_table"])
            logger.info(f"[{name}] watermark: {wm}")
            self._ensure_table(cfg)
            self.spark.sql(self._sql(cfg["sql_file"], wm))

        else:
            raise ValueError(f"[{name}] unknown method '{method}'")

        self._optimize(cfg)
        count = self.spark.table(cfg["target_table"]).count()
        logger.info(f"[{name}] done — {count:,} rows")

    def build(self, table_name: str) -> None:
        matches = [t for t in self.config["tables"] if t["name"].lower() == table_name.lower()]
        if not matches:
            available = [t["name"] for t in self.config["tables"]]
            raise ValueError(f"'{table_name}' not found. Available: {available}")
        self._dispatch(matches[0])

    def build_all(self) -> None:
        for cfg in self.config["tables"]:
            self._dispatch(cfg)


def main():
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--table")
    group.add_argument("--all", action="store_true")
    parser.add_argument("--config", default=DEFAULT_CONFIG)
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()
    builder = GoldBuilder(spark, config_path=args.config)
    builder.build_all() if args.all else builder.build(args.table)


if __name__ == "__main__":
    main()
