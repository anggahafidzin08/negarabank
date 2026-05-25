"""
Reusable Silver layer transformer with SCD Type 2.

Usage (spark_python_task or CLI):
    python silver_transformer.py --table accounts
    python silver_transformer.py --all
    python silver_transformer.py --config /path/to/silver_tables.yml --table transactions

SCD2 flow per table:
    1. Execute transform_sql → incoming DataFrame (deduped, typed, enriched).
    2. Run DQ checks; log results (fail_fast=False so pipeline continues).
    3. Compute _record_hash = MD5(tracked_columns) for change detection.
    4. If target table does not exist → first load: add SCD2 columns, saveAsTable.
    5. If target table exists:
         a. MERGE to expire old versions of changed records (is_current → false).
         b. INSERT new versions for changed + new business keys (is_current = true).
    6. Unchanged records (same key, same hash) produce no writes.
"""
import argparse
import logging
import os
from datetime import datetime
from typing import Optional

import yaml
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, concat_ws, lit, md5

from src.python.dq_framework import DataQualityFramework
from src.python.config import Paths

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("SilverTransformer")

DEFAULT_CONFIG = os.path.join(os.path.dirname(__file__), "../config/silver_tables.yml")


class SilverTransformer:
    def __init__(self, spark: SparkSession, config_path: str = DEFAULT_CONFIG):
        self.spark = spark
        self.config = self._load_config(config_path)

    def _load_config(self, path: str) -> dict:
        with open(path, "r") as f:
            return yaml.safe_load(f)

    # ------------------------------------------------------------------
    # SQL transformation
    # ------------------------------------------------------------------

    def _run_sql(self, sql: str) -> DataFrame:
        """Execute transform_sql against Unity Catalog and return a DataFrame."""
        return self.spark.sql(sql)

    # ------------------------------------------------------------------
    # Data quality
    # ------------------------------------------------------------------

    def _run_dq(self, df: DataFrame, dq_cfg: dict, table_name: str) -> None:
        """Run DQ checks from config. Logs results; does not raise on failure."""
        dq = DataQualityFramework(self.spark, Paths.S3["silver"])

        for column in dq_cfg.get("not_null", []):
            dq.add_null_check(table_name, column, threshold=0.0)

        for column in dq_cfg.get("unique", []):
            dq.add_uniqueness_check(table_name, column)

        results = dq.run_checks(fail_fast=False)
        passed = results.get("passed", 0)
        failed = results.get("failed", 0)
        logger.info(f"[{table_name}] DQ: {passed} passed, {failed} failed")
        if failed:
            logger.warning(f"[{table_name}] DQ failures detected — review results before promotion")

    # ------------------------------------------------------------------
    # SCD2 helpers
    # ------------------------------------------------------------------

    def _add_record_hash(self, df: DataFrame, tracked_columns: list) -> DataFrame:
        """Add _record_hash column: MD5 of tracked columns cast to string."""
        return df.withColumn(
            "_record_hash",
            md5(concat_ws("|", *[col(c).cast("string") for c in tracked_columns])),
        )

    def _add_scd2_columns(
        self, df: DataFrame, effective_date_col: str, is_current: bool = True
    ) -> DataFrame:
        """Append SCD2 bookkeeping columns to a DataFrame."""
        end_date = lit(None).cast("string")
        return (
            df.withColumn("effective_start_date", col(effective_date_col))
              .withColumn("effective_end_date", end_date)
              .withColumn("is_current", lit(is_current))
        )

    def _apply_scd2(self, incoming_df: DataFrame, table_cfg: dict) -> None:
        """
        Merge incoming_df into the Silver Delta table using SCD Type 2 logic.

        Step A (first load): saveAsTable with all rows marked current.
        Step B (incremental):
          1. Identify changed records (same biz_key, different _record_hash).
          2. MERGE to expire old versions (is_current=false, effective_end_date=today).
          3. INSERT new/changed versions as current rows.
        """
        from delta.tables import DeltaTable

        target_table = table_cfg["target_table"]
        business_key = table_cfg["business_key"]
        tracked_cols = table_cfg["scd2"]["tracked_columns"]
        effective_date_col = table_cfg["scd2"]["effective_date_column"]

        hashed_df = self._add_record_hash(incoming_df, tracked_cols)
        today = datetime.now().strftime("%Y-%m-%d")

        # ── First load ────────────────────────────────────────────────
        if not self.spark.catalog.tableExists(target_table):
            logger.info(f"[{target_table}] First load — writing all rows as current")
            (
                self._add_scd2_columns(hashed_df, effective_date_col)
                .drop("_record_hash")
                .write.format("delta")
                .mode("overwrite")
                .saveAsTable(target_table)
            )
            count = self.spark.table(target_table).count()
            logger.info(f"[{target_table}] ✓ {count} records inserted (initial load)")
            return

        # ── Incremental: detect changes ───────────────────────────────
        current_df = (
            self.spark.table(target_table)
            .filter(col("is_current") == True)
            .select(business_key, "_record_hash")
        )

        # Changed: biz_key already in Silver but tracked columns differ
        changed_df = (
            hashed_df.alias("new")
            .join(current_df.alias("cur"), business_key, "inner")
            .where(col("new._record_hash") != col("cur._record_hash"))
            .select("new.*")
        )

        # New: biz_key not yet in Silver at all
        new_df = (
            hashed_df.alias("new")
            .join(current_df.alias("cur"), business_key, "left_anti")
        )

        to_insert = changed_df.union(new_df)
        insert_count = to_insert.count()

        if insert_count == 0:
            logger.info(f"[{target_table}] No changes detected — skipping write")
            return

        # ── Step 1: expire old versions of changed records ────────────
        expire_keys = changed_df.select(
            col(business_key),
            col(effective_date_col).alias("_expire_date"),
        )

        (
            DeltaTable.forName(self.spark, target_table)
            .alias("target")
            .merge(
                expire_keys.alias("src"),
                f"target.{business_key} = src.{business_key} AND target.is_current = true",
            )
            .whenMatchedUpdate(
                set={
                    "effective_end_date": "src._expire_date",
                    "is_current": lit(False),
                }
            )
            .execute()
        )

        expired_count = changed_df.count()
        logger.info(f"[{target_table}] Expired {expired_count} old version(s)")

        # ── Step 2: insert new/changed records as current versions ─────
        (
            self._add_scd2_columns(to_insert, effective_date_col)
            .drop("_record_hash")
            .write.format("delta")
            .mode("append")
            .saveAsTable(target_table)
        )

        logger.info(
            f"[{target_table}] ✓ SCD2 complete: "
            f"{expired_count} expired, {insert_count} inserted"
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def transform(self, table_name: str) -> None:
        """Transform a single Silver table by name (matches name: in YAML)."""
        matches = [
            t for t in self.config["tables"]
            if t["name"].lower() == table_name.lower()
        ]
        if not matches:
            available = [t["name"] for t in self.config["tables"]]
            raise ValueError(
                f"Table '{table_name}' not found in config. Available: {available}"
            )
        self._dispatch(matches[0])

    def transform_all(self) -> None:
        """Transform all Silver tables in declaration order."""
        for table_cfg in self.config["tables"]:
            self._dispatch(table_cfg)

    def _dispatch(self, table_cfg: dict) -> None:
        name = table_cfg["name"]
        logger.info(f"[{name}] Starting Silver transformation")

        # 1. Execute SQL transformation
        incoming_df = self._run_sql(table_cfg["transform_sql"])

        # 2. DQ checks (non-blocking)
        if "dq" in table_cfg:
            self._run_dq(incoming_df, table_cfg["dq"], name)

        # 3. SCD2 merge into Silver Delta table
        self._apply_scd2(incoming_df, table_cfg)


def main():
    parser = argparse.ArgumentParser(description="Silver layer transformer (SCD2)")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--table", help="Transform a single Silver table by name")
    group.add_argument("--all", action="store_true", help="Transform all configured tables")
    parser.add_argument("--config", default=DEFAULT_CONFIG, help="Path to silver_tables.yml")
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()
    transformer = SilverTransformer(spark, config_path=args.config)

    if args.all:
        transformer.transform_all()
    else:
        transformer.transform(args.table)


if __name__ == "__main__":
    main()
