"""
Silver layer transformer. Config-driven via silver_tables.yml.
Applies SCD Type 2 using a record hash to detect changes.

Usage:
    python silver_transformer.py --table accounts
    python silver_transformer.py --all
"""
import argparse
import logging
import os
from datetime import datetime

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
        with open(config_path) as f:
            self.config = yaml.safe_load(f)

    def _run_sql(self, sql: str) -> DataFrame:
        return self.spark.sql(sql)

    def _run_dq(self, df: DataFrame, dq_cfg: dict, table_name: str) -> None:
        dq = DataQualityFramework(self.spark, Paths.S3["silver"])
        for col_name in dq_cfg.get("not_null", []):
            dq.add_null_check(table_name, col_name, threshold=0.0)
        for col_name in dq_cfg.get("unique", []):
            dq.add_uniqueness_check(table_name, col_name)
        results = dq.run_checks(fail_fast=False)
        logger.info(f"[{table_name}] DQ: {results.get('passed', 0)} passed, {results.get('failed', 0)} failed")

    def _record_hash(self, df: DataFrame, tracked_columns: list) -> DataFrame:
        return df.withColumn(
            "_record_hash",
            md5(concat_ws("|", *[col(c).cast("string") for c in tracked_columns])),
        )

    def _scd2_columns(self, df: DataFrame, effective_date_col: str, is_current: bool = True) -> DataFrame:
        return (
            df.withColumn("effective_start_date", col(effective_date_col))
              .withColumn("effective_end_date", lit(None).cast("string"))
              .withColumn("is_current", lit(is_current))
        )

    def _apply_scd2(self, incoming_df: DataFrame, cfg: dict) -> None:
        from delta.tables import DeltaTable

        target_table = cfg["target_table"]
        business_key = cfg["business_key"]
        tracked_cols = cfg["scd2"]["tracked_columns"]
        effective_date_col = cfg["scd2"]["effective_date_column"]

        hashed = self._record_hash(incoming_df, tracked_cols)
        today = datetime.now().strftime("%Y-%m-%d")

        if not self.spark.catalog.tableExists(target_table):
            logger.info(f"[{target_table}] first load")
            (
                self._scd2_columns(hashed, effective_date_col)
                .drop("_record_hash")
                .write.format("delta").mode("overwrite").saveAsTable(target_table)
            )
            logger.info(f"[{target_table}] {self.spark.table(target_table).count()} rows inserted")
            return

        current = (
            self.spark.table(target_table)
            .filter(col("is_current") == True)
            .select(business_key, "_record_hash")
        )

        changed = (
            hashed.alias("new")
            .join(current.alias("cur"), business_key, "inner")
            .where(col("new._record_hash") != col("cur._record_hash"))
            .select("new.*")
        )

        new_records = hashed.alias("new").join(current.alias("cur"), business_key, "left_anti")
        to_insert = changed.union(new_records)
        insert_count = to_insert.count()

        if insert_count == 0:
            logger.info(f"[{target_table}] no changes")
            return

        # Expire changed records
        expire_keys = changed.select(col(business_key), col(effective_date_col).alias("_expire_date"))
        (
            DeltaTable.forName(self.spark, target_table).alias("target")
            .merge(expire_keys.alias("src"), f"target.{business_key} = src.{business_key} AND target.is_current = true")
            .whenMatchedUpdate(set={"effective_end_date": "src._expire_date", "is_current": lit(False)})
            .execute()
        )

        # Insert new versions
        (
            self._scd2_columns(to_insert, effective_date_col)
            .drop("_record_hash")
            .write.format("delta").mode("append").saveAsTable(target_table)
        )

        logger.info(f"[{target_table}] {changed.count()} expired, {insert_count} inserted")

    def transform(self, table_name: str) -> None:
        matches = [t for t in self.config["tables"] if t["name"].lower() == table_name.lower()]
        if not matches:
            available = [t["name"] for t in self.config["tables"]]
            raise ValueError(f"'{table_name}' not found. Available: {available}")
        self._dispatch(matches[0])

    def transform_all(self) -> None:
        for cfg in self.config["tables"]:
            self._dispatch(cfg)

    def _dispatch(self, cfg: dict) -> None:
        name = cfg["name"]
        logger.info(f"[{name}] transforming")
        incoming = self._run_sql(cfg["transform_sql"])
        if "dq" in cfg:
            self._run_dq(incoming, cfg["dq"], name)
        self._apply_scd2(incoming, cfg)


def main():
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--table")
    group.add_argument("--all", action="store_true")
    parser.add_argument("--config", default=DEFAULT_CONFIG)
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()
    transformer = SilverTransformer(spark, config_path=args.config)
    transformer.transform_all() if args.all else transformer.transform(args.table)


if __name__ == "__main__":
    main()
