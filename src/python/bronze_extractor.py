"""
Bronze layer extractor. Config-driven via bronze_tables.yml.
Supports full_snapshot and delta_load (checkpoint-based) per table.

Usage:
    python bronze_extractor.py --table ACCOUNTS
    python bronze_extractor.py --all
"""
import argparse
import logging
import os
from datetime import datetime, timedelta
from typing import Optional
from dotenv import load_dotenv

import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, max as spark_max

from src.python.config import get_oracle_credentials, Paths
from src.python.jdbc_extractor import JDBCExtractor

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("BronzeExtractor")

DEFAULT_CONFIG = os.path.join(os.path.dirname(__file__), "../config/bronze_tables.yml")
CHECKPOINT_TABLE = os.getenv("CHECKPOINT_METADATA_TABLE")


class BronzeExtractor:
    def __init__(self, spark: SparkSession, config_path: str = DEFAULT_CONFIG):
        self.spark = spark
        with open(config_path) as f:
            self.config = yaml.safe_load(f)
        creds = get_oracle_credentials()
        jdbc_url = f"jdbc:oracle:thin:@{creds['host']}:{creds['port']}/banking"
        self.extractor = JDBCExtractor(spark, jdbc_url, creds)

    def _read_checkpoint(self, table_name: str) -> Optional[datetime]:
        if not self.spark.catalog.tableExists(CHECKPOINT_TABLE):
            return None
        try:
            row = (
                self.spark.table(CHECKPOINT_TABLE)
                .filter(col("table_name") == table_name)
                .select("last_ingestion_timestamps")
                .orderBy(col("last_ingestion_timestamps").desc())
                .limit(1)
                .collect()
            )
            if row and row[0][0] is not None:
                return datetime.fromisoformat(str(row[0][0]))
        except Exception as e:
            logger.warning(f"[{table_name}] checkpoint read failed: {e}")
        return None

    def _write_checkpoint(self, table_name: str, max_ts: datetime) -> None:
        from delta.tables import DeltaTable

        new_row = self.spark.createDataFrame(
            [(table_name, max_ts, datetime.now())],
            ["table_name", "last_ingestion_timestamps", "updated_at"],
        )
        if self.spark.catalog.tableExists(CHECKPOINT_TABLE):
            (
                DeltaTable.forName(self.spark, CHECKPOINT_TABLE).alias("target")
                .merge(new_row.alias("source"), "target.table_name = source.table_name")
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )
        else:
            new_row.write.format("delta").mode("overwrite").saveAsTable(CHECKPOINT_TABLE)
        logger.info(f"[{table_name}] checkpoint updated to {max_ts.isoformat()}")

    def _build_predicates(self, timestamp_column: str, start_dt: datetime, end_dt: datetime, slices: int) -> list[str]:
        total_seconds = (end_dt - start_dt).total_seconds()
        slice_seconds = total_seconds / slices
        fmt = "YYYY-MM-DD HH24:MI:SS"
        predicates = []
        for i in range(slices):
            s = start_dt + timedelta(seconds=i * slice_seconds)
            e = end_dt if i == slices - 1 else start_dt + timedelta(seconds=(i + 1) * slice_seconds)
            op = "<=" if i == slices - 1 else "<"
            predicates.append(
                f"{timestamp_column} >= TO_DATE('{s.strftime('%Y-%m-%d %H:%M:%S')}', '{fmt}') "
                f"AND {timestamp_column} {op} TO_DATE('{e.strftime('%Y-%m-%d %H:%M:%S')}', '{fmt}')"
            )
        return predicates

    def _add_metadata(self, df, load_timestamp: datetime):
        return (
            df.withColumn("load_date", lit(load_timestamp.strftime("%Y-%m-%d")))
              .withColumn("load_timestamp", lit(load_timestamp.isoformat()))
        )

    def _extract_full_snapshot(self, cfg: dict) -> None:
        name = cfg["name"]
        bronze_table = cfg["bronze_table"]
        write_mode = cfg.get("write_mode", "overwrite")

        df = self.extractor.extract_full_table(name)
        load_ts = datetime.now()
        df = self._add_metadata(df, load_ts)
        count = df.count()

        df.write.format("delta").mode(write_mode).partitionBy("load_date").save(
            Paths.bronze_table(bronze_table)
        )
        logger.info(f"[{name}] {count} rows written ({write_mode})")

    def _extract_delta_load(self, cfg: dict) -> None:
        name = cfg["name"]
        bronze_table = cfg["bronze_table"]
        timestamp_column = cfg["timestamp_column"]
        slices = cfg.get("predicate_slices", 4)
        buffer_hours = cfg.get("checkpoint_buffer_hours", 12)

        last_checkpoint = self._read_checkpoint(name)

        if last_checkpoint is None:
            logger.info(f"[{name}] no checkpoint, full load")
            df = self.extractor.extract_full_table(name)
            write_mode = "overwrite"
        else:
            start_dt = last_checkpoint - timedelta(hours=buffer_hours)
            end_dt = datetime.now()
            logger.info(f"[{name}] delta load {start_dt} -> {end_dt} ({slices} slices)")
            predicates = self._build_predicates(timestamp_column, start_dt, end_dt, slices)
            df = self.extractor.extract_with_predicate(name, predicates)
            write_mode = cfg.get("write_mode", "append")

        load_ts = datetime.now()
        df = self._add_metadata(df, load_ts)
        count = df.count()

        output_path = Paths.bronze_table(bronze_table)
        df.write.format("delta").mode(write_mode).partitionBy("load_date").save(output_path)
        logger.info(f"[{name}] {count} rows written ({write_mode})")

        max_ts_row = (
            self.spark.read.format("delta").load(output_path)
            .filter(col("load_date") == load_ts.strftime("%Y-%m-%d"))
            .select(spark_max(col(timestamp_column)))
            .collect()[0][0]
        )
        if max_ts_row is not None:
            self._write_checkpoint(name, datetime.fromisoformat(str(max_ts_row)))
        else:
            logger.warning(f"[{name}] max({timestamp_column}) was NULL, checkpoint not updated")

    def extract(self, table_name: str) -> None:
        matches = [t for t in self.config["tables"] if t["name"].upper() == table_name.upper()]
        if not matches:
            available = [t["name"] for t in self.config["tables"]]
            raise ValueError(f"'{table_name}' not found. Available: {available}")
        self._dispatch(matches[0])

    def extract_all(self) -> None:
        for cfg in self.config["tables"]:
            self._dispatch(cfg)

    def _extract_partitioned_load(self, cfg: dict) -> None:
        name = cfg["name"]
        bronze_table = cfg["bronze_table"]
        partition_column = cfg["partition_column"]
        num_partitions = cfg.get("num_partitions", 8)

        # Derive bounds from Oracle if not specified — avoids hardcoding in YAML
        if "lower_bound" in cfg and "upper_bound" in cfg:
            lower_bound = str(cfg["lower_bound"])
            upper_bound = str(cfg["upper_bound"])
        else:
            bounds_df = self.extractor.extract_full_table(
                f"(SELECT MIN({partition_column}) AS lb, MAX({partition_column}) AS ub FROM {name})"
            )
            row = bounds_df.collect()[0]
            lower_bound = str(row["lb"])
            upper_bound = str(row["ub"])

        df = self.extractor.extract_incremental(
            table_name=name,
            partition_column=partition_column,
            lower_bound=lower_bound,
            upper_bound=upper_bound,
            num_partitions=num_partitions,
        )
        load_ts = datetime.now()
        df = self._add_metadata(df, load_ts)
        count = df.count()
        write_mode = cfg.get("write_mode", "overwrite")
        df.write.format("delta").mode(write_mode).partitionBy("load_date").save(
            Paths.bronze_table(bronze_table)
        )
        logger.info(f"[{name}] {count} rows written ({write_mode}, {num_partitions} partitions)")

    def _dispatch(self, cfg: dict) -> None:
        method = cfg["method"]
        if method == "full_snapshot":
            self._extract_full_snapshot(cfg)
        elif method == "delta_load":
            self._extract_delta_load(cfg)
        elif method == "partitioned_load":
            self._extract_partitioned_load(cfg)
        else:
            raise ValueError(f"unknown method '{method}' for {cfg['name']}")


def main():
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--table")
    group.add_argument("--all", action="store_true")
    parser.add_argument("--config", default=DEFAULT_CONFIG)
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()
    extractor = BronzeExtractor(spark, config_path=args.config)
    extractor.extract_all() if args.all else extractor.extract(args.table)


if __name__ == "__main__":
    main()
