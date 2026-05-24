"""
Reusable Bronze layer extractor.

Usage (spark_python_task or CLI):
    python bronze_extractor.py --table ACCOUNTS
    python bronze_extractor.py --all
    python bronze_extractor.py --config /path/to/bronze_tables.yml --table TRANSACTIONS

Delta load flow (checkpoint-based):
    1. Read checkpoint_bronze_metadata for this table_name.
    2. No checkpoint → full load (overwrite), then write checkpoint.
    3. Checkpoint exists → extract from (last_ingestion_timestamps - buffer_hours) to now,
       append to Bronze, then update checkpoint with new max(timestamp_column).
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

load_dotenv()  # Load environment variables from .env file
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("BronzeExtractor")

DEFAULT_CONFIG = os.path.join(os.path.dirname(__file__), "../config/bronze_tables.yml")
CHECKPOINT_TABLE = os.getenv("CHECKPOINT_METADATA_TABLE")


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

    # ------------------------------------------------------------------
    # Checkpoint helpers
    # ------------------------------------------------------------------

    def _read_checkpoint(self, table_name: str) -> Optional[datetime]:
        """Return last_ingestion_timestamps for table_name, or None if not found.

        Uses spark.catalog.tableExists(CHECKPOINT_TABLE) where CHECKPOINT_TABLE is the
        fully-qualified Unity Catalog name (catalog.schema.table), e.g.
        'negarabank.bronze.checkpoint_bronze_metadata'. This avoids fragile S3-path
        probing and works regardless of the underlying storage location.
        """
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
            logger.warning(f"[{table_name}] Could not read checkpoint: {e}")
        return None

    def _write_checkpoint(self, table_name: str, max_ts: datetime) -> None:
        """Upsert last_ingestion_timestamps into the checkpoint table.

        First write uses saveAsTable to register in Unity Catalog.
        Subsequent writes use DeltaTable.forName for a proper MERGE upsert.
        """
        from delta.tables import DeltaTable

        new_row = self.spark.createDataFrame(
            [(table_name, max_ts, datetime.now())],
            ["table_name", "last_ingestion_timestamps", "updated_at"],
        )

        if self.spark.catalog.tableExists(CHECKPOINT_TABLE):
            (
                DeltaTable.forName(self.spark, CHECKPOINT_TABLE)
                .alias("target")
                .merge(new_row.alias("source"), "target.table_name = source.table_name")
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )
        else:
            # First-ever write: register table in Unity Catalog via saveAsTable
            new_row.write.format("delta").mode("overwrite").saveAsTable(CHECKPOINT_TABLE)

        logger.info(f"[{table_name}] Checkpoint updated → {max_ts.isoformat()}")

    # ------------------------------------------------------------------
    # Predicate builder (datetime range → N parallel JDBC slices)
    # ------------------------------------------------------------------

    def _build_predicates(
        self,
        timestamp_column: str,
        start_dt: datetime,
        end_dt: datetime,
        slices: int,
    ) -> list[str]:
        """Split [start_dt, end_dt) into N equal time windows for parallel JDBC reads."""
        total_seconds = (end_dt - start_dt).total_seconds()
        slice_seconds = total_seconds / slices
        fmt = "YYYY-MM-DD HH24:MI:SS"
        predicates = []

        for i in range(slices):
            slice_start = start_dt + timedelta(seconds=i * slice_seconds)
            slice_end = end_dt if i == slices - 1 else start_dt + timedelta(seconds=(i + 1) * slice_seconds)
            op = "<=" if i == slices - 1 else "<"
            predicates.append(
                f"{timestamp_column} >= TO_DATE('{slice_start.strftime('%Y-%m-%d %H:%M:%S')}', '{fmt}') "
                f"AND {timestamp_column} {op} TO_DATE('{slice_end.strftime('%Y-%m-%d %H:%M:%S')}', '{fmt}')"
            )

        return predicates

    # ------------------------------------------------------------------
    # Metadata helper
    # ------------------------------------------------------------------

    def _add_metadata(self, df, load_timestamp: datetime):
        return (
            df.withColumn("load_date", lit(load_timestamp.strftime("%Y-%m-%d")))
              .withColumn("load_timestamp", lit(load_timestamp.isoformat()))
        )

    # ------------------------------------------------------------------
    # Extraction strategies
    # ------------------------------------------------------------------

    def _extract_full_snapshot(self, table_cfg: dict) -> None:
        name = table_cfg["name"]
        bronze_table = table_cfg["bronze_table"]
        write_mode = table_cfg.get("write_mode", "overwrite")

        logger.info(f"[{name}] full_snapshot starting")
        df = self.extractor.extract_full_table(name)
        load_ts = datetime.now()
        df = self._add_metadata(df, load_ts)
        record_count = df.count()

        df.write.format("delta").mode(write_mode).partitionBy("load_date").save(
            Paths.bronze_table(bronze_table)
        )
        logger.info(f"[{name}] ✓ {record_count} records → bronze.{bronze_table} ({write_mode})")

    def _extract_delta_load(self, table_cfg: dict) -> None:
        name = table_cfg["name"]
        bronze_table = table_cfg["bronze_table"]
        timestamp_column = table_cfg["timestamp_column"]
        slices = table_cfg.get("predicate_slices", 4)
        buffer_hours = table_cfg.get("checkpoint_buffer_hours", 12)

        # ── Step 1: Resolve extraction window from checkpoint ──────────
        last_checkpoint = self._read_checkpoint(name)

        if last_checkpoint is None:
            logger.info(f"[{name}] No checkpoint found → performing full load")
            df = self.extractor.extract_full_table(name)
            write_mode = "overwrite"
        else:
            start_dt = last_checkpoint - timedelta(hours=buffer_hours)
            end_dt = datetime.now()
            logger.info(
                f"[{name}] delta_load: {start_dt} → {end_dt} "
                f"({slices} slices, {buffer_hours}h buffer)"
            )
            predicates = self._build_predicates(timestamp_column, start_dt, end_dt, slices)
            df = self.extractor.extract_with_predicate(name, predicates)
            write_mode = table_cfg.get("write_mode", "append")

        # ── Step 2: Add metadata + write ──────────────────────────────
        load_ts = datetime.now()
        df = self._add_metadata(df, load_ts)
        record_count = df.count()

        output_path = Paths.bronze_table(bronze_table)
        df.write.format("delta").mode(write_mode).partitionBy("load_date").save(output_path)
        logger.info(f"[{name}] ✓ {record_count} records → bronze.{bronze_table} ({write_mode})")

        # ── Step 3: Update checkpoint from written Delta table ─────────
        max_ts_row = (
            self.spark.read.format("delta").load(output_path)
            .filter(col("load_date") == load_ts.strftime("%Y-%m-%d"))
            .select(spark_max(col(timestamp_column)))
            .collect()[0][0]
        )
        if max_ts_row is not None:
            self._write_checkpoint(name, datetime.fromisoformat(str(max_ts_row)))
        else:
            logger.warning(f"[{name}] max({timestamp_column}) was NULL — checkpoint not updated")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def extract(self, table_name: str) -> None:
        """Extract a single table by its Oracle name (case-insensitive)."""
        matches = [t for t in self.config["tables"] if t["name"].upper() == table_name.upper()]
        if not matches:
            available = [t["name"] for t in self.config["tables"]]
            raise ValueError(f"Table '{table_name}' not found in config. Available: {available}")
        self._dispatch(matches[0])

    def extract_all(self) -> None:
        """Extract all tables defined in config, in declaration order."""
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
