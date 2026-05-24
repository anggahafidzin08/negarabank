import os
import sys
import argparse
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, count as _count

# Setup structured logging for operational visibility
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("NegaraBank-AccountSnapshot")

def get_secret_credentials():
    """
    Simulates production secret retrieval.
    In real production, use AWS Secrets Manager via boto3 or environment variables.
    """
    db_user = os.getenv("DB_USER", "etl_user")
    db_pass = os.getenv("DB_PASSWORD")
    
    if not db_pass:
        logger.error("Database password environment variable (DB_PASSWORD) is not set!")
        sys.exit(1)
        
    return db_user, db_pass

def generate_daily_predicates(target_date: str) -> list:
    """
    Generates 4 custom Oracle SQL predicates to slice a single day into 
    6-hour parallel execution windows for optimized JDBC extraction.
    """
    slices = [
        ("00:00:00", "05:59:59"),
        ("06:00:00", "11:59:59"),
        ("12:00:00", "17:59:59"),
        ("18:00:00", "23:59:59")
    ]
    
    predicates = []
    for start, end in slices:
        predicate = f"""
            txn_date >= TO_DATE('{target_date} {start}', 'YYYY-MM-DD HH24:MI:SS') 
            AND txn_date <= TO_DATE('{target_date} {end}', 'YYYY-MM-DD HH24:MI:SS')
        """
        predicates.append(predicate.strip())
    return predicates

def main():
    # 1. Parse operational arguments passed by the orchestrator
    parser = argparse.ArgumentParser(description="NegaraBank Production Account Snapshot Ingestion Pipeline")
    parser.add_argument("--execution_date", required=True, help="Target processing date in YYYY-MM-DD format")
    args = parser.parse_args()
    
    target_date = args.execution_date
    logger.info(f"Starting pipeline run for Execution Date: {target_date}")
    
    # 2. Securely load configuration and credentials
    db_user, db_pass = get_secret_credentials()
    jdbc_url = "jdbc:oracle:thin:@core-db:1521/banking"
    
    # 3. Initialize Production Spark Session with Adaptive Query Execution (AQE) enabled
    spark = SparkSession.builder \
        .appName(f"account_snapshot_inc_{target_date}") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    try:
        # 4. Extract ACCOUNTS Master Table (Relatively static data)
        logger.info("Extracting core ACCOUNTS table from Oracle...")
        accounts_df = spark.read.format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", "ACCOUNTS") \
            .option("user", db_user) \
            .option("password", db_pass) \
            .load()

        # 5. Extract TRANSACTIONS Incrementally using parallelized time-sliced predicates
        logger.info(f"Generating parallel query slices for date: {target_date}")
        txn_predicates = generate_daily_predicates(target_date)
        
        logger.info(f"Extracting TRANSACTIONS delta in 4 parallel threads via JDBC predicates...")
        connection_properties = {
            "user": db_user,
            "password": db_pass,
            "driver": "oracle.jdbc.driver.OracleDriver"
        }
        
        # This will spin up 4 parallel JDBC connection tasks across executors
        today_txns_df = spark.read.jdbc(
            url=jdbc_url, 
            table="TRANSACTIONS", 
            predicates=txn_predicates, 
            properties=connection_properties
        )

        # 6. Perform Optimized Transformations (Aggregate transactions first before joining!)
        logger.info("Aggregating daily transaction deltas...")
        txn_aggregates = today_txns_df \
            .groupBy("account_id") \
            .agg(
                _sum("amount").alias("total_txn_amount"),
                _count("txn_id").alias("txn_count")
            )

        # 7. Left Join master accounts with the pre-aggregated daily transactions
        logger.info("Joining account master data with transaction aggregates...")
        final_snapshot_df = accounts_df.join(txn_aggregates, "account_id", "left") \
            .select(
                col("account_id"),
                col("customer_id"),
                col("account_type"),
                col("balance"),
                col("total_txn_amount"),
                col("txn_count")
            )

        # 8. Data Quality Check Guardrail (Idempotence Validation)
        record_count = final_snapshot_df.count()
        logger.info(f"Transformation complete. Total records prepared for storage: {record_count}")
        
        if record_count == 0:
            logger.warning(f"No records captured for date {target_date}. Skipping storage layer write.")
            return

        # 9. Atomic, Safe ACID Write using Delta Lake (Partitioned by Account Type for downstream performance)
        output_path = "s3://negarabank-silver/account_snapshots/"
        logger.info(f"Writing validated atomic snapshot to S3 target location: {output_path}")
        
        final_snapshot_df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("replaceWhere", f"snapshot_date = '{target_date}'") \
            .save(output_path)
            
        logger.info("Pipeline executed successfully and committed cleanly.")

    except Exception as e:
        logger.critical(f"Pipeline failed critically with error: {str(e)}")
        sys.exit(1)
    finally:
        spark.stop()
        logger.info("Spark context stopped.")

if __name__ == "__main__":
    main()