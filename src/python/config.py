import os
import json
import boto3
from typing import Dict, Any

def get_oracle_credentials() -> Dict[str, str]:
    """Retrieve Oracle credentials from AWS Secrets Manager."""
    client = boto3.client("secretsmanager", region_name=os.getenv("AWS_REGION", "ap-southeast-1"))

    try:
        response = client.get_secret_value(SecretId="negarabank/oracle/jdbc")
        secret = json.loads(response["SecretString"])
        return {
            "user": secret["username"],
            "password": secret["password"],
            "host": secret["host"],
            "port": secret["port"],
        }
    except Exception as e:
        raise RuntimeError(f"Failed to retrieve Oracle credentials: {str(e)}")

def get_s3_paths() -> Dict[str, str]:
    """Get S3 bucket paths for each medallion layer."""
    return {
        "bronze": f"s3://{os.getenv('S3_BUCKET_BRONZE', 'negarabank-bronze')}/",
        "silver": f"s3://{os.getenv('S3_BUCKET_SILVER', 'negarabank-silver')}/",
        "gold": f"s3://{os.getenv('S3_BUCKET_GOLD', 'negarabank-gold')}/",
        "checkpoints": f"s3://{os.getenv('S3_BUCKET_CHECKPOINTS', 'negarabank-checkpoints')}/",
    }

def get_kafka_config() -> Dict[str, str]:
    """Get Kafka configuration."""
    return {
        "bootstrap_servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        "mobile_events_topic": os.getenv("KAFKA_TOPIC_MOBILE_EVENTS", "mobile_clickstream"),
    }

class Paths:
    """Centralized path management."""

    S3 = get_s3_paths()

    @staticmethod
    def bronze_table(table_name: str) -> str:
        return f"{Paths.S3['bronze']}{table_name}/"

    @staticmethod
    def silver_table(table_name: str) -> str:
        return f"{Paths.S3['silver']}{table_name}/"

    @staticmethod
    def gold_table(table_name: str) -> str:
        return f"{Paths.S3['gold']}{table_name}/"

    @staticmethod
    def checkpoint(job_name: str) -> str:
        return f"{Paths.S3['checkpoints']}{job_name}/"
