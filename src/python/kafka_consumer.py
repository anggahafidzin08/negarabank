from kafka import KafkaConsumer
from typing import Optional, Dict, Any
import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("KafkaConsumer")

class MobileEventConsumer:
    """Kafka consumer for mobile clickstream events."""

    def __init__(self, bootstrap_servers: str, topic: str):
        """
        Initialize Kafka consumer.

        Args:
            bootstrap_servers: Comma-separated broker list
            topic: Kafka topic to consume from
        """
        self.bootstrap_servers = bootstrap_servers.split(",")
        self.topic = topic
        self.consumer = None

    def connect(self):
        """Establish Kafka connection."""
        try:
            self.consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers=self.bootstrap_servers,
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id='negarabank-fraud-detection',
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                session_timeout_ms=6000,
                heartbeat_interval_ms=3000,
            )
            logger.info(f"Connected to Kafka topic: {self.topic}")
        except Exception as e:
            raise RuntimeError(f"Failed to connect to Kafka: {str(e)}")

    def consume_batch(self, timeout_ms: int = 5000, max_records: int = 1000) -> list:
        """
        Consume a batch of messages.

        Args:
            timeout_ms: Poll timeout
            max_records: Max records per poll

        Returns:
            List of message values
        """
        messages = self.consumer.poll(timeout_ms=timeout_ms, max_records=max_records)

        batch = []
        for topic_partition, records in messages.items():
            for record in records:
                batch.append(record.value)

        return batch

    def close(self):
        """Close Kafka connection."""
        if self.consumer:
            self.consumer.close()
            logger.info("Kafka consumer closed")
