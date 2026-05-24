# Databricks notebook source
# Gold Layer: Dimension - EVENT_TYPE

from pyspark.sql.functions import col, lit
from src.python.config import Paths
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("GoldDimEventType")

# COMMAND ----------

# Define event types (could come from mobile app schema)
event_types = [
    {"event_type_id": 1, "event_type": "page_view", "category": "engagement", "is_sensitive": 0},
    {"event_type_id": 2, "event_type": "button_click", "category": "engagement", "is_sensitive": 0},
    {"event_type_id": 3, "event_type": "form_submit", "category": "transaction", "is_sensitive": 1},
    {"event_type_id": 4, "event_type": "login", "category": "security", "is_sensitive": 1},
    {"event_type_id": 5, "event_type": "logout", "category": "security", "is_sensitive": 0},
    {"event_type_id": 6, "event_type": "payment", "category": "transaction", "is_sensitive": 1},
    {"event_type_id": 7, "event_type": "transfer", "category": "transaction", "is_sensitive": 1},
]

dim_event_type = spark.createDataFrame(event_types)

# COMMAND ----------

# Write to Gold
output_path = Paths.gold_table("dim_event_type")
dim_event_type.write \
    .format("delta") \
    .mode("overwrite") \
    .save(output_path)

logger.info(f"✓ Dimension EVENT_TYPE written: {dim_event_type.count()} records")
print(f"✓ DIM_EVENT_TYPE built")

# COMMAND ----------

dim_event_type.show()
