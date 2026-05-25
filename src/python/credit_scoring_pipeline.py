"""
Credit scoring pipeline — placeholder.
Reads account_daily_snapshot as feature input and produces credit score predictions.

TODO: implement model loading from MLflow registry and batch inference logic.
"""
import logging
from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("CreditScoringPipeline")


def run(spark: SparkSession) -> None:
    snapshot = spark.table("negarabank.gold.account_daily_snapshot")
    logger.info(f"Loaded account_daily_snapshot: {snapshot.count()} rows")

    # TODO: load model from MLflow registry
    # model = mlflow.pyfunc.spark_udf(spark, model_uri="models:/credit_score_model/production")

    # TODO: run batch inference
    # predictions = snapshot.withColumn("predicted_score", model(*feature_cols))

    # TODO: write predictions to negarabank.gold.credit_score_predictions or Feature Store

    logger.info("Credit scoring pipeline placeholder — no predictions written")


if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    run(spark)
