from pyspark.sql import SparkSession, DataFrame

from ecommerce.common_functions.config_utils import get_config, set_logger
from ecommerce.common_functions.delta_utils import create_table, optimize_vacuum_table

from pyspark.sql.functions import col, monotonically_increasing_id


def prepare_clickstream_data(event_clickstream: DataFrame) -> DataFrame:
    """Prepare and transform clickstream events data."""

    clickstream_selected = event_clickstream.select(
        monotonically_increasing_id().alias("EVENT_SURROGATE_KEY"),
        col("EVENT_ID").alias("EVENT_ID"),
        col("EVENT_TYPE").alias("EVENT_TYPE"),
        col("EVENT_TIME").alias("EVENT_TIME"),
        col("SESSION_ID").alias("SESSION_ID"),
        col("PRODUCT_ID").alias("PRODUCT_ID"),
    )

    return clickstream_selected


def entry_point_event_clickstream(
    spark: SparkSession, env: str, log_level: int
) -> None:
    """Entry point for silver events_clickstream table ETL."""
    logger = set_logger(log_level)
    logger.info("Running entry_point for silver events_clickstream table.")

    config = get_config(
        env=env, layer="silver", config_filename="event_clickstream.json"
    )[0]
    table_name_uc = config["table_name"]
    partition_key = config["partition_key"]

    event_clickstream = spark.read.table(
        f"databricks_{env}_domain.ecommerce_bronze.event_clickstream"
    )

    clickstream_silver = prepare_clickstream_data(event_clickstream=event_clickstream)

    create_table(
        source_data=clickstream_silver,
        table_name_uc=table_name_uc,
        partition_key=partition_key,
    )

    optimize_vacuum_table(spark=spark, table_name_uc=table_name_uc)
