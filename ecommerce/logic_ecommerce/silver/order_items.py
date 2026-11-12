from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    abs as spark_abs,
    col,
    monotonically_increasing_id,
)

from ecommerce.common_functions.config_utils import get_config, set_logger
from ecommerce.common_functions.delta_utils import create_table, optimize_vacuum_table


def prepare_order_items_data(
    order_items: DataFrame, order_item_pricing: DataFrame
) -> DataFrame:
    """Prepare and transform order items data by joining with pricing information."""
    valid_items = order_items.filter(
        col("ORDER_ITEM_ID").isNotNull()
        & col("ORDER_ID").isNotNull()
        & col("PRODUCT_ID").isNotNull()
    )

    order_items_joined = valid_items.alias("oi").join(
        other=order_item_pricing.alias("oip"),
        on=col("oi.ORDER_ITEM_ID") == col("oip.ORDER_ITEM_ID"),
        how="left",
    )

    order_items_selected = order_items_joined.select(
        monotonically_increasing_id().alias("ORDER_ITEM_SURROGATE_KEY"),
        col("oi.ORDER_ITEM_ID").alias("ORDER_ITEM_ID"),
        spark_abs(col("oi.ORDER_ITEM_SEQUENTIAL").cast("integer")).alias(
            "ITEM_SEQUENCE"
        ),
        spark_abs(col("oi.QUANTITY")).alias("QUANTITY"),
        spark_abs(col("oip.UNIT_PRICE")).alias("UNIT_PRICE"),
        spark_abs(col("oip.DISCOUNT")).alias("DISCOUNT"),
        col("oi.ORDER_ID").alias("ORDER_ID"),
        col("oi.PRODUCT_ID").alias("PRODUCT_ID"),
    )

    return order_items_selected


def entry_point_order_items(spark: SparkSession, env: str, log_level: int) -> None:
    """Entry point for silver order_items table ETL."""
    logger = set_logger(log_level)
    logger.info("Running entry_point for silver order_items table.")

    config = get_config(env=env, layer="silver", config_filename="order_items.json")[0]
    table_name_uc = config["table_name_uc"]
    partition_key = config["partition_key"]

    order_items = spark.read.table(
        f"databricks_{env}_domain.ecommerce_bronze.order_items"
    )

    order_item_pricing = spark.read.table(
        f"databricks_{env}_domain.ecommerce_bronze.order_item_pricing"
    )

    order_items_silver = prepare_order_items_data(
        order_items=order_items, order_item_pricing=order_item_pricing
    )

    create_table(
        source_data=order_items_silver,
        table_name_uc=table_name_uc,
        partition_key=partition_key,
    )

    optimize_vacuum_table(spark=spark, table_name_uc=table_name_uc)
