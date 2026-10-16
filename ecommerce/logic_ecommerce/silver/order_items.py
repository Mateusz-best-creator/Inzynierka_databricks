from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col

from ecommerce.common_functions.config_utils import get_config, set_logger
from ecommerce.common_functions.delta_utils import create_table, optimize_vacuum_table


def prepare_order_items_data(
    order_items: DataFrame, order_item_pricing: DataFrame
) -> DataFrame:
    """"""
    order_items_joined = order_items.alias("oi").join(
        other=order_item_pricing.alias("oip"),
        on=col("oi.order_item_id") == col("oip").order_item_id,
        how="left",
    )

    order_items_selected = order_items_joined.select(
        col("oi.order_item_id").alias("ORDER_ITEM_ID"),
        col("oi.order_id").alias("ORDER_ID"),
        col("oi.product_id").alias("PRODUCT_ID"),
        col("oi.quantity").alias("QUANTITY"),
        col("oi.unit_price").alias("UNIT_PRICE"),
        col("oi.discount").alias("DISCOUNT"),
    )

    return order_items_selected.withColumn(
        "TOTAL_PRICE", col("QUANTITY") * col("UNIT_PRICE")
    ).withColumn(
        "TOTAL_PRICE_AFTER_DISCOUNT",
        col("QUANTITY") * (col("UNIT_PRICE") - col("DISCOUNT")),
    )


def entry_point_order_items(spark: SparkSession, env: str, log_level: int) -> None:
    """"""
    logger = set_logger(log_level)
    logger.info(f"Running entry_point for silver order_items table.")

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
