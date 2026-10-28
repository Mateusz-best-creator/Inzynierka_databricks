from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    abs as spark_abs,
    coalesce,
    col,
    lead,
    lit,
    row_number,
)
from pyspark.sql.types import IntegerType
from pyspark.sql.window import Window

from ecommerce.common_functions.config_utils import get_config, set_logger
from ecommerce.common_functions.delta_utils import create_table, optimize_vacuum_table


def prepare_products_data(
    product_catalog: DataFrame,
    product_pricing_history: DataFrame,
    product_inventory: DataFrame,
) -> DataFrame:
    # Step 1: Collect all unique boundary timestamps from pricing and inventory
    pricing_from = product_pricing_history.select(
        col("product_id"), col("valid_from").alias("timeline_point")
    )
    pricing_to = product_pricing_history.filter(col("valid_to").isNotNull()).select(
        col("product_id"), col("valid_to").alias("timeline_point")
    )
    inventory_from = product_inventory.select(
        col("product_id"), col("valid_from").alias("timeline_point")
    )
    inventory_to = product_inventory.filter(col("valid_to").isNotNull()).select(
        col("product_id"), col("valid_to").alias("timeline_point")
    )

    product_timelines = (
        pricing_from.union(pricing_to)
        .union(inventory_from)
        .union(inventory_to)
        .filter(col("timeline_point").isNotNull())
        .distinct()
    )

    # Step 2: Create continuous, non-overlapping time intervals per product
    timeline_window = Window.partitionBy("product_id").orderBy("timeline_point")
    ordered_timelines = product_timelines.select(
        col("product_id"),
        col("timeline_point").alias("valid_from"),
        lead("timeline_point").over(timeline_window).alias("valid_to"),
    )

    # Step 3: Join catalog, active price window, and active inventory window
    surrogate_window = Window.orderBy(col("ot.product_id"), col("ot.valid_from"))

    joined_products = (
        ordered_timelines.alias("ot")
        .join(
            other=product_catalog.alias("c"),
            on=col("ot.product_id") == col("c.product_id"),
            how="inner",
        )
        .join(
            other=product_pricing_history.alias("p"),
            on=(col("ot.product_id") == col("p.product_id"))
            & (col("ot.valid_from") >= col("p.valid_from"))
            & ((col("ot.valid_from") < col("p.valid_to")) | col("p.valid_to").isNull()),
            how="left",
        )
        .join(
            other=product_inventory.alias("i"),
            on=(col("ot.product_id") == col("i.product_id"))
            & (col("ot.valid_from") >= col("i.valid_from"))
            & ((col("ot.valid_from") < col("i.valid_to")) | col("i.valid_to").isNull()),
            how="left",
        )
        .select(
            row_number().over(surrogate_window).alias("PRODUCT_SURROGATE_KEY"),
            col("c.product_id").alias("PRODUCT_ID"),
            col("c.product_name").alias("PRODUCT_NAME"),
            col("c.brand").alias("BRAND"),
            col("c.description").alias("DESCRIPTION"),
            spark_abs(coalesce(col("p.price").cast("double"), lit(0.0))).alias(
                "BASE_PRICE"
            ),
            spark_abs(coalesce(col("p.cost_price").cast("double"), lit(0.0))).alias(
                "COST_PRICE"
            ),
            col("i.weight_g").cast("double").alias("WEIGHT_G"),
            col("i.length_cm").cast("double").alias("LENGTH_CM"),
            col("i.height_cm").cast("double").alias("HEIGHT_CM"),
            col("i.width_cm").cast("double").alias("WIDTH_CM"),
            coalesce(col("i.stock_quantity").cast(IntegerType()), lit(0)).alias(
                "STOCK_QUANTITY"
            ),
            col("c.valid_from").alias("CREATED_AT"),
            col("ot.valid_from").alias("VALID_FROM"),
            col("ot.valid_to").alias("VALID_TO"),
            col("c.is_active").cast(IntegerType()).alias("IS_ACTIVE"),
            col("c.category_id").alias("CATEGORY_ID"),
        )
    )

    return joined_products


def entry_point_products(spark: SparkSession, env: str, log_level: int) -> None:
    logger = set_logger(log_level=log_level)
    logger.info("Running entry_point for silver products table.")

    config = get_config(env=env, layer="silver", config_filename="products.json")[0]
    table_name_uc = config["table_name_uc"]
    partition_key = config["partition_key"]

    product_catalog_df = spark.read.table(
        f"databricks_{env}_domain.ecommerce_bronze.product_catalog"
    )
    product_pricing_df = spark.read.table(
        f"databricks_{env}_domain.ecommerce_bronze.product_pricing_history"
    )
    product_inventory_df = spark.read.table(
        f"databricks_{env}_domain.ecommerce_bronze.product_inventory"
    )

    products_silver_df = prepare_products_data(
        product_catalog=product_catalog_df,
        product_pricing_history=product_pricing_df,
        product_inventory=product_inventory_df,
    )

    create_table(
        source_data=products_silver_df,
        table_name_uc=table_name_uc,
        partition_key=partition_key,
    )

    optimize_vacuum_table(spark=spark, table_name_uc=table_name_uc)
