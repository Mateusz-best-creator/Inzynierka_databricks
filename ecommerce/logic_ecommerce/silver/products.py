from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    abs as spark_abs,
    coalesce,
    col,
    lead,
    lit,
    min as spark_min,
    row_number,
    trim,
    when,
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
    # Heal potential SCD2 gaps in pricing and inventory timelines
    price_win = Window.partitionBy("product_id").orderBy("valid_from")
    pricing_healed = product_pricing_history.withColumn(
        "valid_to", coalesce(lead("valid_from").over(price_win), col("valid_to"))
    )

    inv_win = Window.partitionBy("product_id").orderBy("valid_from")
    inventory_healed = product_inventory.withColumn(
        "valid_to", coalesce(lead("valid_from").over(inv_win), col("valid_to"))
    )

    has_catalog_valid_to = "valid_to" in [c.lower() for c in product_catalog.columns]
    if has_catalog_valid_to:
        cat_win = Window.partitionBy("product_id").orderBy("valid_from")
        catalog_healed = product_catalog.withColumn(
            "valid_to", coalesce(lead("valid_from").over(cat_win), col("valid_to"))
        ).withColumn(
            "product_created_at",
            spark_min("valid_from").over(Window.partitionBy("product_id")),
        )
    else:
        catalog_healed = product_catalog.withColumn(
            "product_created_at", col("valid_from")
        )

    # Step 1: Collect all unique boundary timestamps across product tables
    pricing_from = pricing_healed.select(
        col("product_id"), col("valid_from").alias("timeline_point")
    )
    pricing_to = pricing_healed.filter(col("valid_to").isNotNull()).select(
        col("product_id"), col("valid_to").alias("timeline_point")
    )
    inventory_from = inventory_healed.select(
        col("product_id"), col("valid_from").alias("timeline_point")
    )
    inventory_to = inventory_healed.filter(col("valid_to").isNotNull()).select(
        col("product_id"), col("valid_to").alias("timeline_point")
    )

    product_timelines = (
        pricing_from.union(pricing_to).union(inventory_from).union(inventory_to)
    )

    if has_catalog_valid_to:
        cat_from = catalog_healed.select(
            col("product_id"), col("valid_from").alias("timeline_point")
        )
        cat_to = catalog_healed.filter(col("valid_to").isNotNull()).select(
            col("product_id"), col("valid_to").alias("timeline_point")
        )
        product_timelines = product_timelines.union(cat_from).union(cat_to)

    product_timelines = product_timelines.filter(
        col("timeline_point").isNotNull()
    ).distinct()

    # Step 2: Create continuous, non-overlapping time intervals per product
    timeline_window = Window.partitionBy("product_id").orderBy("timeline_point")
    ordered_timelines = product_timelines.select(
        col("product_id"),
        col("timeline_point").alias("valid_from"),
        lead("timeline_point").over(timeline_window).alias("valid_to"),
    )

    # Step 3: Join catalog, active price window, and active inventory window
    surrogate_window = Window.orderBy(col("ot.product_id"), col("ot.valid_from"))

    if has_catalog_valid_to:
        catalog_join_cond = (
            (col("ot.product_id") == col("c.product_id"))
            & (col("ot.valid_from") >= col("c.valid_from"))
            & ((col("ot.valid_from") < col("c.valid_to")) | col("c.valid_to").isNull())
        )
    else:
        catalog_join_cond = col("ot.product_id") == col("c.product_id")

    clean_product_name = when(
        col("c.product_name").isNull() | (trim(col("c.product_name")) == ""),
        lit("Unknown Product"),
    ).otherwise(trim(col("c.product_name")))

    joined_products = (
        ordered_timelines.alias("ot")
        .join(
            other=catalog_healed.alias("c"),
            on=catalog_join_cond,
            how="inner",
        )
        .join(
            other=pricing_healed.alias("p"),
            on=(col("ot.product_id") == col("p.product_id"))
            & (col("ot.valid_from") >= col("p.valid_from"))
            & ((col("ot.valid_from") < col("p.valid_to")) | col("p.valid_to").isNull()),
            how="left",
        )
        .join(
            other=inventory_healed.alias("i"),
            on=(col("ot.product_id") == col("i.product_id"))
            & (col("ot.valid_from") >= col("i.valid_from"))
            & ((col("ot.valid_from") < col("i.valid_to")) | col("i.valid_to").isNull()),
            how="left",
        )
        .select(
            row_number().over(surrogate_window).alias("PRODUCT_SURROGATE_KEY"),
            col("c.product_id").alias("PRODUCT_ID"),
            clean_product_name.alias("PRODUCT_NAME"),
            col("c.brand").alias("BRAND"),
            col("c.description").alias("DESCRIPTION"),
            spark_abs(coalesce(col("p.price").cast("double"), lit(0.0))).alias(
                "BASE_PRICE"
            ),
            spark_abs(coalesce(col("p.cost_price").cast("double"), lit(0.0))).alias(
                "COST_PRICE"
            ),
            spark_abs(col("i.weight_g").cast("double")).alias("WEIGHT_G"),
            spark_abs(col("i.length_cm").cast("double")).alias("LENGTH_CM"),
            spark_abs(col("i.height_cm").cast("double")).alias("HEIGHT_CM"),
            spark_abs(col("i.width_cm").cast("double")).alias("WIDTH_CM"),
            spark_abs(
                coalesce(col("i.stock_quantity").cast(IntegerType()), lit(0))
            ).alias("STOCK_QUANTITY"),
            col("c.product_created_at").alias("CREATED_AT"),
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
