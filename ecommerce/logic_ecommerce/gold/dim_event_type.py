from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit, row_number, when
from pyspark.sql.window import Window

from ecommerce.common_functions.config_utils import get_config, set_logger
from ecommerce.common_functions.delta_utils import create_table, optimize_vacuum_table


def prepare_dim_event_type_data(silver_events: DataFrame) -> DataFrame:
    # Filter out empty event types and keep distinct values
    unique_events = (
        silver_events.filter(col("EVENT_TYPE").isNotNull())
        .select(col("EVENT_TYPE").alias("event_type_name"))
        .dropDuplicates(["event_type_name"])
    )

    # Map raw event types to funnel steps and funnel stages
    mapped_events = unique_events.withColumn(
        "funnel_step_order",
        when(col("event_type_name").isin("View", "PageView", "ProductView"), lit(1))
        .when(col("event_type_name").isin("AddToCart", "Cart", "Wishlist"), lit(2))
        .when(col("event_type_name").isin("Checkout", "Payment"), lit(3))
        .when(col("event_type_name").isin("Purchase", "Complete"), lit(4))
        .otherwise(lit(5)),
    ).withColumn(
        "funnel_stage_name",
        when(
            col("event_type_name").isin("View", "PageView", "ProductView"),
            lit("1. Awareness (View)"),
        )
        .when(
            col("event_type_name").isin("AddToCart", "Cart", "Wishlist"),
            lit("2. Consideration (Cart)"),
        )
        .when(
            col("event_type_name").isin("Checkout", "Payment", "Purchase", "Complete"),
            lit("3. Conversion (Purchase)"),
        )
        .otherwise(lit("4. Other / Unclassified")),
    )

    surrogate_window = Window.orderBy("funnel_step_order", "event_type_name")

    return mapped_events.select(
        row_number().over(surrogate_window).alias("EVENT_TYPE_KEY"),
        col("event_type_name").alias("EVENT_TYPE_NAME"),
        col("funnel_step_order").alias("FUNNEL_STEP_ORDER"),
        col("funnel_stage_name").alias("FUNNEL_STAGE_NAME"),
    )


def entry_point_dim_event_type(spark: SparkSession, env: str, log_level: int) -> None:
    logger = set_logger(log_level=log_level)
    logger.info("Running entry_point for gold dim_event_type table.")

    config = get_config(env=env, layer="gold", config_filename="dim_event_type.json")[0]
    table_name_uc = str(
        config.get("table_name_uc") or config.get("table_name") or "dim_event_type"
    )
    partition_key = config.get("partition_key", [])

    silver_events_df = spark.read.table(
        f"databricks_{env}_domain.ecommerce_silver.event_clickstream"
    )

    dim_event_type_df = prepare_dim_event_type_data(silver_events=silver_events_df)

    create_table(
        source_data=dim_event_type_df,
        table_name_uc=table_name_uc,
        partition_key=partition_key,
    )

    optimize_vacuum_table(spark=spark, table_name_uc=table_name_uc)
