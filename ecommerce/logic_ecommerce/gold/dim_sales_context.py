from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import coalesce, col, lit, row_number, when
from pyspark.sql.window import Window

from ecommerce.common_functions.config_utils import get_config, set_logger
from ecommerce.common_functions.delta_utils import create_table, optimize_vacuum_table


def prepare_dim_sales_context_data(silver_orders: DataFrame) -> DataFrame:
    # Check if optional columns exist to prevent crashes during dataframe evaluation
    has_discount = "DISCOUNT_AMOUNT" in silver_orders.columns
    has_actual_delivery = "ACTUAL_DELIVERY_DATE" in silver_orders.columns
    has_estimated_delivery = "ESTIMATED_DELIVERY_DATE" in silver_orders.columns

    # Define discount status logic
    if has_discount:
        discount_logic = when(
            coalesce(col("DISCOUNT_AMOUNT"), lit(0)) > 0, lit("Discounted")
        ).otherwise(lit("Full Price"))
    else:
        discount_logic = lit("Full Price")

    # Define delivery SLA status logic
    if has_actual_delivery and has_estimated_delivery:
        sla_logic = (
            when(
                col("ACTUAL_DELIVERY_DATE").isNull()
                | col("ESTIMATED_DELIVERY_DATE").isNull(),
                lit("On-Time Delivery"),
            )
            .when(
                col("ACTUAL_DELIVERY_DATE") <= col("ESTIMATED_DELIVERY_DATE"),
                lit("On-Time Delivery"),
            )
            .otherwise(lit("Delayed Delivery"))
        )
    else:
        sla_logic = lit("On-Time Delivery")

    # Extract unique context combinations
    context_df = (
        silver_orders.withColumn(
            "shipping_method_clean", coalesce(col("SHIPPING_METHOD"), lit("Standard"))
        )
        .withColumn("discount_status", discount_logic)
        .withColumn("delivery_sla_status", sla_logic)
        .select(
            col("shipping_method_clean").alias("shipping_method"),
            col("discount_status"),
            col("delivery_sla_status"),
        )
        .dropDuplicates(["shipping_method", "discount_status", "delivery_sla_status"])
    )

    surrogate_window = Window.orderBy(
        "shipping_method", "discount_status", "delivery_sla_status"
    )

    return context_df.select(
        row_number().over(surrogate_window).alias("SALES_CONTEXT_KEY"),
        col("shipping_method").alias("SHIPPING_METHOD"),
        col("discount_status").alias("DISCOUNT_STATUS"),
        col("delivery_sla_status").alias("DELIVERY_SLA_STATUS"),
    )


def entry_point_dim_sales_context(
    spark: SparkSession, env: str, log_level: int
) -> None:
    logger = set_logger(log_level=log_level)
    logger.info("Running entry_point for gold dim_sales_context table.")

    config = get_config(
        env=env, layer="gold", config_filename="dim_sales_context.json"
    )[0]
    table_name_uc = str(
        config.get("table_name_uc") or config.get("table_name") or "dim_sales_context"
    )
    partition_key = config.get("partition_key", [])

    silver_orders_df = spark.read.table(
        f"databricks_{env}_domain.ecommerce_silver.orders"
    )

    dim_sales_context_df = prepare_dim_sales_context_data(
        silver_orders=silver_orders_df
    )

    create_table(
        source_data=dim_sales_context_df,
        table_name_uc=table_name_uc,
        partition_key=partition_key,
    )

    optimize_vacuum_table(spark=spark, table_name_uc=table_name_uc)
