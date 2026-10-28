from pyspark.sql import SparkSession, DataFrame

from ecommerce.common_functions.config_utils import get_config, set_logger
from ecommerce.common_functions.delta_utils import create_table, optimize_vacuum_table

from pyspark.sql.functions import (
    col,
    lit,
    trim,
    upper,
    year,
    month,
    when,
    concat,
    coalesce,
    sum,
    max as spark_max,
    monotonically_increasing_id,
)
from pyspark.sql.types import IntegerType, DecimalType


def add_date_columns(
    df: DataFrame, timestamp_column_name: str = "CREATED_AT_TIMESTAMP"
) -> DataFrame:
    timestamp_column = month(col(timestamp_column_name))

    df_year_month = df.withColumn(
        "YEAR", year(col(timestamp_column_name)).cast(IntegerType())
    ).withColumn(
        "MONTH",
        when(timestamp_column < 10, concat(lit("0"), timestamp_column)).otherwise(
            timestamp_column.cast("string")
        ),
    )

    quarter = (
        when(col("MONTH").isin(["01", "02", "03"]), "Q1")
        .when(col("MONTH").isin(["04", "05", "06"]), "Q2")
        .when(col("MONTH").isin(["07", "08", "09"]), "Q3")
        .when(col("MONTH").isin(["10", "11", "12"]), "Q4")
        .otherwise("INCORRECT_DATE")
    )

    return df_year_month.withColumn("QUARTER", quarter)


def prepare_orders_data(
    orders: DataFrame,
    order_details: DataFrame,
    order_status_history: DataFrame,
    order_items: DataFrame,
    order_item_pricing: DataFrame,
    silver_users: DataFrame,
) -> DataFrame:
    """Prepare and pivot orders data matching the silver relational logic."""
    item_totals = (
        order_items.alias("oi")
        .join(
            order_item_pricing.alias("ip"),
            upper(trim(col("oi.ORDER_ITEM_ID")))
            == upper(trim(col("ip.ORDER_ITEM_ID"))),
            how="inner",
        )
        .groupBy(col("oi.ORDER_ID"))
        .agg(
            sum(
                col("oi.QUANTITY").cast(DecimalType(10, 2))
                * (col("ip.UNIT_PRICE") - coalesce(col("ip.DISCOUNT"), lit(0.00)))
            ).alias("ITEMS_TOTAL")
        )
    )

    pivoted_status = (
        order_status_history.alias("osh")
        .groupBy(col("ORDER_ID"))
        .agg(
            spark_max(
                when(col("CURRENT_STATUS") == "PENDING_PAYMENT", col("VALID_FROM"))
            ).alias("PENDING_PAYMENT_TIMESTAMP"),
            spark_max(when(col("CURRENT_STATUS") == "PAID", col("VALID_FROM"))).alias(
                "PAID_TIMESTAMP"
            ),
            spark_max(when(col("CURRENT_STATUS") == "PROCESSING", col("VALID_FROM"))).alias(
                "PROCESSING_TIMESTAMP"
            ),
            spark_max(when(col("CURRENT_STATUS") == "SHIPPED", col("VALID_FROM"))).alias(
                "SHIPPED_TIMESTAMP"
            ),
            spark_max(when(col("CURRENT_STATUS") == "DELIVERED", col("VALID_FROM"))).alias(
                "DELIVERED_TIMESTAMP"
            ),
        )
    )

    joined_orders = (
        orders.alias("o")
        .join(
            order_details.alias("od"),
            upper(trim(col("o.ORDER_ID"))) == upper(trim(col("od.ORDER_ID"))),
            how="left",
        )
        .join(
            pivoted_status.alias("osh"),
            upper(trim(col("o.ORDER_ID"))) == upper(trim(col("osh.ORDER_ID"))),
            how="left",
        )
        .join(
            item_totals.alias("it"),
            upper(trim(col("o.ORDER_ID"))) == upper(trim(col("it.ORDER_ID"))),
            how="left",
        )
        .join(
            silver_users.alias("u"),
            (upper(trim(col("o.USER_ID"))) == upper(trim(col("u.USER_ID"))))
            & (col("o.CREATED_AT") >= col("u.VALID_FROM"))
            & ((col("o.CREATED_AT") < col("u.VALID_TO")) | col("u.VALID_TO").isNull()),
            how="left",
        )
    )

    orders_selected = joined_orders.select(
        monotonically_increasing_id().alias("ORDER_SURROGATE_KEY"),
        col("o.ORDER_ID").alias("ORDER_ID"),
        (
            coalesce(col("it.ITEMS_TOTAL"), lit(0.00))
            + coalesce(col("od.SHIPPING_COST"), lit(0.00))
        ).alias("TOTAL_AMOUNT"),
        coalesce(col("od.SHIPPING_COST"), lit(0.00)).alias("FREIGHT_VALUE"),
        col("od.SHIPPING_METHOD").alias("SHIPPING_METHOD"),
        col("o.CREATED_AT").alias("CREATED_AT_TIMESTAMP"),
        col("osh.PENDING_PAYMENT_TIMESTAMP"),
        col("osh.PAID_TIMESTAMP"),
        col("osh.PROCESSING_TIMESTAMP"),
        col("osh.SHIPPED_TIMESTAMP"),
        col("osh.DELIVERED_TIMESTAMP"),
        col("o.SESSION_ID").alias("SESSION_ID"),
        col("u.USER_SURROGATE_KEY").alias("USER_SURROGATE_KEY"),
    )

    return orders_selected


def entry_point_orders(spark: SparkSession, env: str, log_level: int) -> None:
    """Entry point for silver orders table ETL."""
    logger = set_logger(log_level)
    logger.info("Running entry_point for silver orders table.")

    config = get_config(env=env, layer="silver", config_filename="orders.json")[0]
    table_name_uc = config["table_name_uc"]
    partition_key = config["partition_key"]

    orders = spark.read.table(f"databricks_{env}_domain.ecommerce_bronze.orders")
    order_details = spark.read.table(
        f"databricks_{env}_domain.ecommerce_bronze.order_details"
    )
    order_status_history = spark.read.table(
        f"databricks_{env}_domain.ecommerce_bronze.order_status_history"
    )
    order_items = spark.read.table(
        f"databricks_{env}_domain.ecommerce_bronze.order_items"
    )
    order_item_pricing = spark.read.table(
        f"databricks_{env}_domain.ecommerce_bronze.order_item_pricing"
    )
    silver_users = spark.read.table(
        f"databricks_{env}_domain.ecommerce_silver.users"
    )

    order_silver = prepare_orders_data(
        orders=orders,
        order_details=order_details,
        order_status_history=order_status_history,
        order_items=order_items,
        order_item_pricing=order_item_pricing,
        silver_users=silver_users,
    )

    orders_with_date_columns = add_date_columns(order_silver)

    create_table(
        source_data=orders_with_date_columns,
        table_name_uc=table_name_uc,
        partition_key=partition_key,
    )

    optimize_vacuum_table(spark=spark, table_name_uc=table_name_uc)