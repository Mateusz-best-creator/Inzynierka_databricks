from pyspark.sql import SparkSession, DataFrame

from ecommerce.common_functions.config_utils import get_config, set_logger
from ecommerce.common_functions.delta_utils import create_table, optimize_vacuum_table

from pyspark.sql.functions import col, lit, trim, upper, year, month, when, concat
from pyspark.sql.types import IntegerType


def add_date_columns(
    df: DataFrame, timestamp_column_name: str = "CREATED_AT"
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
    order_details: DataFrame, order_status_history: DataFrame, orders: DataFrame
) -> DataFrame:
    """"""
    joined_orders_data = (
        orders.alias("orders")
        .join(
            other=order_details.alias("od"),
            on=upper(trim(col("orders.order_id"))) == upper(trim(col("od.order_id"))),
            how="left",
        )
        .join(
            order_status_history.alias("osh"),
            on=upper(trim(col("orders.order_id"))) == upper(trim(col("osh.order_id"))),
            how="left",
        )
        .select(
            col("orders.order_id").alias("ORDER_ID"),
            col("orders.user_id").alias("USER_ID"),
            col("orders.session_id").alias("SESSION_ID"),
            col("orders.created_at").alias("CREATED_AT"),
            col("od.order_channel").alias("ORDER_CHANNEL"),
            col("od.shipping_method").alias("SHIPPING_METHOD"),
            col("od.shipping_cost").alias("SHIPPING_COST"),
            col("osh.status").alias("STATUS"),
            col("osh.STATUS_CHANGE_DESCRIPTION").alias("STATUS_CHANGE_DESCRIPTION"),
            col("osh.valid_from").alias("VALID_FROM"),
        )
    )

    return joined_orders_data


def entry_point_orders(spark: SparkSession, env: str, log_level: int) -> None:
    """"""
    logger = set_logger(log_level)
    logger.info(f"Running entry_point for silver order_items table.")

    config = get_config(env=env, layer="silver", config_filename="orders.json")[0]
    table_name_uc = config["table_name_uc"]
    partition_key = config["partition_key"]

    order_details = spark.read.table(
        f"databricks_{env}_domain.ecommerce_bronze.order_details"
    )

    order_status_history = spark.read.table(
        f"databricks_{env}_domain.ecommerce_bronze.order_status_history"
    ).filter(col("IS_CURRENT") == lit(1))

    orders = spark.read.table(f"databricks_{env}_domain.ecommerce_bronze.orders")

    order_silver = prepare_orders_data(
        order_details=order_details,
        order_status_history=order_status_history,
        orders=orders,
    )

    orders_with_date_columns = add_date_columns(order_silver)

    create_table(
        source_data=orders_with_date_columns,
        table_name_uc=table_name_uc,
        partition_key=partition_key,
    )

    optimize_vacuum_table(spark=spark, table_name_uc=table_name_uc)
