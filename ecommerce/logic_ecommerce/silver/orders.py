from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    abs as spark_abs,
    coalesce,
    col,
    concat,
    lit,
    max as spark_max,
    monotonically_increasing_id,
    month,
    row_number,
    sum,
    trim,
    upper,
    when,
    year,
)
from pyspark.sql.types import DecimalType, IntegerType
from pyspark.sql.window import Window

from ecommerce.common_functions.config_utils import get_config, set_logger
from ecommerce.common_functions.delta_utils import create_table, optimize_vacuum_table


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
    # Sanitize negative prices, discounts, and quantities when calculating order totals
    item_totals = (
        order_items.alias("oi")
        .filter(col("oi.ORDER_ID").isNotNull())
        .join(
            order_item_pricing.alias("ip"),
            upper(trim(col("oi.ORDER_ITEM_ID")))
            == upper(trim(col("ip.ORDER_ITEM_ID"))),
            how="inner",
        )
        .groupBy(col("oi.ORDER_ID"))
        .agg(
            sum(
                spark_abs(col("oi.QUANTITY")).cast(DecimalType(10, 2))
                * (
                    spark_abs(col("ip.UNIT_PRICE"))
                    - coalesce(spark_abs(col("ip.DISCOUNT")), lit(0.00))
                )
            ).alias("ITEMS_TOTAL")
        )
    )

    # Heal inverted status timestamps by aligning lifecycle stage rank with chronological order
    status_dedup = (
        order_status_history.filter(
            col("ORDER_ID").isNotNull()
            & col("CURRENT_STATUS").isNotNull()
            & col("VALID_FROM").isNotNull()
        )
        .groupBy(col("ORDER_ID"), col("CURRENT_STATUS"))
        .agg(spark_max(col("VALID_FROM")).alias("VALID_FROM"))
    )

    status_ranked = status_dedup.withColumn(
        "status_rank",
        when(col("CURRENT_STATUS") == "CREATED", lit(0))
        .when(col("CURRENT_STATUS") == "PENDING_PAYMENT", lit(1))
        .when(col("CURRENT_STATUS") == "PAID", lit(2))
        .when(col("CURRENT_STATUS") == "PROCESSING", lit(3))
        .when(col("CURRENT_STATUS") == "SHIPPED", lit(4))
        .when(col("CURRENT_STATUS") == "DELIVERED", lit(5))
        .otherwise(lit(99)),
    )

    rank_win = Window.partitionBy("ORDER_ID").orderBy("status_rank")
    time_win = Window.partitionBy("ORDER_ID").orderBy("VALID_FROM")

    status_by_rank = status_ranked.select(
        col("ORDER_ID"),
        col("CURRENT_STATUS"),
        row_number().over(rank_win).alias("rn"),
    )
    status_by_time = status_ranked.select(
        col("ORDER_ID"),
        col("VALID_FROM"),
        row_number().over(time_win).alias("rn"),
    )

    healed_status = status_by_rank.join(
        status_by_time, on=["ORDER_ID", "rn"], how="inner"
    )

    pivoted_status = (
        healed_status.alias("osh")
        .groupBy(col("ORDER_ID"))
        .agg(
            spark_max(
                when(col("CURRENT_STATUS") == "PENDING_PAYMENT", col("VALID_FROM"))
            ).alias("PENDING_PAYMENT_TIMESTAMP"),
            spark_max(when(col("CURRENT_STATUS") == "PAID", col("VALID_FROM"))).alias(
                "PAID_TIMESTAMP"
            ),
            spark_max(
                when(col("CURRENT_STATUS") == "PROCESSING", col("VALID_FROM"))
            ).alias("PROCESSING_TIMESTAMP"),
            spark_max(
                when(col("CURRENT_STATUS") == "SHIPPED", col("VALID_FROM"))
            ).alias("SHIPPED_TIMESTAMP"),
            spark_max(
                when(col("CURRENT_STATUS") == "DELIVERED", col("VALID_FROM"))
            ).alias("DELIVERED_TIMESTAMP"),
        )
    )

    # Join orders with status history first to repair corrupted CREATED_AT dates
    orders_with_status = (
        orders.filter(col("ORDER_ID").isNotNull())
        .alias("o")
        .join(
            pivoted_status.alias("osh"),
            upper(trim(col("o.ORDER_ID"))) == upper(trim(col("osh.ORDER_ID"))),
            how="left",
        )
        .drop(col("osh.ORDER_ID"))  # Drop to prevent ambiguity downstream
        .withColumn(
            "effective_created_at",
            when(
                col("CREATED_AT").isNull()
                | (year(col("CREATED_AT")) < 2000)
                | (year(col("CREATED_AT")) > 2030),
                coalesce(
                    col("PENDING_PAYMENT_TIMESTAMP"),
                    col("PAID_TIMESTAMP"),
                    col("CREATED_AT"),
                ),
            ).otherwise(col("CREATED_AT")),
        )
    )

    joined_orders = (
        orders_with_status.alias("os")
        .join(
            order_details.alias("od"),
            upper(trim(col("os.ORDER_ID"))) == upper(trim(col("od.ORDER_ID"))),
            how="left",
        )
        .join(
            item_totals.alias("it"),
            upper(trim(col("os.ORDER_ID"))) == upper(trim(col("it.ORDER_ID"))),
            how="left",
        )
        .join(
            silver_users.alias("u"),
            (upper(trim(col("os.USER_ID"))) == upper(trim(col("u.USER_ID"))))
            & (col("os.effective_created_at") >= col("u.VALID_FROM"))
            & (
                (col("os.effective_created_at") < col("u.VALID_TO"))
                | col("u.VALID_TO").isNull()
            ),
            how="left",
        )
    )

    clean_shipping_cost = coalesce(spark_abs(col("od.SHIPPING_COST")), lit(0.00))

    orders_selected = joined_orders.select(
        monotonically_increasing_id().alias("ORDER_SURROGATE_KEY"),
        col("os.ORDER_ID").alias("ORDER_ID"),
        (coalesce(col("it.ITEMS_TOTAL"), lit(0.00)) + clean_shipping_cost).alias(
            "TOTAL_AMOUNT"
        ),
        clean_shipping_cost.alias("FREIGHT_VALUE"),
        col("od.SHIPPING_METHOD").alias("SHIPPING_METHOD"),
        col("os.effective_created_at").alias("CREATED_AT_TIMESTAMP"),
        col("os.PENDING_PAYMENT_TIMESTAMP"),
        col("os.PAID_TIMESTAMP"),
        col("os.PROCESSING_TIMESTAMP"),
        col("os.SHIPPED_TIMESTAMP"),
        col("os.DELIVERED_TIMESTAMP"),
        col("os.SESSION_ID").alias("SESSION_ID"),
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
    silver_users = spark.read.table(f"databricks_{env}_domain.ecommerce_silver.users")

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
