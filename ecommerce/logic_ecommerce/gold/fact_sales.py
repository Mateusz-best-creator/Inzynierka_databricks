from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    coalesce,
    col,
    countDistinct,
    current_timestamp,
    hour,
    lit,
    max as _max,
    minute,
    second,
    to_date,
    unix_timestamp,
    when,
)

from ecommerce.common_functions.config_utils import get_config, set_logger
from ecommerce.common_functions.delta_utils import create_table, optimize_vacuum_table


def prepare_fact_sales_data(
    silver_orders: DataFrame,
    silver_order_items: DataFrame,
    silver_payments: DataFrame,
    silver_users: DataFrame,
    dim_date: DataFrame,
    dim_time: DataFrame,
    dim_session: DataFrame,
    dim_user: DataFrame,
    dim_geography: DataFrame,
    dim_product: DataFrame,
    dim_payment_type: DataFrame,
    dim_sales_context: DataFrame,
) -> DataFrame:
    # 1. Aggregate payments to order grain
    payments_agg = silver_payments.groupBy("ORDER_ID").agg(
        _max("PAYMENT_INSTALLMENTS").alias("payment_installments_count"),
        countDistinct("PAYMENT_TYPE").alias("payment_methods_used_count"),
        _max("PAYMENT_TYPE").alias("primary_payment_type"),
    )

    # 2. Join base order data and explicitly select columns to avoid ambiguity
    sales_base = (
        silver_order_items.alias("soi")
        .join(silver_orders.alias("so"), on="ORDER_ID", how="left")
        .join(payments_agg.alias("pa"), on="ORDER_ID", how="left")
        .join(
            silver_users.alias("su"),
            col("so.USER_SURROGATE_KEY") == col("su.USER_SURROGATE_KEY"),
            "left",
        )
        .select(
            col("soi.ORDER_ID"),
            col("soi.ITEM_SEQUENCE"),
            col("soi.PRODUCT_ID"),
            col("soi.QUANTITY"),
            col("soi.UNIT_PRICE"),
            col("soi.DISCOUNT"),
            col("so.FREIGHT_VALUE"),
            col("so.SHIPPING_METHOD"),
            col("so.CREATED_AT_TIMESTAMP"),
            col("so.PAID_TIMESTAMP"),
            col("so.SHIPPED_TIMESTAMP"),
            col("so.DELIVERED_TIMESTAMP"),
            col("so.SESSION_ID"),
            col("so.USER_SURROGATE_KEY"),
            col("su.CITY"),
            col("su.STATE"),
            col("su.COUNTRY"),
            col("pa.payment_installments_count"),
            col("pa.payment_methods_used_count"),
            col("pa.primary_payment_type"),
        )
    )

    # 3. Calculate item-level measures and dates
    sales_measures = (
        sales_base.withColumn("gross_item_amount", col("QUANTITY") * col("UNIT_PRICE"))
        .withColumn(
            "net_item_amount",
            col("gross_item_amount") - coalesce(col("DISCOUNT"), lit(0.0)),
        )
        .withColumn("order_date", to_date("CREATED_AT_TIMESTAMP"))
        .withColumn("paid_date", to_date("PAID_TIMESTAMP"))
        .withColumn("shipped_date", to_date("SHIPPED_TIMESTAMP"))
        .withColumn("delivered_date", to_date("DELIVERED_TIMESTAMP"))
        .withColumn("order_hour", hour("CREATED_AT_TIMESTAMP"))
        .withColumn("order_minute", minute("CREATED_AT_TIMESTAMP"))
        .withColumn("order_second", second("CREATED_AT_TIMESTAMP"))
    )

    # 4. Join with Dimensions to get Surrogate Keys
    fact_df = (
        sales_measures.alias("sm")
        .join(
            dim_date.alias("dd_ord"),
            col("sm.order_date") == col("dd_ord.FULL_DATE"),
            "left",
        )
        .join(
            dim_date.alias("dd_pay"),
            col("sm.paid_date") == col("dd_pay.FULL_DATE"),
            "left",
        )
        .join(
            dim_date.alias("dd_shp"),
            col("sm.shipped_date") == col("dd_shp.FULL_DATE"),
            "left",
        )
        .join(
            dim_date.alias("dd_del"),
            col("sm.delivered_date") == col("dd_del.FULL_DATE"),
            "left",
        )
        .join(
            dim_time.alias("dt"),
            (col("sm.order_hour") == col("dt.HOUR"))
            & (col("sm.order_minute") == col("dt.MINUTE"))
            & (col("sm.order_second") == col("dt.SECONDS")),
            "left",
        )
        .join(
            dim_session.alias("ds"), col("sm.SESSION_ID") == col("ds.SESSION_ID"), "left"
        )
        .join(
            dim_user.alias("du"),
            col("sm.USER_SURROGATE_KEY") == col("du.USER_SURROGATE_KEY"),
            "left",
        )
        .join(
            dim_geography.alias("dg"),
            (col("sm.CITY") == col("dg.CITY"))
            & (col("sm.STATE") == col("dg.STATE"))
            & (col("sm.COUNTRY") == col("dg.COUNTRY")),
            "left",
        )
        .join(
            dim_payment_type.alias("dpt"),
            col("sm.primary_payment_type") == col("dpt.PAYMENT_TYPE_NAME"),
            "left",
        )
        .join(
            dim_sales_context.alias("dsc"),
            col("sm.SHIPPING_METHOD") == col("dsc.SHIPPING_METHOD"),
            "left",
        )
        .join(
            dim_product.alias("dp"),
            (col("sm.PRODUCT_ID") == col("dp.PRODUCT_ID"))
            & (col("sm.CREATED_AT_TIMESTAMP") >= col("dp.VALID_FROM"))
            & (
                col("sm.CREATED_AT_TIMESTAMP")
                <= coalesce(col("dp.VALID_TO"), current_timestamp())
            ),
            "left",
        )
    )

    # 5. Finalize calculations using product attributes
    unit_cost_price = coalesce(
        col("dp.COST_PRICE"), col("dp.BASE_PRICE") * lit(0.7), col("sm.UNIT_PRICE") * lit(0.7)
    )
    total_cost = unit_cost_price * col("sm.QUANTITY")

    fact_final = (
        fact_df.withColumn(
            "net_profit_margin",
            when(
                col("sm.net_item_amount") > 0,
                (col("sm.net_item_amount") - total_cost) / col("sm.net_item_amount"),
            ).otherwise(lit(0.0)),
        )
        .withColumn(
            "minutes_to_pay",
            (
                unix_timestamp("sm.PAID_TIMESTAMP")
                - unix_timestamp("sm.CREATED_AT_TIMESTAMP")
            )
            / 60,
        )
        .withColumn(
            "hours_to_process",
            (
                unix_timestamp("sm.SHIPPED_TIMESTAMP")
                - unix_timestamp("sm.PAID_TIMESTAMP")
            )
            / 3600,
        )
        .withColumn(
            "hours_to_deliver",
            (
                unix_timestamp("sm.DELIVERED_TIMESTAMP")
                - unix_timestamp("sm.SHIPPED_TIMESTAMP")
            )
            / 3600,
        )
        .withColumn(
            "total_fulfillment_days",
            (
                unix_timestamp("sm.DELIVERED_TIMESTAMP")
                - unix_timestamp("sm.CREATED_AT_TIMESTAMP")
            )
            / 86400,
        )
    )

    return fact_final.select(
        col("dd_ord.DATE_KEY").alias("ORDER_DATE_KEY"),
        col("dt.TIME_KEY").alias("ORDER_TIME_KEY"),
        col("dd_pay.DATE_KEY").alias("PAID_DATE_KEY"),
        col("dd_shp.DATE_KEY").alias("SHIPPED_DATE_KEY"),
        col("dd_del.DATE_KEY").alias("DELIVERED_DATE_KEY"),
        col("du.USER_SURROGATE_KEY"),
        col("dg.GEOGRAPHY_KEY"),
        col("dp.PRODUCT_SURROGATE_KEY"),
        col("ds.SESSION_SURROGATE_KEY"),
        col("dpt.PAYMENT_TYPE_KEY"),
        col("dsc.SALES_CONTEXT_KEY"),
        col("sm.ORDER_ID"),
        col("sm.ITEM_SEQUENCE"),
        col("sm.QUANTITY"),
        col("sm.UNIT_PRICE"),
        unit_cost_price.alias("UNIT_COST_PRICE"),
        coalesce(col("sm.DISCOUNT"), lit(0.0)).alias("DISCOUNT_AMOUNT"),
        col("sm.gross_item_amount").alias("GROSS_ITEM_AMOUNT"),
        col("sm.net_item_amount").alias("NET_ITEM_AMOUNT"),
        col("sm.FREIGHT_VALUE"),
        (col("sm.net_item_amount") + coalesce(col("sm.FREIGHT_VALUE"), lit(0.0))).alias(
            "TOTAL_LINE_AMOUNT"
        ),
        col("net_profit_margin").cast("decimal(10,2)").alias("NET_PROFIT_MARGIN"),
        lit(0).alias("TOTAL_ITEM_WEIGHT_G"),
        lit(0).alias("TOTAL_ITEM_VOLUME_CM3"),
        col("sm.payment_installments_count").alias("PAYMENT_INSTALLMENTS_COUNT"),
        col("sm.payment_methods_used_count").alias("PAYMENT_METHODS_USED_COUNT"),
        col("minutes_to_pay").cast("integer").alias("MINUTES_TO_PAY"),
        col("hours_to_process").cast("integer").alias("HOURS_TO_PROCESS"),
        col("hours_to_process").cast("integer").alias("HOURS_TO_SHIP"),
        col("hours_to_deliver").cast("integer").alias("HOURS_TO_DELIVER"),
        col("total_fulfillment_days")
        .cast("decimal(10,2)")
        .alias("TOTAL_FULFILLMENT_DAYS"),
    )


def entry_point_fact_sales(spark: SparkSession, env: str, log_level: int) -> None:
    logger = set_logger(log_level=log_level)
    logger.info("Running entry_point for gold fact_sales table.")

    config = get_config(env=env, layer="gold", config_filename="fact_sales.json")[0]
    table_name_uc = str(
        config.get("table_name_uc") or config.get("table_name") or "fact_sales"
    )
    partition_key = config.get("partition_key", [])

    silver_orders_df = spark.read.table(
        f"databricks_{env}_domain.ecommerce_silver.orders"
    )
    silver_order_items_df = spark.read.table(
        f"databricks_{env}_domain.ecommerce_silver.order_items"
    )
    silver_payments_df = spark.read.table(
        f"databricks_{env}_domain.ecommerce_silver.payments"
    )
    silver_users_df = spark.read.table(
        f"databricks_{env}_domain.ecommerce_silver.users"
    )

    dim_date_df = spark.read.table(f"databricks_{env}_domain.ecommerce_gold.dim_date")
    dim_time_df = spark.read.table(f"databricks_{env}_domain.ecommerce_gold.dim_time")
    dim_session_df = spark.read.table(
        f"databricks_{env}_domain.ecommerce_gold.dim_session"
    )
    dim_user_df = spark.read.table(f"databricks_{env}_domain.ecommerce_gold.dim_user")
    dim_geography_df = spark.read.table(
        f"databricks_{env}_domain.ecommerce_gold.dim_geography"
    )
    dim_product_df = spark.read.table(
        f"databricks_{env}_domain.ecommerce_gold.dim_product"
    )
    dim_payment_type_df = spark.read.table(
        f"databricks_{env}_domain.ecommerce_gold.dim_payment_type"
    )
    dim_sales_context_df = spark.read.table(
        f"databricks_{env}_domain.ecommerce_gold.dim_sales_context"
    )

    fact_sales_df = prepare_fact_sales_data(
        silver_orders=silver_orders_df,
        silver_order_items=silver_order_items_df,
        silver_payments=silver_payments_df,
        silver_users=silver_users_df,
        dim_date=dim_date_df,
        dim_time=dim_time_df,
        dim_session=dim_session_df,
        dim_user=dim_user_df,
        dim_geography=dim_geography_df,
        dim_product=dim_product_df,
        dim_payment_type=dim_payment_type_df,
        dim_sales_context=dim_sales_context_df,
    )

    create_table(
        source_data=fact_sales_df,
        table_name_uc=table_name_uc,
        partition_key=partition_key,
    )

    optimize_vacuum_table(spark=spark, table_name_uc=table_name_uc)