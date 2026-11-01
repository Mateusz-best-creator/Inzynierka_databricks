from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    coalesce,
    col,
    countDistinct,
    greatest,
    hour,
    lit,
    max as _max,
    minute,
    second,
    to_date,
    trim,
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
    # 1. Aggregate payments to order grain and compute installment tier
    payments_agg = (
        silver_payments.groupBy("ORDER_ID")
        .agg(
            _max("PAYMENT_INSTALLMENTS").alias("payment_installments_count"),
            countDistinct("PAYMENT_TYPE").alias("payment_methods_used_count"),
            _max("PAYMENT_TYPE").alias("primary_payment_type"),
        )
        .withColumn(
            "installment_tier",
            when(
                coalesce(col("payment_installments_count"), lit(1)) <= 1,
                lit("Single Payment"),
            )
            .when(
                col("payment_installments_count").between(2, 3),
                lit("2-3 Installments"),
            )
            .otherwise(lit("4+ Installments")),
        )
    )

    has_processing_ts = "PROCESSING_TIMESTAMP" in silver_orders.columns
    has_user_zip = "ZIP_CODE" in silver_users.columns
    has_geo_zip = "ZIP_CODE" in dim_geography.columns

    # 2. Join base order data and explicitly select columns to avoid ambiguity
    base_select_cols = [
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
        (
            col("so.PROCESSING_TIMESTAMP")
            if has_processing_ts
            else lit(None).cast("timestamp").alias("PROCESSING_TIMESTAMP")
        ),
        col("so.SHIPPED_TIMESTAMP"),
        col("so.DELIVERED_TIMESTAMP"),
        col("so.SESSION_ID"),
        col("so.USER_SURROGATE_KEY"),
        col("su.CITY"),
        col("su.STATE"),
        col("su.COUNTRY"),
        (
            col("su.ZIP_CODE")
            if has_user_zip
            else lit(None).cast("string").alias("ZIP_CODE")
        ),
        col("pa.payment_installments_count"),
        col("pa.payment_methods_used_count"),
        col("pa.primary_payment_type"),
        col("pa.installment_tier"),
    ]

    sales_base = (
        silver_order_items.alias("soi")
        .join(silver_orders.alias("so"), on="ORDER_ID", how="left")
        .join(payments_agg.alias("pa"), on="ORDER_ID", how="left")
        .join(
            silver_users.alias("su"),
            col("so.USER_SURROGATE_KEY") == col("su.USER_SURROGATE_KEY"),
            "left",
        )
        .select(*base_select_cols)
    )

    # 3. Calculate item-level measures and dates
    sales_measures = (
        sales_base.withColumn("gross_item_amount", col("QUANTITY") * col("UNIT_PRICE"))
        .withColumn(
            "net_item_amount",
            col("gross_item_amount") - coalesce(col("DISCOUNT"), lit(0.0)),
        )
        .withColumn(
            "shipping_method_clean", coalesce(col("SHIPPING_METHOD"), lit("Standard"))
        )
        .withColumn("order_date", to_date("CREATED_AT_TIMESTAMP"))
        .withColumn("paid_date", to_date("PAID_TIMESTAMP"))
        .withColumn("shipped_date", to_date("SHIPPED_TIMESTAMP"))
        .withColumn("delivered_date", to_date("DELIVERED_TIMESTAMP"))
        .withColumn("order_hour", hour("CREATED_AT_TIMESTAMP"))
        .withColumn("order_minute", minute("CREATED_AT_TIMESTAMP"))
        .withColumn("order_second", second("CREATED_AT_TIMESTAMP"))
    )

    # Prepare dimension join conditions to prevent row multiplication
    if has_user_zip and has_geo_zip:
        geo_dedup = dim_geography.dropDuplicates(
            ["CITY", "STATE", "COUNTRY", "ZIP_CODE"]
        )
        geo_join_cond = (
            (trim(col("sm.CITY")) == trim(col("dg.CITY")))
            & (trim(col("sm.STATE")) == trim(col("dg.STATE")))
            & (trim(col("sm.COUNTRY")) == trim(col("dg.COUNTRY")))
            & (
                coalesce(trim(col("sm.ZIP_CODE")), lit(""))
                == coalesce(trim(col("dg.ZIP_CODE")), lit(""))
            )
        )
    else:
        geo_dedup = dim_geography.dropDuplicates(["CITY", "STATE", "COUNTRY"])
        geo_join_cond = (
            (trim(col("sm.CITY")) == trim(col("dg.CITY")))
            & (trim(col("sm.STATE")) == trim(col("dg.STATE")))
            & (trim(col("sm.COUNTRY")) == trim(col("dg.COUNTRY")))
        )

    if "INSTALLMENT_TIER" in dim_payment_type.columns:
        dpt_dedup = dim_payment_type.dropDuplicates(
            ["PAYMENT_TYPE_NAME", "INSTALLMENT_TIER"]
        )
        dpt_join_cond = (
            col("sm.primary_payment_type") == col("dpt.PAYMENT_TYPE_NAME")
        ) & (col("sm.installment_tier") == col("dpt.INSTALLMENT_TIER"))
    else:
        dpt_dedup = dim_payment_type.dropDuplicates(["PAYMENT_TYPE_NAME"])
        dpt_join_cond = col("sm.primary_payment_type") == col("dpt.PAYMENT_TYPE_NAME")

    dsc_dedup = dim_sales_context.dropDuplicates(["SHIPPING_METHOD"])

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
            dim_session.alias("ds"),
            col("sm.SESSION_ID") == col("ds.SESSION_ID"),
            "left",
        )
        .join(
            dim_user.alias("du"),
            col("sm.USER_SURROGATE_KEY") == col("du.USER_SURROGATE_KEY"),
            "left",
        )
        .join(
            geo_dedup.alias("dg"),
            geo_join_cond,
            "left",
        )
        .join(
            dpt_dedup.alias("dpt"),
            dpt_join_cond,
            "left",
        )
        .join(
            dsc_dedup.alias("dsc"),
            col("sm.shipping_method_clean") == col("dsc.SHIPPING_METHOD"),
            "left",
        )
        .join(
            dim_product.alias("dp"),
            (col("sm.PRODUCT_ID") == col("dp.PRODUCT_ID"))
            & (col("sm.CREATED_AT_TIMESTAMP") >= col("dp.VALID_FROM"))
            & (
                (col("sm.CREATED_AT_TIMESTAMP") < col("dp.VALID_TO"))
                | col("dp.VALID_TO").isNull()
            ),
            "left",
        )
    )

    # 5. Finalize financial, physical, and fulfillment calculations
    unit_cost_price = coalesce(
        col("dp.COST_PRICE"),
        col("dp.BASE_PRICE") * lit(0.7),
        col("sm.UNIT_PRICE") * lit(0.7),
    )
    total_cost = unit_cost_price * col("sm.QUANTITY")

    raw_minutes_to_pay = (
        unix_timestamp("sm.PAID_TIMESTAMP") - unix_timestamp("sm.CREATED_AT_TIMESTAMP")
    ) / 60

    raw_hours_to_process = when(
        col("sm.PROCESSING_TIMESTAMP").isNotNull(),
        (
            unix_timestamp("sm.PROCESSING_TIMESTAMP")
            - unix_timestamp("sm.PAID_TIMESTAMP")
        )
        / 3600,
    ).otherwise(
        (unix_timestamp("sm.SHIPPED_TIMESTAMP") - unix_timestamp("sm.PAID_TIMESTAMP"))
        / 3600
    )

    raw_hours_to_ship = when(
        col("sm.PROCESSING_TIMESTAMP").isNotNull(),
        (
            unix_timestamp("sm.SHIPPED_TIMESTAMP")
            - unix_timestamp("sm.PROCESSING_TIMESTAMP")
        )
        / 3600,
    ).otherwise(
        (unix_timestamp("sm.SHIPPED_TIMESTAMP") - unix_timestamp("sm.PAID_TIMESTAMP"))
        / 3600
    )

    raw_hours_to_deliver = (
        unix_timestamp("sm.DELIVERED_TIMESTAMP")
        - unix_timestamp("sm.SHIPPED_TIMESTAMP")
    ) / 3600

    raw_fulfillment_days = (
        unix_timestamp("sm.DELIVERED_TIMESTAMP")
        - unix_timestamp("sm.CREATED_AT_TIMESTAMP")
    ) / 86400

    has_weight = "WEIGHT_G" in dim_product.columns
    has_dims = {"LENGTH_CM", "WIDTH_CM", "HEIGHT_CM"}.issubset(set(dim_product.columns))

    total_weight_expr = (
        (coalesce(col("dp.WEIGHT_G"), lit(0.0)) * col("sm.QUANTITY")).cast("integer")
        if has_weight
        else lit(0)
    )
    total_volume_expr = (
        (
            coalesce(col("dp.LENGTH_CM"), lit(0.0))
            * coalesce(col("dp.WIDTH_CM"), lit(0.0))
            * coalesce(col("dp.HEIGHT_CM"), lit(0.0))
            * col("sm.QUANTITY")
        ).cast("integer")
        if has_dims
        else lit(0)
    )

    fact_final = (
        fact_df.withColumn(
            "net_profit_margin",
            when(
                col("sm.net_item_amount") > 0,
                (col("sm.net_item_amount") - total_cost) / col("sm.net_item_amount"),
            ).otherwise(lit(0.0)),
        )
        .withColumn("minutes_to_pay", greatest(raw_minutes_to_pay, lit(0.0)))
        .withColumn("hours_to_process", greatest(raw_hours_to_process, lit(0.0)))
        .withColumn("hours_to_ship", greatest(raw_hours_to_ship, lit(0.0)))
        .withColumn("hours_to_deliver", greatest(raw_hours_to_deliver, lit(0.0)))
        .withColumn("total_fulfillment_days", greatest(raw_fulfillment_days, lit(0.0)))
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
        total_weight_expr.alias("TOTAL_ITEM_WEIGHT_G"),
        total_volume_expr.alias("TOTAL_ITEM_VOLUME_CM3"),
        col("sm.payment_installments_count").alias("PAYMENT_INSTALLMENTS_COUNT"),
        col("sm.payment_methods_used_count").alias("PAYMENT_METHODS_USED_COUNT"),
        col("minutes_to_pay").cast("integer").alias("MINUTES_TO_PAY"),
        col("hours_to_process").cast("integer").alias("HOURS_TO_PROCESS"),
        col("hours_to_ship").cast("integer").alias("HOURS_TO_SHIP"),
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
