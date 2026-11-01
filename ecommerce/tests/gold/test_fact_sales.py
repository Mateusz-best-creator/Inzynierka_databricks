import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp
from ecommerce.logic_ecommerce.gold.fact_sales import prepare_fact_sales_data


def test_prepare_fact_sales_schema(spark: SparkSession) -> None:
    # Verify the exact output columns and their order in the resulting fact table
    silver_order_items = spark.createDataFrame(
        [],
        schema="ORDER_ID string, ITEM_SEQUENCE int, PRODUCT_ID string, QUANTITY int, UNIT_PRICE double, DISCOUNT double",
    )
    silver_orders = spark.createDataFrame(
        [],
        schema="ORDER_ID string, FREIGHT_VALUE double, SHIPPING_METHOD string, CREATED_AT_TIMESTAMP timestamp, PAID_TIMESTAMP timestamp, PROCESSING_TIMESTAMP timestamp, SHIPPED_TIMESTAMP timestamp, DELIVERED_TIMESTAMP timestamp, SESSION_ID string, USER_SURROGATE_KEY int",
    )
    silver_payments = spark.createDataFrame(
        [], schema="ORDER_ID string, PAYMENT_INSTALLMENTS int, PAYMENT_TYPE string"
    )
    silver_users = spark.createDataFrame(
        [],
        schema="USER_SURROGATE_KEY int, CITY string, STATE string, COUNTRY string, ZIP_CODE string",
    )

    dim_date = spark.createDataFrame([], schema="FULL_DATE date, DATE_KEY int")
    dim_time = spark.createDataFrame(
        [], schema="HOUR int, MINUTE int, SECONDS int, TIME_KEY int"
    )
    dim_session = spark.createDataFrame(
        [], schema="SESSION_ID string, SESSION_SURROGATE_KEY int"
    )
    dim_user = spark.createDataFrame([], schema="USER_SURROGATE_KEY int")
    dim_geography = spark.createDataFrame(
        [],
        schema="CITY string, STATE string, COUNTRY string, ZIP_CODE string, GEOGRAPHY_KEY int",
    )
    dim_payment_type = spark.createDataFrame(
        [],
        schema="PAYMENT_TYPE_NAME string, INSTALLMENT_TIER string, PAYMENT_TYPE_KEY int",
    )
    dim_sales_context = spark.createDataFrame(
        [], schema="SHIPPING_METHOD string, SALES_CONTEXT_KEY int"
    )
    dim_product = spark.createDataFrame(
        [],
        schema="PRODUCT_SURROGATE_KEY int, PRODUCT_ID string, BASE_PRICE double, COST_PRICE double, WEIGHT_G double, LENGTH_CM double, WIDTH_CM double, HEIGHT_CM double, VALID_FROM timestamp, VALID_TO timestamp",
    )

    result_df = prepare_fact_sales_data(
        silver_orders,
        silver_order_items,
        silver_payments,
        silver_users,
        dim_date,
        dim_time,
        dim_session,
        dim_user,
        dim_geography,
        dim_product,
        dim_payment_type,
        dim_sales_context,
    )

    expected_columns = [
        "ORDER_DATE_KEY",
        "ORDER_TIME_KEY",
        "PAID_DATE_KEY",
        "SHIPPED_DATE_KEY",
        "DELIVERED_DATE_KEY",
        "USER_SURROGATE_KEY",
        "GEOGRAPHY_KEY",
        "PRODUCT_SURROGATE_KEY",
        "SESSION_SURROGATE_KEY",
        "PAYMENT_TYPE_KEY",
        "SALES_CONTEXT_KEY",
        "ORDER_ID",
        "ITEM_SEQUENCE",
        "QUANTITY",
        "UNIT_PRICE",
        "UNIT_COST_PRICE",
        "DISCOUNT_AMOUNT",
        "GROSS_ITEM_AMOUNT",
        "NET_ITEM_AMOUNT",
        "FREIGHT_VALUE",
        "TOTAL_LINE_AMOUNT",
        "NET_PROFIT_MARGIN",
        "TOTAL_ITEM_WEIGHT_G",
        "TOTAL_ITEM_VOLUME_CM3",
        "PAYMENT_INSTALLMENTS_COUNT",
        "PAYMENT_METHODS_USED_COUNT",
        "MINUTES_TO_PAY",
        "HOURS_TO_PROCESS",
        "HOURS_TO_SHIP",
        "HOURS_TO_DELIVER",
        "TOTAL_FULFILLMENT_DAYS",
    ]
    assert result_df.columns == expected_columns


def test_prepare_fact_sales_metrics_and_aggregations(spark: SparkSession) -> None:
    # Setup mock sales data to verify complex financial measures and timestamp differences
    silver_order_items = spark.createDataFrame(
        [("ORD-1", 1, "PROD-1", 2, 100.0, 10.0)],
        [
            "ORDER_ID",
            "ITEM_SEQUENCE",
            "PRODUCT_ID",
            "QUANTITY",
            "UNIT_PRICE",
            "DISCOUNT",
        ],
    )

    silver_orders = spark.createDataFrame(
        [
            (
                "ORD-1",
                15.0,
                "Express",
                "2025-01-01 10:00:00",
                "2025-01-01 10:30:00",
                "2025-01-02 10:30:00",
                "2025-01-04 10:30:00",
                "SESS-1",
                99,
            )
        ],
        [
            "ORDER_ID",
            "FREIGHT_VALUE",
            "SHIPPING_METHOD",
            "CREATED_AT_TIMESTAMP",
            "PAID_TIMESTAMP",
            "SHIPPED_TIMESTAMP",
            "DELIVERED_TIMESTAMP",
            "SESSION_ID",
            "USER_SURROGATE_KEY",
        ],
    ).select(
        "ORDER_ID",
        "FREIGHT_VALUE",
        "SHIPPING_METHOD",
        "SESSION_ID",
        "USER_SURROGATE_KEY",
        to_timestamp("CREATED_AT_TIMESTAMP").alias("CREATED_AT_TIMESTAMP"),
        to_timestamp("PAID_TIMESTAMP").alias("PAID_TIMESTAMP"),
        to_timestamp("SHIPPED_TIMESTAMP").alias("SHIPPED_TIMESTAMP"),
        to_timestamp("DELIVERED_TIMESTAMP").alias("DELIVERED_TIMESTAMP"),
    )

    silver_payments = spark.createDataFrame(
        [("ORD-1", 1, "CREDIT_CARD"), ("ORD-1", 3, "BLIK")],
        ["ORDER_ID", "PAYMENT_INSTALLMENTS", "PAYMENT_TYPE"],
    )

    silver_users = spark.createDataFrame(
        [(99, "Warsaw", "Maz", "PL")],
        ["USER_SURROGATE_KEY", "CITY", "STATE", "COUNTRY"],
    )
    dim_date = spark.createDataFrame(
        [("2025-01-01", 20250101)], ["FULL_DATE", "DATE_KEY"]
    )
    dim_time = spark.createDataFrame(
        [(10, 0, 0, 100000)], ["HOUR", "MINUTE", "SECONDS", "TIME_KEY"]
    )
    dim_session = spark.createDataFrame(
        [("SESS-1", 101)], ["SESSION_ID", "SESSION_SURROGATE_KEY"]
    )
    dim_user = spark.createDataFrame([(99,)], ["USER_SURROGATE_KEY"])
    dim_geography = spark.createDataFrame(
        [("Warsaw", "Maz", "PL", 55)], ["CITY", "STATE", "COUNTRY", "GEOGRAPHY_KEY"]
    )
    dim_payment_type = spark.createDataFrame(
        [("CREDIT_CARD", 1)], ["PAYMENT_TYPE_NAME", "PAYMENT_TYPE_KEY"]
    )
    dim_sales_context = spark.createDataFrame(
        [("Express", 11)], ["SHIPPING_METHOD", "SALES_CONTEXT_KEY"]
    )

    dim_product = (
        spark.createDataFrame(
            [(1, "PROD-1", 100.0, 50.0, "2024-01-01 00:00:00", None)],
            schema="PRODUCT_SURROGATE_KEY int, PRODUCT_ID string, BASE_PRICE double, COST_PRICE double, VALID_FROM string, VALID_TO string",
        )
        .withColumn("VALID_FROM", to_timestamp("VALID_FROM"))
        .withColumn("VALID_TO", to_timestamp("VALID_TO"))
    )

    # Execute fact transformation
    result_df = prepare_fact_sales_data(
        silver_orders,
        silver_order_items,
        silver_payments,
        silver_users,
        dim_date,
        dim_time,
        dim_session,
        dim_user,
        dim_geography,
        dim_product,
        dim_payment_type,
        dim_sales_context,
    )

    row = result_df.collect()[0]

    # Verify payments aggregation (Max installments, count of methods)
    assert row["PAYMENT_INSTALLMENTS_COUNT"] == 3
    assert row["PAYMENT_METHODS_USED_COUNT"] == 2

    # Verify financial line calculations:
    # Qty 2 * 100 = 200 Gross
    # 200 - 10 discount = 190 Net
    assert row["GROSS_ITEM_AMOUNT"] == 200.0
    assert row["NET_ITEM_AMOUNT"] == 190.0
    assert row["TOTAL_LINE_AMOUNT"] == 190.0 + 15.0  # Net + Freight

    # Verify profit margin calculation:
    # Item net is 190. Cost is 50 per unit * 2 = 100.
    # Profit = (190 - 100) / 190 = 90 / 190 = 0.4736... -> rounded to 0.47
    assert float(row["NET_PROFIT_MARGIN"]) == pytest.approx(0.47, 0.01)

    # Verify duration metrics
    assert row["MINUTES_TO_PAY"] == 30  # 10:00 to 10:30
    assert row["HOURS_TO_PROCESS"] == 24  # 10:30 to next day 10:30
    assert row["HOURS_TO_DELIVER"] == 48  # Jan 02 to Jan 04
    assert float(row["TOTAL_FULFILLMENT_DAYS"]) == pytest.approx(
        3.02, 0.01
    )  # Jan 01 to Jan 04

    # Verify surrogate keys
    assert row["ORDER_DATE_KEY"] == 20250101
    assert row["PRODUCT_SURROGATE_KEY"] == 1


def test_prepare_fact_sales_no_fanout_and_extended_metrics(spark: SparkSession) -> None:
    # Verify no row multiplication across multi-tier payment types and zip codes, plus physical and processing metrics
    silver_order_items = spark.createDataFrame(
        [("ORD-2", 1, "PROD-2", 3, 50.0, 0.0)],
        [
            "ORDER_ID",
            "ITEM_SEQUENCE",
            "PRODUCT_ID",
            "QUANTITY",
            "UNIT_PRICE",
            "DISCOUNT",
        ],
    )

    # Use a DDL schema string so Spark knows None maps to a string type for SHIPPING_METHOD
    silver_orders = spark.createDataFrame(
        [
            (
                "ORD-2",
                10.0,
                None,
                "2025-01-01 10:00:00",
                "2025-01-01 10:15:00",
                "2025-01-01 14:15:00",
                "2025-01-02 02:15:00",
                "2025-01-03 02:15:00",
                "SESS-2",
                99,
            )
        ],
        schema="ORDER_ID string, FREIGHT_VALUE double, SHIPPING_METHOD string, CREATED_AT_TIMESTAMP string, PAID_TIMESTAMP string, PROCESSING_TIMESTAMP string, SHIPPED_TIMESTAMP string, DELIVERED_TIMESTAMP string, SESSION_ID string, USER_SURROGATE_KEY int",
    ).select(
        "ORDER_ID",
        "FREIGHT_VALUE",
        "SHIPPING_METHOD",
        "SESSION_ID",
        "USER_SURROGATE_KEY",
        to_timestamp("CREATED_AT_TIMESTAMP").alias("CREATED_AT_TIMESTAMP"),
        to_timestamp("PAID_TIMESTAMP").alias("PAID_TIMESTAMP"),
        to_timestamp("PROCESSING_TIMESTAMP").alias("PROCESSING_TIMESTAMP"),
        to_timestamp("SHIPPED_TIMESTAMP").alias("SHIPPED_TIMESTAMP"),
        to_timestamp("DELIVERED_TIMESTAMP").alias("DELIVERED_TIMESTAMP"),
    )

    silver_payments = spark.createDataFrame(
        [("ORD-2", 6, "CREDIT_CARD")],
        ["ORDER_ID", "PAYMENT_INSTALLMENTS", "PAYMENT_TYPE"],
    )

    silver_users = spark.createDataFrame(
        [(99, "Warsaw", "Maz", "PL", "00-002")],
        ["USER_SURROGATE_KEY", "CITY", "STATE", "COUNTRY", "ZIP_CODE"],
    )
    dim_date = spark.createDataFrame(
        [("2025-01-01", 20250101)], ["FULL_DATE", "DATE_KEY"]
    )
    dim_time = spark.createDataFrame(
        [(10, 0, 0, 100000)], ["HOUR", "MINUTE", "SECONDS", "TIME_KEY"]
    )
    dim_session = spark.createDataFrame(
        [("SESS-2", 102)], ["SESSION_ID", "SESSION_SURROGATE_KEY"]
    )
    dim_user = spark.createDataFrame([(99,)], ["USER_SURROGATE_KEY"])
    dim_geography = spark.createDataFrame(
        [
            ("Warsaw", "Maz", "PL", "00-001", 55),
            ("Warsaw", "Maz", "PL", "00-002", 56),
        ],
        ["CITY", "STATE", "COUNTRY", "ZIP_CODE", "GEOGRAPHY_KEY"],
    )
    dim_payment_type = spark.createDataFrame(
        [
            ("CREDIT_CARD", "Single Payment", 1),
            ("CREDIT_CARD", "2-3 Installments", 2),
            ("CREDIT_CARD", "4+ Installments", 3),
        ],
        ["PAYMENT_TYPE_NAME", "INSTALLMENT_TIER", "PAYMENT_TYPE_KEY"],
    )
    dim_sales_context = spark.createDataFrame(
        [
            ("Standard", "Full Price", "On-Time Delivery", 20),
            ("Standard", "Discounted", "Delayed Delivery", 21),
        ],
        [
            "SHIPPING_METHOD",
            "DISCOUNT_STATUS",
            "DELIVERY_SLA_STATUS",
            "SALES_CONTEXT_KEY",
        ],
    )

    dim_product = (
        spark.createDataFrame(
            [
                (
                    10,
                    "PROD-2",
                    50.0,
                    30.0,
                    200.0,
                    10.0,
                    5.0,
                    4.0,
                    "2024-01-01 00:00:00",
                    None,
                )
            ],
            schema="PRODUCT_SURROGATE_KEY int, PRODUCT_ID string, BASE_PRICE double, COST_PRICE double, WEIGHT_G double, LENGTH_CM double, WIDTH_CM double, HEIGHT_CM double, VALID_FROM string, VALID_TO string",
        )
        .withColumn("VALID_FROM", to_timestamp("VALID_FROM"))
        .withColumn("VALID_TO", to_timestamp("VALID_TO"))
    )

    result_df = prepare_fact_sales_data(
        silver_orders,
        silver_order_items,
        silver_payments,
        silver_users,
        dim_date,
        dim_time,
        dim_session,
        dim_user,
        dim_geography,
        dim_product,
        dim_payment_type,
        dim_sales_context,
    )

    rows = result_df.collect()
    assert len(rows) == 1

    row = rows[0]
    assert row["PAYMENT_TYPE_KEY"] == 3
    assert row["GEOGRAPHY_KEY"] == 56
    assert row["HOURS_TO_PROCESS"] == 4
    assert row["HOURS_TO_SHIP"] == 12
    assert row["TOTAL_ITEM_WEIGHT_G"] == 600
    assert row["TOTAL_ITEM_VOLUME_CM3"] == 600
