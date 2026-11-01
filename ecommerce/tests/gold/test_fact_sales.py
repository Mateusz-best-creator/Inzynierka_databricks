import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from ecommerce.logic_ecommerce.gold.fact_sales import prepare_fact_sales_data

SILVER_ORDER_ITEMS_SCHEMA = StructType(
    [
        StructField("ORDER_ID", StringType(), True),
        StructField("ITEM_SEQUENCE", IntegerType(), True),
        StructField("PRODUCT_ID", StringType(), True),
        StructField("QUANTITY", IntegerType(), True),
        StructField("UNIT_PRICE", DoubleType(), True),
        StructField("DISCOUNT", DoubleType(), True),
    ]
)

SILVER_ORDERS_SCHEMA = StructType(
    [
        StructField("ORDER_ID", StringType(), True),
        StructField("FREIGHT_VALUE", DoubleType(), True),
        StructField("SHIPPING_METHOD", StringType(), True),
        StructField("CREATED_AT_TIMESTAMP", StringType(), True),
        StructField("PAID_TIMESTAMP", StringType(), True),
        StructField("PROCESSING_TIMESTAMP", StringType(), True),
        StructField("SHIPPED_TIMESTAMP", StringType(), True),
        StructField("DELIVERED_TIMESTAMP", StringType(), True),
        StructField("SESSION_ID", StringType(), True),
        StructField("USER_SURROGATE_KEY", IntegerType(), True),
    ]
)

SILVER_PAYMENTS_SCHEMA = StructType(
    [
        StructField("ORDER_ID", StringType(), True),
        StructField("PAYMENT_INSTALLMENTS", IntegerType(), True),
        StructField("PAYMENT_TYPE", StringType(), True),
    ]
)

SILVER_USERS_SCHEMA = StructType(
    [
        StructField("USER_SURROGATE_KEY", IntegerType(), True),
        StructField("CITY", StringType(), True),
        StructField("STATE", StringType(), True),
        StructField("COUNTRY", StringType(), True),
        StructField("ZIP_CODE", StringType(), True),
    ]
)

DIM_DATE_SCHEMA = StructType(
    [
        StructField("FULL_DATE", StringType(), True),
        StructField("DATE_KEY", IntegerType(), True),
    ]
)

DIM_TIME_SCHEMA = StructType(
    [
        StructField("HOUR", IntegerType(), True),
        StructField("MINUTE", IntegerType(), True),
        StructField("SECONDS", IntegerType(), True),
        StructField("TIME_KEY", IntegerType(), True),
    ]
)

DIM_SESSION_SCHEMA = StructType(
    [
        StructField("SESSION_ID", StringType(), True),
        StructField("SESSION_SURROGATE_KEY", IntegerType(), True),
    ]
)

DIM_USER_SCHEMA = StructType(
    [
        StructField("USER_SURROGATE_KEY", IntegerType(), True),
    ]
)

DIM_GEOGRAPHY_SCHEMA = StructType(
    [
        StructField("CITY", StringType(), True),
        StructField("STATE", StringType(), True),
        StructField("COUNTRY", StringType(), True),
        StructField("ZIP_CODE", StringType(), True),
        StructField("GEOGRAPHY_KEY", IntegerType(), True),
    ]
)

DIM_PAYMENT_TYPE_SCHEMA = StructType(
    [
        StructField("PAYMENT_TYPE_NAME", StringType(), True),
        StructField("INSTALLMENT_TIER", StringType(), True),
        StructField("PAYMENT_TYPE_KEY", IntegerType(), True),
    ]
)

DIM_SALES_CONTEXT_SCHEMA = StructType(
    [
        StructField("SHIPPING_METHOD", StringType(), True),
        StructField("DISCOUNT_STATUS", StringType(), True),
        StructField("DELIVERY_SLA_STATUS", StringType(), True),
        StructField("SALES_CONTEXT_KEY", IntegerType(), True),
    ]
)

DIM_PRODUCT_SCHEMA = StructType(
    [
        StructField("PRODUCT_SURROGATE_KEY", IntegerType(), True),
        StructField("PRODUCT_ID", StringType(), True),
        StructField("BASE_PRICE", DoubleType(), True),
        StructField("COST_PRICE", DoubleType(), True),
        StructField("WEIGHT_G", DoubleType(), True),
        StructField("LENGTH_CM", DoubleType(), True),
        StructField("WIDTH_CM", DoubleType(), True),
        StructField("HEIGHT_CM", DoubleType(), True),
        StructField("VALID_FROM", StringType(), True),
        StructField("VALID_TO", StringType(), True),
    ]
)

EXPECTED_COLUMNS = [
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


def test_prepare_fact_sales_schema_and_empty_input(spark: SparkSession) -> None:
    # Verify output schema and empty dataframe handling
    result_df = prepare_fact_sales_data(
        silver_orders=spark.createDataFrame([], schema=SILVER_ORDERS_SCHEMA),
        silver_order_items=spark.createDataFrame([], schema=SILVER_ORDER_ITEMS_SCHEMA),
        silver_payments=spark.createDataFrame([], schema=SILVER_PAYMENTS_SCHEMA),
        silver_users=spark.createDataFrame([], schema=SILVER_USERS_SCHEMA),
        dim_date=spark.createDataFrame([], schema=DIM_DATE_SCHEMA),
        dim_time=spark.createDataFrame([], schema=DIM_TIME_SCHEMA),
        dim_session=spark.createDataFrame([], schema=DIM_SESSION_SCHEMA),
        dim_user=spark.createDataFrame([], schema=DIM_USER_SCHEMA),
        dim_geography=spark.createDataFrame([], schema=DIM_GEOGRAPHY_SCHEMA),
        dim_product=spark.createDataFrame([], schema=DIM_PRODUCT_SCHEMA),
        dim_payment_type=spark.createDataFrame([], schema=DIM_PAYMENT_TYPE_SCHEMA),
        dim_sales_context=spark.createDataFrame([], schema=DIM_SALES_CONTEXT_SCHEMA),
    )

    assert result_df.columns == EXPECTED_COLUMNS
    assert result_df.count() == 0


def test_prepare_fact_sales_financial_metrics_and_aggregations(
    spark: SparkSession,
) -> None:
    # Verify financial line calculations, profit margins, payment aggregations, and fulfillment times
    silver_order_items = spark.createDataFrame(
        [("ORD-1", 1, "PROD-1", 2, 100.0, 10.0)],
        schema=SILVER_ORDER_ITEMS_SCHEMA,
    )
    silver_orders = spark.createDataFrame(
        [
            (
                "ORD-1",
                15.0,
                "Express",
                "2025-01-01 10:00:00",
                "2025-01-01 10:30:00",
                None,
                "2025-01-02 10:30:00",
                "2025-01-04 10:30:00",
                "SESS-1",
                99,
            )
        ],
        schema=SILVER_ORDERS_SCHEMA,
    )
    silver_payments = spark.createDataFrame(
        [("ORD-1", 1, "CREDIT_CARD"), ("ORD-1", 3, "BLIK")],
        schema=SILVER_PAYMENTS_SCHEMA,
    )
    silver_users = spark.createDataFrame(
        [(99, "Warsaw", "Maz", "PL", "00-001")],
        schema=SILVER_USERS_SCHEMA,
    )
    dim_date = spark.createDataFrame([("2025-01-01", 20250101)], schema=DIM_DATE_SCHEMA)
    dim_time = spark.createDataFrame([(10, 0, 0, 100000)], schema=DIM_TIME_SCHEMA)
    dim_session = spark.createDataFrame([("SESS-1", 101)], schema=DIM_SESSION_SCHEMA)
    dim_user = spark.createDataFrame([(99,)], schema=DIM_USER_SCHEMA)
    dim_geography = spark.createDataFrame(
        [("Warsaw", "Maz", "PL", "00-001", 55)],
        schema=DIM_GEOGRAPHY_SCHEMA,
    )
    dim_payment_type = spark.createDataFrame(
        [("CREDIT_CARD", "2-3 Installments", 1)],
        schema=DIM_PAYMENT_TYPE_SCHEMA,
    )
    dim_sales_context = spark.createDataFrame(
        [("Express", "Discounted", "On-Time Delivery", 11)],
        schema=DIM_SALES_CONTEXT_SCHEMA,
    )
    dim_product = spark.createDataFrame(
        [
            (
                1,
                "PROD-1",
                100.0,
                50.0,
                None,
                None,
                None,
                None,
                "2024-01-01 00:00:00",
                None,
            )
        ],
        schema=DIM_PRODUCT_SCHEMA,
    )

    result_df = prepare_fact_sales_data(
        silver_orders=silver_orders,
        silver_order_items=silver_order_items,
        silver_payments=silver_payments,
        silver_users=silver_users,
        dim_date=dim_date,
        dim_time=dim_time,
        dim_session=dim_session,
        dim_user=dim_user,
        dim_geography=dim_geography,
        dim_product=dim_product,
        dim_payment_type=dim_payment_type,
        dim_sales_context=dim_sales_context,
    )
    row = result_df.collect()[0]

    assert row["PAYMENT_INSTALLMENTS_COUNT"] == 3
    assert row["PAYMENT_METHODS_USED_COUNT"] == 2
    assert row["GROSS_ITEM_AMOUNT"] == pytest.approx(200.0)
    assert row["NET_ITEM_AMOUNT"] == pytest.approx(190.0)
    assert row["TOTAL_LINE_AMOUNT"] == pytest.approx(205.0)
    assert float(row["NET_PROFIT_MARGIN"]) == pytest.approx(0.47, 0.01)

    assert row["MINUTES_TO_PAY"] == 30
    assert row["HOURS_TO_PROCESS"] == 24
    assert row["HOURS_TO_DELIVER"] == 48
    assert float(row["TOTAL_FULFILLMENT_DAYS"]) == pytest.approx(3.02, 0.01)

    assert row["ORDER_DATE_KEY"] == 20250101
    assert row["PRODUCT_SURROGATE_KEY"] == 1
    assert row["PAYMENT_TYPE_KEY"] == 1


def test_prepare_fact_sales_no_fanout_and_physical_metrics(spark: SparkSession) -> None:
    # Verify no row multiplication across multi-tier dimensions, plus physical and processing metrics
    silver_order_items = spark.createDataFrame(
        [("ORD-2", 1, "PROD-2", 3, 50.0, 0.0)],
        schema=SILVER_ORDER_ITEMS_SCHEMA,
    )
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
        schema=SILVER_ORDERS_SCHEMA,
    )
    silver_payments = spark.createDataFrame(
        [("ORD-2", 6, "CREDIT_CARD")],
        schema=SILVER_PAYMENTS_SCHEMA,
    )
    silver_users = spark.createDataFrame(
        [(99, "Warsaw", "Maz", "PL", "00-002")],
        schema=SILVER_USERS_SCHEMA,
    )
    dim_date = spark.createDataFrame([("2025-01-01", 20250101)], schema=DIM_DATE_SCHEMA)
    dim_time = spark.createDataFrame([(10, 0, 0, 100000)], schema=DIM_TIME_SCHEMA)
    dim_session = spark.createDataFrame([("SESS-2", 102)], schema=DIM_SESSION_SCHEMA)
    dim_user = spark.createDataFrame([(99,)], schema=DIM_USER_SCHEMA)
    dim_geography = spark.createDataFrame(
        [
            ("Warsaw", "Maz", "PL", "00-001", 55),
            ("Warsaw", "Maz", "PL", "00-002", 56),
        ],
        schema=DIM_GEOGRAPHY_SCHEMA,
    )
    dim_payment_type = spark.createDataFrame(
        [
            ("CREDIT_CARD", "Single Payment", 1),
            ("CREDIT_CARD", "2-3 Installments", 2),
            ("CREDIT_CARD", "4+ Installments", 3),
        ],
        schema=DIM_PAYMENT_TYPE_SCHEMA,
    )
    dim_sales_context = spark.createDataFrame(
        [
            ("Standard", "Full Price", "On-Time Delivery", 20),
            ("Standard", "Discounted", "Delayed Delivery", 21),
        ],
        schema=DIM_SALES_CONTEXT_SCHEMA,
    )
    dim_product = spark.createDataFrame(
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
        schema=DIM_PRODUCT_SCHEMA,
    )

    result_df = prepare_fact_sales_data(
        silver_orders=silver_orders,
        silver_order_items=silver_order_items,
        silver_payments=silver_payments,
        silver_users=silver_users,
        dim_date=dim_date,
        dim_time=dim_time,
        dim_session=dim_session,
        dim_user=dim_user,
        dim_geography=dim_geography,
        dim_product=dim_product,
        dim_payment_type=dim_payment_type,
        dim_sales_context=dim_sales_context,
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
