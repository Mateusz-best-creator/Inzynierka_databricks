import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from ecommerce.logic_ecommerce.silver.orders import (
    add_date_columns,
    prepare_orders_data,
)

ORDERS_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("session_id", StringType(), True),
        StructField("created_at", StringType(), True),
    ]
)

DETAILS_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), True),
        StructField("shipping_method", StringType(), True),
        StructField("shipping_cost", DoubleType(), True),
    ]
)

STATUS_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), True),
        StructField("current_status", StringType(), True),
        StructField("valid_from", StringType(), True),
    ]
)

ITEMS_SCHEMA = StructType(
    [
        StructField("order_item_id", StringType(), True),
        StructField("order_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("quantity", IntegerType(), True),
    ]
)

PRICING_SCHEMA = StructType(
    [
        StructField("order_item_id", StringType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("discount", DoubleType(), True),
    ]
)

USERS_SCHEMA = StructType(
    [
        StructField("user_surrogate_key", IntegerType(), True),
        StructField("user_id", StringType(), True),
        StructField("valid_from", StringType(), True),
        StructField("valid_to", StringType(), True),
    ]
)

EXPECTED_COLUMNS = [
    "ORDER_SURROGATE_KEY",
    "ORDER_ID",
    "TOTAL_AMOUNT",
    "FREIGHT_VALUE",
    "SHIPPING_METHOD",
    "CREATED_AT_TIMESTAMP",
    "PENDING_PAYMENT_TIMESTAMP",
    "PAID_TIMESTAMP",
    "PROCESSING_TIMESTAMP",
    "SHIPPED_TIMESTAMP",
    "DELIVERED_TIMESTAMP",
    "SESSION_ID",
    "USER_SURROGATE_KEY",
]


def test_prepare_orders_schema_and_empty_input(spark: SparkSession) -> None:
    # Verify output schema and empty dataframe handling
    joined_df = prepare_orders_data(
        orders=spark.createDataFrame([], schema=ORDERS_SCHEMA),
        order_details=spark.createDataFrame([], schema=DETAILS_SCHEMA),
        order_status_history=spark.createDataFrame([], schema=STATUS_SCHEMA),
        order_items=spark.createDataFrame([], schema=ITEMS_SCHEMA),
        order_item_pricing=spark.createDataFrame([], schema=PRICING_SCHEMA),
        silver_users=spark.createDataFrame([], schema=USERS_SCHEMA),
    )

    assert joined_df.columns == EXPECTED_COLUMNS
    assert joined_df.count() == 0

    with_dates_df = add_date_columns(joined_df)
    assert with_dates_df.columns == EXPECTED_COLUMNS + ["YEAR", "MONTH", "QUARTER"]


def test_prepare_orders_calculations_and_status_pivots(spark: SparkSession) -> None:
    # Verify order total calculation, status timestamp pivoting, and SCD2 user lookup
    orders_df = spark.createDataFrame(
        [("ORD-100", "USR-10", "SESS-10", "2025-03-10 12:00:00")],
        schema=ORDERS_SCHEMA,
    )
    order_details_df = spark.createDataFrame(
        [("ORD-100", "EXPRESS", 10.0)],
        schema=DETAILS_SCHEMA,
    )
    order_status_df = spark.createDataFrame(
        [
            ("ORD-100", "PENDING_PAYMENT", "2025-03-10 12:01:00"),
            ("ORD-100", "PAID", "2025-03-10 12:05:00"),
            ("ORD-100", "SHIPPED", "2025-03-11 08:00:00"),
        ],
        schema=STATUS_SCHEMA,
    )
    order_items_df = spark.createDataFrame(
        [
            ("ITEM-1", "ORD-100", "PROD-1", 2),
            ("ITEM-2", "ORD-100", "PROD-2", 1),
        ],
        schema=ITEMS_SCHEMA,
    )
    order_pricing_df = spark.createDataFrame(
        [
            ("ITEM-1", 100.0, 10.0),
            ("ITEM-2", 50.0, 0.0),
        ],
        schema=PRICING_SCHEMA,
    )
    silver_users_df = spark.createDataFrame(
        [(99, "USR-10", "2025-01-01 00:00:00", None)],
        schema=USERS_SCHEMA,
    )

    result_df = prepare_orders_data(
        orders=orders_df,
        order_details=order_details_df,
        order_status_history=order_status_df,
        order_items=order_items_df,
        order_item_pricing=order_pricing_df,
        silver_users=silver_users_df,
    )
    row = result_df.collect()[0]

    assert row["ORDER_ID"] == "ORD-100"
    assert row["TOTAL_AMOUNT"] == pytest.approx(240.0)
    assert row["FREIGHT_VALUE"] == pytest.approx(10.0)
    assert row["SHIPPING_METHOD"] == "EXPRESS"
    assert row["USER_SURROGATE_KEY"] == 99
    assert row["PENDING_PAYMENT_TIMESTAMP"] == "2025-03-10 12:01:00"
    assert row["PAID_TIMESTAMP"] == "2025-03-10 12:05:00"
    assert row["SHIPPED_TIMESTAMP"] == "2025-03-11 08:00:00"
    assert row["DELIVERED_TIMESTAMP"] is None


def test_prepare_orders_sanitization_and_missing_details(spark: SparkSession) -> None:
    # Verify anomaly healing (1900 date, swapped status times, negative prices) and missing detail defaults
    orders_df = spark.createDataFrame(
        [
            ("ORD-ERR", "USR-1", "SESS-1", "1900-01-01 00:00:00"),
            ("ORD-EMPTY", "USR-1", "SESS-2", "2025-05-02 11:00:00"),
        ],
        schema=ORDERS_SCHEMA,
    )
    order_details_df = spark.createDataFrame(
        [("ORD-ERR", "EXPRESS", -15.0)],
        schema=DETAILS_SCHEMA,
    )
    order_status_df = spark.createDataFrame(
        [
            ("ORD-ERR", "PENDING_PAYMENT", "2025-04-10 10:01:00"),
            ("ORD-ERR", "PAID", "2025-04-11 15:00:00"),
            ("ORD-ERR", "SHIPPED", "2025-04-10 10:05:00"),
        ],
        schema=STATUS_SCHEMA,
    )
    order_items_df = spark.createDataFrame(
        [("ITEM-1", "ORD-ERR", "PROD-1", -2)],
        schema=ITEMS_SCHEMA,
    )
    order_pricing_df = spark.createDataFrame(
        [("ITEM-1", -50.0, -5.0)],
        schema=PRICING_SCHEMA,
    )
    silver_users_df = spark.createDataFrame(
        [(77, "USR-1", "2025-01-01 00:00:00", None)],
        schema=USERS_SCHEMA,
    )

    result_df = prepare_orders_data(
        orders=orders_df,
        order_details=order_details_df,
        order_status_history=order_status_df,
        order_items=order_items_df,
        order_item_pricing=order_pricing_df,
        silver_users=silver_users_df,
    )
    rows = {row["ORDER_ID"]: row for row in result_df.collect()}

    err_row = rows["ORD-ERR"]
    assert err_row["CREATED_AT_TIMESTAMP"] == "2025-04-10 10:01:00"
    assert err_row["USER_SURROGATE_KEY"] == 77
    assert err_row["PAID_TIMESTAMP"] == "2025-04-10 10:05:00"
    assert err_row["SHIPPED_TIMESTAMP"] == "2025-04-11 15:00:00"
    assert err_row["FREIGHT_VALUE"] == pytest.approx(15.0)
    assert err_row["TOTAL_AMOUNT"] == pytest.approx(105.0)

    empty_row = rows["ORD-EMPTY"]
    assert empty_row["SHIPPING_METHOD"] is None
    assert empty_row["FREIGHT_VALUE"] == pytest.approx(0.0)
    assert empty_row["TOTAL_AMOUNT"] == pytest.approx(0.0)


def test_add_date_columns_quarters_and_formatting(spark: SparkSession) -> None:
    # Verify year, zero-padded month, and quarter derivation across all quarters and null dates
    schema = StructType(
        [
            StructField("ORDER_ID", StringType(), True),
            StructField("CREATED_AT_TIMESTAMP", StringType(), True),
        ]
    )
    sample_df = spark.createDataFrame(
        [
            ("O1", "2025-02-15 10:00:00"),
            ("O2", "2025-05-15 10:00:00"),
            ("O3", "2025-08-15 10:00:00"),
            ("O4", "2025-11-15 10:00:00"),
            ("O5", None),
        ],
        schema=schema,
    )

    result_df = add_date_columns(sample_df)
    rows = {row["ORDER_ID"]: row for row in result_df.collect()}

    assert rows["O1"]["YEAR"] == 2025
    assert rows["O1"]["MONTH"] == "02"
    assert rows["O1"]["QUARTER"] == "Q1"

    assert rows["O2"]["MONTH"] == "05"
    assert rows["O2"]["QUARTER"] == "Q2"

    assert rows["O3"]["MONTH"] == "08"
    assert rows["O3"]["QUARTER"] == "Q3"

    assert rows["O4"]["MONTH"] == "11"
    assert rows["O4"]["QUARTER"] == "Q4"

    assert rows["O5"]["QUARTER"] == "INCORRECT_DATE"
