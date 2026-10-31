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


def test_prepare_orders_data_schema_and_columns(spark: SparkSession) -> None:
    orders_schema = StructType(
        [
            StructField("order_id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("session_id", StringType(), True),
            StructField("created_at", StringType(), True),
        ]
    )
    details_schema = StructType(
        [
            StructField("order_id", StringType(), True),
            StructField("shipping_method", StringType(), True),
            StructField("shipping_cost", DoubleType(), True),
        ]
    )
    status_schema = StructType(
        [
            StructField("order_id", StringType(), True),
            StructField("current_status", StringType(), True),
            StructField("valid_from", StringType(), True),
        ]
    )
    items_schema = StructType(
        [
            StructField("order_item_id", StringType(), True),
            StructField("order_id", StringType(), True),
            StructField("quantity", IntegerType(), True),
        ]
    )
    pricing_schema = StructType(
        [
            StructField("order_item_id", StringType(), True),
            StructField("unit_price", DoubleType(), True),
            StructField("discount", DoubleType(), True),
        ]
    )
    users_schema = StructType(
        [
            StructField("user_surrogate_key", IntegerType(), True),
            StructField("user_id", StringType(), True),
            StructField("valid_from", StringType(), True),
            StructField("valid_to", StringType(), True),
        ]
    )

    orders_df = spark.createDataFrame(
        [("ORD-1", "USR-1", "SESS-1", "2025-01-15 10:00:00")], schema=orders_schema
    )
    order_details_df = spark.createDataFrame(
        [("ORD-1", "COURIER", 15.0)], schema=details_schema
    )
    order_status_history_df = spark.createDataFrame(
        [("ORD-1", "DELIVERED", "2025-01-18 14:00:00")], schema=status_schema
    )
    order_items_df = spark.createDataFrame(
        [("ITEM-1", "ORD-1", 2)], schema=items_schema
    )
    order_item_pricing_df = spark.createDataFrame(
        [("ITEM-1", 50.0, 5.0)], schema=pricing_schema
    )
    silver_users_df = spark.createDataFrame(
        [(1, "USR-1", "2025-01-01 00:00:00", None)], schema=users_schema
    )

    result_df = prepare_orders_data(
        orders=orders_df,
        order_details=order_details_df,
        order_status_history=order_status_history_df,
        order_items=order_items_df,
        order_item_pricing=order_item_pricing_df,
        silver_users=silver_users_df,
    )

    expected_columns = [
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
    assert result_df.columns == expected_columns


def test_prepare_orders_data_calculations_and_pivots(spark: SparkSession) -> None:
    orders_schema = StructType(
        [
            StructField("order_id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("session_id", StringType(), True),
            StructField("created_at", StringType(), True),
        ]
    )
    details_schema = StructType(
        [
            StructField("order_id", StringType(), True),
            StructField("shipping_method", StringType(), True),
            StructField("shipping_cost", DoubleType(), True),
        ]
    )
    status_schema = StructType(
        [
            StructField("order_id", StringType(), True),
            StructField("current_status", StringType(), True),
            StructField("valid_from", StringType(), True),
        ]
    )
    items_schema = StructType(
        [
            StructField("order_item_id", StringType(), True),
            StructField("order_id", StringType(), True),
            StructField("quantity", IntegerType(), True),
        ]
    )
    pricing_schema = StructType(
        [
            StructField("order_item_id", StringType(), True),
            StructField("unit_price", DoubleType(), True),
            StructField("discount", DoubleType(), True),
        ]
    )
    users_schema = StructType(
        [
            StructField("user_surrogate_key", IntegerType(), True),
            StructField("user_id", StringType(), True),
            StructField("valid_from", StringType(), True),
            StructField("valid_to", StringType(), True),
        ]
    )

    orders_df = spark.createDataFrame(
        [("ORD-100", "USR-10", "SESS-10", "2025-03-10 12:00:00")], schema=orders_schema
    )
    order_details_df = spark.createDataFrame(
        [("ORD-100", "EXPRESS", 10.0)], schema=details_schema
    )
    order_status_history_df = spark.createDataFrame(
        [
            ("ORD-100", "PENDING_PAYMENT", "2025-03-10 12:01:00"),
            ("ORD-100", "PAID", "2025-03-10 12:05:00"),
            ("ORD-100", "SHIPPED", "2025-03-11 08:00:00"),
        ],
        schema=status_schema,
    )
    order_items_df = spark.createDataFrame(
        [
            ("ITEM-1", "ORD-100", 2),
            ("ITEM-2", "ORD-100", 1),
        ],
        schema=items_schema,
    )
    order_item_pricing_df = spark.createDataFrame(
        [
            ("ITEM-1", 100.0, 10.0),
            ("ITEM-2", 50.0, 0.0),
        ],
        schema=pricing_schema,
    )
    silver_users_df = spark.createDataFrame(
        [(99, "USR-10", "2025-01-01 00:00:00", None)], schema=users_schema
    )

    result_df = prepare_orders_data(
        orders=orders_df,
        order_details=order_details_df,
        order_status_history=order_status_history_df,
        order_items=order_items_df,
        order_item_pricing=order_item_pricing_df,
        silver_users=silver_users_df,
    )
    rows = {row["ORDER_ID"]: row for row in result_df.collect()}

    assert len(rows) == 1
    row = rows["ORD-100"]

    assert row["TOTAL_AMOUNT"] == pytest.approx(240.0)
    assert row["FREIGHT_VALUE"] == pytest.approx(10.0)
    assert row["SHIPPING_METHOD"] == "EXPRESS"
    assert row["USER_SURROGATE_KEY"] == 99
    assert row["PENDING_PAYMENT_TIMESTAMP"] == "2025-03-10 12:01:00"
    assert row["PAID_TIMESTAMP"] == "2025-03-10 12:05:00"
    assert row["SHIPPED_TIMESTAMP"] == "2025-03-11 08:00:00"
    assert row["DELIVERED_TIMESTAMP"] is None


def test_prepare_orders_data_left_join_missing_details(spark: SparkSession) -> None:
    orders_schema = StructType(
        [
            StructField("order_id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("session_id", StringType(), True),
            StructField("created_at", StringType(), True),
        ]
    )
    details_schema = StructType(
        [
            StructField("order_id", StringType(), True),
            StructField("shipping_method", StringType(), True),
            StructField("shipping_cost", DoubleType(), True),
        ]
    )
    status_schema = StructType(
        [
            StructField("order_id", StringType(), True),
            StructField("current_status", StringType(), True),
            StructField("valid_from", StringType(), True),
        ]
    )
    items_schema = StructType(
        [
            StructField("order_item_id", StringType(), True),
            StructField("order_id", StringType(), True),
            StructField("quantity", IntegerType(), True),
        ]
    )
    pricing_schema = StructType(
        [
            StructField("order_item_id", StringType(), True),
            StructField("unit_price", DoubleType(), True),
            StructField("discount", DoubleType(), True),
        ]
    )
    users_schema = StructType(
        [
            StructField("user_surrogate_key", IntegerType(), True),
            StructField("user_id", StringType(), True),
            StructField("valid_from", StringType(), True),
            StructField("valid_to", StringType(), True),
        ]
    )

    orders_df = spark.createDataFrame(
        [
            ("ORD-1", "USR-1", "SESS-1", "2025-05-01 10:00:00"),
            ("ORD-2", "USR-2", "SESS-2", "2025-05-02 11:00:00"),
        ],
        schema=orders_schema,
    )

    order_details_df = spark.createDataFrame(
        [("ORD-1", "STANDARD", 12.5)], schema=details_schema
    )

    order_status_history_df = spark.createDataFrame(
        [("ORD-1", "PENDING_PAYMENT", "2025-05-01 10:01:00")], schema=status_schema
    )

    order_items_df = spark.createDataFrame(
        [("ITEM-1", "ORD-1", 1)], schema=items_schema
    )

    order_item_pricing_df = spark.createDataFrame(
        [("ITEM-1", 20.0, 0.0)], schema=pricing_schema
    )

    silver_users_df = spark.createDataFrame(
        [
            (1, "USR-1", "2025-01-01 00:00:00", None),
            (2, "USR-2", "2025-01-01 00:00:00", None),
        ],
        schema=users_schema,
    )

    result_df = prepare_orders_data(
        orders=orders_df,
        order_details=order_details_df,
        order_status_history=order_status_history_df,
        order_items=order_items_df,
        order_item_pricing=order_item_pricing_df,
        silver_users=silver_users_df,
    )
    rows = {row["ORDER_ID"]: row for row in result_df.collect()}

    assert len(rows) == 2
    assert rows["ORD-1"]["PENDING_PAYMENT_TIMESTAMP"] == "2025-05-01 10:01:00"
    assert rows["ORD-1"]["FREIGHT_VALUE"] == pytest.approx(12.5)
    assert rows["ORD-1"]["TOTAL_AMOUNT"] == pytest.approx(32.5)

    assert rows["ORD-2"]["USER_SURROGATE_KEY"] == 2
    assert rows["ORD-2"]["SHIPPING_METHOD"] is None
    assert rows["ORD-2"]["FREIGHT_VALUE"] == pytest.approx(0.0)
    assert rows["ORD-2"]["TOTAL_AMOUNT"] == pytest.approx(0.0)
    assert rows["ORD-2"]["PENDING_PAYMENT_TIMESTAMP"] is None


def test_prepare_orders_data_empty_dataframes(spark: SparkSession) -> None:
    orders_schema = StructType(
        [
            StructField("order_id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("session_id", StringType(), True),
            StructField("created_at", StringType(), True),
        ]
    )
    details_schema = StructType(
        [
            StructField("order_id", StringType(), True),
            StructField("shipping_method", StringType(), True),
            StructField("shipping_cost", DoubleType(), True),
        ]
    )
    status_schema = StructType(
        [
            StructField("order_id", StringType(), True),
            StructField("current_status", StringType(), True),
            StructField("valid_from", StringType(), True),
        ]
    )
    items_schema = StructType(
        [
            StructField("order_item_id", StringType(), True),
            StructField("order_id", StringType(), True),
            StructField("quantity", IntegerType(), True),
        ]
    )
    pricing_schema = StructType(
        [
            StructField("order_item_id", StringType(), True),
            StructField("unit_price", DoubleType(), True),
            StructField("discount", DoubleType(), True),
        ]
    )
    users_schema = StructType(
        [
            StructField("user_surrogate_key", IntegerType(), True),
            StructField("user_id", StringType(), True),
            StructField("valid_from", StringType(), True),
            StructField("valid_to", StringType(), True),
        ]
    )

    empty_orders = spark.createDataFrame([], schema=orders_schema)
    empty_details = spark.createDataFrame([], schema=details_schema)
    empty_status = spark.createDataFrame([], schema=status_schema)
    empty_items = spark.createDataFrame([], schema=items_schema)
    empty_pricing = spark.createDataFrame([], schema=pricing_schema)
    empty_users = spark.createDataFrame([], schema=users_schema)

    joined_df = prepare_orders_data(
        orders=empty_orders,
        order_details=empty_details,
        order_status_history=empty_status,
        order_items=empty_items,
        order_item_pricing=empty_pricing,
        silver_users=empty_users,
    )
    result_df = add_date_columns(joined_df)

    assert result_df.count() == 0
    assert "YEAR" in result_df.columns
    assert "MONTH" in result_df.columns
    assert "QUARTER" in result_df.columns
