import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
)

from ecommerce.logic_ecommerce.silver.orders import (
    add_date_columns,
    prepare_orders_data,
)


def test_prepare_orders_data_schema_and_columns(spark: SparkSession) -> None:
    # Arrange verifying output schema
    orders_df = spark.createDataFrame(
        [("ORD-1", "USR-1", "SESS-1", "2025-01-15 10:00:00")],
        ["order_id", "user_id", "session_id", "created_at"],
    )
    order_details_df = spark.createDataFrame(
        [("ORD-1", "WEB", "COURIER", 15.0)],
        ["order_id", "order_channel", "shipping_method", "shipping_cost"],
    )
    order_status_history_df = spark.createDataFrame(
        [("ORD-1", "DELIVERED", "Order delivered", "2025-01-18 14:00:00")],
        ["order_id", "status", "STATUS_CHANGE_DESCRIPTION", "valid_from"],
    )

    # Act
    result_df = prepare_orders_data(
        order_details=order_details_df,
        order_status_history=order_status_history_df,
        orders=orders_df,
    )

    # Assert checking if all column names are uppercase and in the expected order
    expected_columns = [
        "ORDER_ID",
        "USER_ID",
        "SESSION_ID",
        "CREATED_AT",
        "ORDER_CHANNEL",
        "SHIPPING_METHOD",
        "SHIPPING_COST",
        "STATUS",
        "STATUS_CHANGE_DESCRIPTION",
        "VALID_FROM",
    ]
    assert result_df.columns == expected_columns


def test_prepare_orders_data_joins_with_trim_and_case(spark: SparkSession) -> None:
    # Arrange creating orders with different case and spaces in order_id
    orders_df = spark.createDataFrame(
        [
            (" ord-100 ", "USR-10", "SESS-10", "2025-03-10 12:00:00"),
            ("ORD-101", "USR-11", "SESS-11", "2025-03-11 15:30:00"),
        ],
        ["order_id", "user_id", "session_id", "created_at"],
    )
    order_details_df = spark.createDataFrame(
        [
            ("ORD-100", "MOBILE", "LOCKER", 9.99),
            (" ord-101", "WEB", "EXPRESS", 25.0),
        ],
        ["order_id", "order_channel", "shipping_method", "shipping_cost"],
    )
    order_status_history_df = spark.createDataFrame(
        [
            ("ord-100", "PAID", "Payment confirmed", "2025-03-10 12:05:00"),
            ("ORD-101 ", "SHIPPED", "Sent to customer", "2025-03-12 08:00:00"),
        ],
        ["order_id", "status", "STATUS_CHANGE_DESCRIPTION", "valid_from"],
    )

    # Act
    result_df = prepare_orders_data(
        order_details=order_details_df,
        order_status_history=order_status_history_df,
        orders=orders_df,
    )
    rows = {row["USER_ID"]: row for row in result_df.collect()}

    # Assert
    assert len(rows) == 2

    assert rows["USR-10"]["ORDER_CHANNEL"] == "MOBILE"
    assert rows["USR-10"]["SHIPPING_METHOD"] == "LOCKER"
    assert rows["USR-10"]["SHIPPING_COST"] == pytest.approx(9.99)
    assert rows["USR-10"]["STATUS"] == "PAID"

    assert rows["USR-11"]["ORDER_CHANNEL"] == "WEB"
    assert rows["USR-11"]["SHIPPING_COST"] == pytest.approx(25.0)
    assert rows["USR-11"]["STATUS"] == "SHIPPED"


def test_prepare_orders_data_left_join_missing_details(spark: SparkSession) -> None:
    # Arrange ORD-2 has no matching records in details and status history
    orders_df = spark.createDataFrame(
        [
            ("ORD-1", "USR-1", "SESS-1", "2025-05-01 10:00:00"),
            ("ORD-2", "USR-2", "SESS-2", "2025-05-02 11:00:00"),
        ],
        ["order_id", "user_id", "session_id", "created_at"],
    )
    order_details_df = spark.createDataFrame(
        [("ORD-1", "WEB", "STANDARD", 12.5)],
        ["order_id", "order_channel", "shipping_method", "shipping_cost"],
    )
    order_status_history_df = spark.createDataFrame(
        [("ORD-1", "CREATED", "New order", "2025-05-01 10:01:00")],
        ["order_id", "status", "STATUS_CHANGE_DESCRIPTION", "valid_from"],
    )

    # Act
    result_df = prepare_orders_data(
        order_details=order_details_df,
        order_status_history=order_status_history_df,
        orders=orders_df,
    )
    rows = {row["ORDER_ID"]: row for row in result_df.collect()}

    # Assert left join save ORD-2 with NULL values for missing columns
    assert len(rows) == 2
    assert rows["ORD-1"]["STATUS"] == "CREATED"
    assert rows["ORD-1"]["SHIPPING_COST"] == pytest.approx(12.5)

    assert rows["ORD-2"]["USER_ID"] == "USR-2"
    assert rows["ORD-2"]["ORDER_CHANNEL"] is None
    assert rows["ORD-2"]["SHIPPING_COST"] is None
    assert rows["ORD-2"]["STATUS"] is None


def test_add_date_columns_all_quarters_and_invalid_date(spark: SparkSession) -> None:
    # Arrange dates for all quarters and one invalid date
    input_df = spark.createDataFrame(
        [
            ("ORD-1", "2024-02-15 10:00:00"),
            ("ORD-2", "2024-05-20 12:30:00"),
            ("ORD-3", "2025-08-05 08:15:00"),
            ("ORD-4", "2025-11-30 22:00:00"),
            ("ORD-5", None),
        ],
        ["ORDER_ID", "CREATED_AT"],
    )

    # Act
    result_df = add_date_columns(input_df)
    rows = {row["ORDER_ID"]: row for row in result_df.collect()}

    # Assert checking year month formatting and quarter mapping
    assert rows["ORD-1"]["YEAR"] == 2024
    assert rows["ORD-1"]["MONTH"] == "02"
    assert rows["ORD-1"]["QUARTER"] == "Q1"

    assert rows["ORD-2"]["YEAR"] == 2024
    assert rows["ORD-2"]["MONTH"] == "05"
    assert rows["ORD-2"]["QUARTER"] == "Q2"

    assert rows["ORD-3"]["YEAR"] == 2025
    assert rows["ORD-3"]["MONTH"] == "08"
    assert rows["ORD-3"]["QUARTER"] == "Q3"

    assert rows["ORD-4"]["YEAR"] == 2025
    assert rows["ORD-4"]["MONTH"] == "11"
    assert rows["ORD-4"]["QUARTER"] == "Q4"

    assert rows["ORD-5"]["YEAR"] is None
    assert rows["ORD-5"]["QUARTER"] == "INCORRECT_DATE"


def test_prepare_orders_data_empty_dataframes(spark: SparkSession) -> None:
    # Arrange define schemas for empty dataframes
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
            StructField("order_channel", StringType(), True),
            StructField("shipping_method", StringType(), True),
            StructField("shipping_cost", DoubleType(), True),
        ]
    )
    status_schema = StructType(
        [
            StructField("order_id", StringType(), True),
            StructField("status", StringType(), True),
            StructField("STATUS_CHANGE_DESCRIPTION", StringType(), True),
            StructField("valid_from", StringType(), True),
        ]
    )

    empty_orders = spark.createDataFrame([], schema=orders_schema)
    empty_details = spark.createDataFrame([], schema=details_schema)
    empty_status = spark.createDataFrame([], schema=status_schema)

    # Act
    joined_df = prepare_orders_data(
        order_details=empty_details,
        order_status_history=empty_status,
        orders=empty_orders,
    )
    result_df = add_date_columns(joined_df)

    # Assert
    assert result_df.count() == 0
    assert "YEAR" in result_df.columns
    assert "MONTH" in result_df.columns
    assert "QUARTER" in result_df.columns