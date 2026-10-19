import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from ecommerce.logic_ecommerce.silver.payments import prepare_payments_data


def test_prepare_payments_schema_and_columns(spark: SparkSession) -> None:
    # Arrange verifying output schema
    payment_details_df = spark.createDataFrame(
        [("PAY-1", "ORD-1", 1, "credit_card", 3, 100.0)],
        ["payment_id", "order_id", "payment_sequential", "PAYMENT_TYPE", "payment_installments", "PAYMENT_VALUE"],
    )
    payment_status_history_df = spark.createDataFrame(
        [("PAY-1", "LOG-1", "SUCCESS", "APPROVED", "2025-01-01 12:00:00")],
        ["payment_id", "log_id", "STATUS", "GATEWAY_RESPONSE", "VALID_FROM"],
    )

    # Act
    result_df = prepare_payments_data(
        payment_details=payment_details_df,
        payment_status_history=payment_status_history_df,
    )

    # Assert checking if all column names are uppercase and in the expected order
    expected_columns = [
        "PAYMENT_ID",
        "ORDER_ID",
        "PAYMENT_SEQUENTIAL",
        "PAYMENT_TYPE",
        "PAYMENT_INSTALLMENTS",
        "PAYMENT_VALUE",
        "LOG_ID",
        "PAYMENT_CURRENT_STATUS",
        "GATEWAY_RESPONSE",
        "VALID_FROM",
    ]
    assert result_df.columns == expected_columns


def test_prepare_payments_joins_with_trim_and_case(spark: SparkSession) -> None:
    # Arrange creating payments with different case and spaces in payment_id
    payment_details_df = spark.createDataFrame(
        [
            (" pay-100 ", "ORD-10", 1, "boleto", 1, 50.0),
            ("PAY-101", "ORD-11", 1, "credit_card", 2, 200.0),
        ],
        ["payment_id", "order_id", "payment_sequential", "PAYMENT_TYPE", "payment_installments", "PAYMENT_VALUE"],
    )
    payment_status_history_df = spark.createDataFrame(
        [
            ("PAY-100", "LOG-10", "SUCCESS", "APPROVED", "2025-02-01 10:00:00"),
            (" pay-101 ", "LOG-11", "PENDING", "WAITING", "2025-02-02 11:00:00"),
        ],
        ["payment_id", "log_id", "STATUS", "GATEWAY_RESPONSE", "VALID_FROM"],
    )

    # Act
    result_df = prepare_payments_data(
        payment_details=payment_details_df,
        payment_status_history=payment_status_history_df,
    )
    rows = {row["ORDER_ID"]: row for row in result_df.collect()}

    # Assert
    assert len(rows) == 2

    assert rows["ORD-10"]["PAYMENT_ID"] == " pay-100 "
    assert rows["ORD-10"]["PAYMENT_CURRENT_STATUS"] == "SUCCESS"
    assert rows["ORD-10"]["PAYMENT_VALUE"] == pytest.approx(50.0)

    assert rows["ORD-11"]["PAYMENT_ID"] == "PAY-101"
    assert rows["ORD-11"]["PAYMENT_CURRENT_STATUS"] == "PENDING"
    assert rows["ORD-11"]["PAYMENT_VALUE"] == pytest.approx(200.0)


def test_prepare_payments_left_join_missing_status(spark: SparkSession) -> None:
    # Arrange PAY-2 has no matching record in payment_status_history
    payment_details_df = spark.createDataFrame(
        [
            ("PAY-1", "ORD-1", 1, "credit_card", 1, 150.0),
            ("PAY-2", "ORD-2", 1, "voucher", 1, 20.0),
        ],
        ["payment_id", "order_id", "payment_sequential", "PAYMENT_TYPE", "payment_installments", "PAYMENT_VALUE"],
    )
    payment_status_history_df = spark.createDataFrame(
        [("PAY-1", "LOG-1", "SUCCESS", "APPROVED", "2025-03-01 14:00:00")],
        ["payment_id", "log_id", "STATUS", "GATEWAY_RESPONSE", "VALID_FROM"],
    )

    # Act
    result_df = prepare_payments_data(
        payment_details=payment_details_df,
        payment_status_history=payment_status_history_df,
    )
    rows = {row["PAYMENT_ID"]: row for row in result_df.collect()}

    # Assert left join saves PAY-2 with NULL values for status columns
    assert len(rows) == 2
    assert rows["PAY-1"]["PAYMENT_CURRENT_STATUS"] == "SUCCESS"

    assert rows["PAY-2"]["ORDER_ID"] == "ORD-2"
    assert rows["PAY-2"]["LOG_ID"] is None
    assert rows["PAY-2"]["PAYMENT_CURRENT_STATUS"] is None
    assert rows["PAY-2"]["GATEWAY_RESPONSE"] is None
    assert rows["PAY-2"]["VALID_FROM"] is None


def test_prepare_payments_empty_dataframes(spark: SparkSession) -> None:
    # Arrange define schemas for empty dataframes
    details_schema = StructType(
        [
            StructField("payment_id", StringType(), True),
            StructField("order_id", StringType(), True),
            StructField("payment_sequential", IntegerType(), True),
            StructField("PAYMENT_TYPE", StringType(), True),
            StructField("payment_installments", IntegerType(), True),
            StructField("PAYMENT_VALUE", DoubleType(), True),
        ]
    )
    status_schema = StructType(
        [
            StructField("payment_id", StringType(), True),
            StructField("log_id", StringType(), True),
            StructField("STATUS", StringType(), True),
            StructField("GATEWAY_RESPONSE", StringType(), True),
            StructField("valid_from", StringType(), True),
        ]
    )

    empty_details = spark.createDataFrame([], schema=details_schema)
    empty_status = spark.createDataFrame([], schema=status_schema)

    # Act
    result_df = prepare_payments_data(
        payment_details=empty_details,
        payment_status_history=empty_status,
    )

    # Assert
    assert result_df.count() == 0
    assert "PAYMENT_ID" in result_df.columns
    assert "PAYMENT_CURRENT_STATUS" in result_df.columns