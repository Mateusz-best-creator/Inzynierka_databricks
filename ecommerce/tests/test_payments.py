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

PAYMENTS_SCHEMA = StructType(
    [
        StructField("payment_id", StringType(), True),
        StructField("order_id", StringType(), True),
        StructField("payment_sequential", IntegerType(), True),
        StructField("payment_type", StringType(), True),
        StructField("payment_installments", IntegerType(), True),
        StructField("payment_value", DoubleType(), True),
    ]
)


def test_prepare_payments_schema_and_columns(spark: SparkSession) -> None:
    # Verify output schema and column names
    payments_df = spark.createDataFrame(
        [("PAY-1", "ORD-1", 1, "CREDIT_CARD", 3, 150.0)],
        schema=PAYMENTS_SCHEMA,
    )

    result_df = prepare_payments_data(payment_details=payments_df)

    expected_columns = [
        "PAYMENT_SURROGATE_KEY",
        "PAYMENT_ID",
        "PAYMENT_SEQUENTIAL",
        "PAYMENT_TYPE",
        "PAYMENT_INSTALLMENTS",
        "PAYMENT_VALUE",
        "ORDER_ID",
    ]
    assert result_df.columns == expected_columns


def test_prepare_payments_value_sanitization_and_null_filtering(
    spark: SparkSession,
) -> None:
    # Verify negative values are converted to positive, nulls to 0.0, and null IDs dropped
    payments_df = spark.createDataFrame(
        [
            ("PAY-1", "ORD-1", 1, "CREDIT_CARD", 1, -99.50),
            ("PAY-2", "ORD-2", 1, "BLIK", 1, None),
            (None, "ORD-3", 1, "VOUCHER", 1, 50.0),
        ],
        schema=PAYMENTS_SCHEMA,
    )

    result_df = prepare_payments_data(payment_details=payments_df)
    rows = {row["PAYMENT_ID"]: row for row in result_df.collect()}

    assert len(rows) == 2
    assert rows["PAY-1"]["PAYMENT_VALUE"] == pytest.approx(99.50)
    assert rows["PAY-2"]["PAYMENT_VALUE"] == pytest.approx(0.0)


def test_prepare_payments_empty_dataframe(spark: SparkSession) -> None:
    # Verify behavior on empty input dataframe
    empty_df = spark.createDataFrame([], schema=PAYMENTS_SCHEMA)
    result_df = prepare_payments_data(payment_details=empty_df)

    assert result_df.count() == 0
