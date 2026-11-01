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

EXPECTED_COLUMNS = [
    "PAYMENT_SURROGATE_KEY",
    "PAYMENT_ID",
    "PAYMENT_SEQUENTIAL",
    "PAYMENT_TYPE",
    "PAYMENT_INSTALLMENTS",
    "PAYMENT_VALUE",
    "ORDER_ID",
]


def test_prepare_payments_schema_and_empty_input(spark: SparkSession) -> None:
    # Verify output schema and empty dataframe handling
    empty_df = spark.createDataFrame([], schema=PAYMENTS_SCHEMA)

    result_df = prepare_payments_data(payment_details=empty_df)

    assert result_df.columns == EXPECTED_COLUMNS
    assert result_df.count() == 0


def test_prepare_payments_transformations(spark: SparkSession) -> None:
    # Verify standard payment mapping and surrogate key generation
    payments_df = spark.createDataFrame(
        [
            ("PAY-1", "ORD-1", 1, "CREDIT_CARD", 3, 150.0),
            ("PAY-2", "ORD-1", 2, "BLIK", 1, 50.0),
        ],
        schema=PAYMENTS_SCHEMA,
    )

    result_df = prepare_payments_data(payment_details=payments_df)
    rows = {row["PAYMENT_ID"]: row for row in result_df.collect()}

    assert len(rows) == 2
    assert rows["PAY-1"]["PAYMENT_INSTALLMENTS"] == 3
    assert rows["PAY-1"]["PAYMENT_VALUE"] == pytest.approx(150.0)
    assert rows["PAY-2"]["PAYMENT_SEQUENTIAL"] == 2


def test_prepare_payments_sanitization_and_deduplication(spark: SparkSession) -> None:
    # Verify negative value fix, null handling, sequence deduplication, and installment reset
    payments_df = spark.createDataFrame(
        [
            ("PAY-1", "ORD-1", 1, "VOUCHER", 12, -99.50),
            ("PAY-2", "ORD-1", 1, "BLIK", 1, 50.0),
            ("PAY-3", "ORD-2", 1, None, 1, None),
            (None, "ORD-3", 1, "CREDIT_CARD", 1, 25.0),
        ],
        schema=PAYMENTS_SCHEMA,
    )

    result_df = prepare_payments_data(payment_details=payments_df)
    rows = {row["PAYMENT_ID"]: row for row in result_df.collect()}

    assert len(rows) == 2
    assert rows["PAY-1"]["PAYMENT_VALUE"] == pytest.approx(99.50)
    assert rows["PAY-1"]["PAYMENT_INSTALLMENTS"] == 1
    assert rows["PAY-3"]["PAYMENT_TYPE"] == "UNKNOWN"
    assert rows["PAY-3"]["PAYMENT_VALUE"] == pytest.approx(0.0)
