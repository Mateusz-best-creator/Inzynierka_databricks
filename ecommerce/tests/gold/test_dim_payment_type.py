from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from ecommerce.logic_ecommerce.gold.dim_payment_type import (
    prepare_dim_payment_type_data,
)

PAYMENTS_SCHEMA = StructType(
    [
        StructField("PAYMENT_TYPE", StringType(), True),
        StructField("PAYMENT_INSTALLMENTS", IntegerType(), True),
    ]
)

EXPECTED_COLUMNS = [
    "PAYMENT_TYPE_KEY",
    "PAYMENT_TYPE_NAME",
    "INSTALLMENT_TIER",
]


def test_prepare_dim_payment_type_schema_and_empty_input(spark: SparkSession) -> None:
    # Verify output schema and empty dataframe handling
    empty_df = spark.createDataFrame([], schema=PAYMENTS_SCHEMA)

    result_df = prepare_dim_payment_type_data(silver_payments=empty_df)

    assert result_df.columns == EXPECTED_COLUMNS
    assert result_df.count() == 0


def test_prepare_dim_payment_type_tiers_and_deduplication(spark: SparkSession) -> None:
    # Verify installment tier bucketing, null installment fallback, and deduplication
    payments_df = spark.createDataFrame(
        [
            ("CREDIT_CARD", 1),
            ("CREDIT_CARD", None),
            ("CREDIT_CARD", 3),
            ("CREDIT_CARD", 3),
            ("BLIK", 6),
        ],
        schema=PAYMENTS_SCHEMA,
    )

    result_df = prepare_dim_payment_type_data(silver_payments=payments_df)
    rows = result_df.collect()

    assert len(rows) == 3
    results_map = {f"{r['PAYMENT_TYPE_NAME']}-{r['INSTALLMENT_TIER']}": r for r in rows}

    assert "CREDIT_CARD-Single Payment" in results_map
    assert "CREDIT_CARD-2-3 Installments" in results_map
    assert "BLIK-4+ Installments" in results_map
