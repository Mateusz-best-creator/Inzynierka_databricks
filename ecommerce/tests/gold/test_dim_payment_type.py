import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from ecommerce.logic_ecommerce.gold.dim_payment_type import (
    prepare_dim_payment_type_data,
)


def test_prepare_dim_payment_type_tiers_and_deduplication(spark: SparkSession) -> None:
    schema = StructType(
        [
            StructField("PAYMENT_TYPE", StringType(), True),
            StructField("PAYMENT_INSTALLMENTS", IntegerType(), True),
        ]
    )

    # Mix of payment types and installments, including duplicates
    payments_df = spark.createDataFrame(
        [
            ("CREDIT_CARD", 1),
            ("CREDIT_CARD", None),  # Should default to 1 (Single Payment)
            ("CREDIT_CARD", 3),  # 2-3 Installments
            ("CREDIT_CARD", 3),  # Duplicate, should be dropped
            ("BLIK", 6),  # 4+ Installments
        ],
        schema=schema,
    )

    result_df = prepare_dim_payment_type_data(silver_payments=payments_df)
    rows = result_df.collect()

    # Assertions
    assert len(rows) == 3  # Deduplicated to 3 unique combinations
    assert "PAYMENT_TYPE_KEY" in result_df.columns

    # Verify logic tiers
    results_map = {f"{r['PAYMENT_TYPE_NAME']}-{r['INSTALLMENT_TIER']}": r for r in rows}

    assert "CREDIT_CARD-Single Payment" in results_map
    assert "CREDIT_CARD-2-3 Installments" in results_map
    assert "BLIK-4+ Installments" in results_map


def test_prepare_dim_payment_type_empty(spark: SparkSession) -> None:
    schema = StructType(
        [
            StructField("PAYMENT_TYPE", StringType(), True),
            StructField("PAYMENT_INSTALLMENTS", IntegerType(), True),
        ]
    )
    payments_df = spark.createDataFrame([], schema=schema)
    result_df = prepare_dim_payment_type_data(silver_payments=payments_df)
    assert result_df.count() == 0
