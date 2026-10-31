import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from ecommerce.logic_ecommerce.gold.dim_sales_context import (
    prepare_dim_sales_context_data,
)


def test_prepare_dim_sales_context_with_optional_columns(spark: SparkSession) -> None:
    schema = StructType(
        [
            StructField("SHIPPING_METHOD", StringType(), True),
            StructField("DISCOUNT_AMOUNT", DoubleType(), True),
            StructField("ACTUAL_DELIVERY_DATE", StringType(), True),
            StructField("ESTIMATED_DELIVERY_DATE", StringType(), True),
        ]
    )

    orders_df = spark.createDataFrame(
        [
            ("Standard", 0.0, "2025-01-10", "2025-01-12"),  # Full Price, On-Time
            ("Express", 15.5, "2025-01-13", "2025-01-12"),  # Discounted, Delayed
            (None, None, None, None),  # Full Price (null), On-Time (nulls) -> Standard
        ],
        schema=schema,
    )

    result_df = prepare_dim_sales_context_data(silver_orders=orders_df)
    rows = result_df.collect()

    assert len(rows) == 2
    assert "SALES_CONTEXT_KEY" in result_df.columns

    res_map = {
        f"{r['SHIPPING_METHOD']}-{r['DISCOUNT_STATUS']}-{r['DELIVERY_SLA_STATUS']}": r
        for r in rows
    }

    assert "Standard-Full Price-On-Time Delivery" in res_map
    assert "Express-Discounted-Delayed Delivery" in res_map
    assert "Standard-Full Price-On-Time Delivery" in res_map


def test_prepare_dim_sales_context_missing_optional_columns(
    spark: SparkSession,
) -> None:
    schema = StructType([StructField("SHIPPING_METHOD", StringType(), True)])
    orders_df = spark.createDataFrame([("Courier",)], schema=schema)

    result_df = prepare_dim_sales_context_data(silver_orders=orders_df)
    row = result_df.collect()[0]

    assert row["SHIPPING_METHOD"] == "Courier"
    assert row["DISCOUNT_STATUS"] == "Full Price"
    assert row["DELIVERY_SLA_STATUS"] == "On-Time Delivery"
