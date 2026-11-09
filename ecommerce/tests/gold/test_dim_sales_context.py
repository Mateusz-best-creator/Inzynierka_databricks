from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from ecommerce.logic_ecommerce.gold.dim_sales_context import (
    prepare_dim_sales_context_data,
)

ORDERS_FULL_SCHEMA = StructType(
    [
        StructField("SHIPPING_METHOD", StringType(), True),
        StructField("DISCOUNT_AMOUNT", DoubleType(), True),
        StructField("ACTUAL_DELIVERY_DATE", StringType(), True),
        StructField("ESTIMATED_DELIVERY_DATE", StringType(), True),
    ]
)

ORDERS_MINIMAL_SCHEMA = StructType(
    [
        StructField("SHIPPING_METHOD", StringType(), True),
    ]
)

EXPECTED_COLUMNS = [
    "SALES_CONTEXT_KEY",
    "SHIPPING_METHOD",
    "DISCOUNT_STATUS",
    "DELIVERY_SLA_STATUS",
]


def test_prepare_dim_sales_context_schema_and_empty_input(spark: SparkSession) -> None:
    # Verify output schema and empty dataframe handling
    empty_df = spark.createDataFrame([], schema=ORDERS_FULL_SCHEMA)

    result_df = prepare_dim_sales_context_data(silver_orders=empty_df)

    assert result_df.columns == EXPECTED_COLUMNS
    assert result_df.count() == 0


def test_prepare_dim_sales_context_with_optional_columns(spark: SparkSession) -> None:
    # Verify discount status, delivery SLA evaluation, and duplicate context removal
    orders_df = spark.createDataFrame(
        [
            ("Standard", 0.0, "2025-01-10", "2025-01-12"),
            ("Express", 15.5, "2025-01-13", "2025-01-12"),
            (None, None, None, None),
        ],
        schema=ORDERS_FULL_SCHEMA,
    )

    result_df = prepare_dim_sales_context_data(silver_orders=orders_df)
    rows = result_df.collect()

    assert len(rows) == 2
    res_map = {
        f"{r['SHIPPING_METHOD']}-{r['DISCOUNT_STATUS']}-{r['DELIVERY_SLA_STATUS']}": r
        for r in rows
    }

    assert "Standard-Full Price-On-Time Delivery" in res_map
    assert "Express-Discounted-Delayed Delivery" in res_map


def test_prepare_dim_sales_context_missing_optional_columns(
    spark: SparkSession,
) -> None:
    # Verify fallback to default statuses when optional discount and delivery columns are absent
    orders_df = spark.createDataFrame([("Courier",)], schema=ORDERS_MINIMAL_SCHEMA)

    result_df = prepare_dim_sales_context_data(silver_orders=orders_df)
    row = result_df.collect()[0]

    assert row["SHIPPING_METHOD"] == "Courier"
    assert row["DISCOUNT_STATUS"] == "Full Price"
    assert row["DELIVERY_SLA_STATUS"] == "On-Time Delivery"
