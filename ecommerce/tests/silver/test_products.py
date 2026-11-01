import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from ecommerce.logic_ecommerce.silver.products import prepare_products_data

CATALOG_SCHEMA = StructType(
    [
        StructField("product_id", StringType(), True),
        StructField("category_id", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("brand", StringType(), True),
        StructField("description", StringType(), True),
        StructField("is_active", IntegerType(), True),
        StructField("valid_from", StringType(), True),
    ]
)

PRICING_SCHEMA = StructType(
    [
        StructField("unique_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("cost_price", DoubleType(), True),
        StructField("valid_from", StringType(), True),
        StructField("valid_to", StringType(), True),
        StructField("is_current", IntegerType(), True),
    ]
)

INVENTORY_SCHEMA = StructType(
    [
        StructField("inventory_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("weight_g", DoubleType(), True),
        StructField("length_cm", DoubleType(), True),
        StructField("height_cm", DoubleType(), True),
        StructField("width_cm", DoubleType(), True),
        StructField("stock_quantity", IntegerType(), True),
        StructField("valid_from", StringType(), True),
        StructField("valid_to", StringType(), True),
        StructField("is_current", IntegerType(), True),
    ]
)


def test_prepare_products_schema_and_time_slicing(spark: SparkSession) -> None:
    # Verify SCD2 time-slicing across overlapping pricing and inventory changes
    catalog_df = spark.createDataFrame(
        [("PROD-1", "CAT-1", "Mouse", "Logi", "Wireless", 1, "2025-01-01 00:00:00")],
        schema=CATALOG_SCHEMA,
    )
    pricing_df = spark.createDataFrame(
        [
            (
                "P-1",
                "PROD-1",
                100.0,
                60.0,
                "2025-01-01 00:00:00",
                "2025-03-01 00:00:00",
                0,
            ),
            ("P-2", "PROD-1", 120.0, 60.0, "2025-03-01 00:00:00", None, 1),
        ],
        schema=PRICING_SCHEMA,
    )
    inventory_df = spark.createDataFrame(
        [
            (
                "I-1",
                "PROD-1",
                100.0,
                10.0,
                5.0,
                5.0,
                50,
                "2025-01-01 00:00:00",
                "2025-02-01 00:00:00",
                0,
            ),
            (
                "I-2",
                "PROD-1",
                100.0,
                10.0,
                5.0,
                5.0,
                30,
                "2025-02-01 00:00:00",
                None,
                1,
            ),
        ],
        schema=INVENTORY_SCHEMA,
    )

    result_df = prepare_products_data(
        product_catalog=catalog_df,
        product_pricing_history=pricing_df,
        product_inventory=inventory_df,
    )

    expected_columns = [
        "PRODUCT_SURROGATE_KEY",
        "PRODUCT_ID",
        "PRODUCT_NAME",
        "BRAND",
        "DESCRIPTION",
        "BASE_PRICE",
        "COST_PRICE",
        "WEIGHT_G",
        "LENGTH_CM",
        "HEIGHT_CM",
        "WIDTH_CM",
        "STOCK_QUANTITY",
        "CREATED_AT",
        "VALID_FROM",
        "VALID_TO",
        "IS_ACTIVE",
        "CATEGORY_ID",
    ]
    assert result_df.columns == expected_columns

    rows = sorted(result_df.collect(), key=lambda r: r["VALID_FROM"])
    assert len(rows) == 3

    # Window 1: Jan 1 to Feb 1 (Price 100, Stock 50)
    assert rows[0]["VALID_FROM"] == "2025-01-01 00:00:00"
    assert rows[0]["VALID_TO"] == "2025-02-01 00:00:00"
    assert rows[0]["BASE_PRICE"] == pytest.approx(100.0)
    assert rows[0]["STOCK_QUANTITY"] == 50

    # Window 2: Feb 1 to Mar 1 (Price 100, Stock 30)
    assert rows[1]["VALID_FROM"] == "2025-02-01 00:00:00"
    assert rows[1]["VALID_TO"] == "2025-03-01 00:00:00"
    assert rows[1]["BASE_PRICE"] == pytest.approx(100.0)
    assert rows[1]["STOCK_QUANTITY"] == 30

    # Window 3: Mar 1 to None (Price 120, Stock 30)
    assert rows[2]["VALID_FROM"] == "2025-03-01 00:00:00"
    assert rows[2]["VALID_TO"] is None
    assert rows[2]["BASE_PRICE"] == pytest.approx(120.0)
    assert rows[2]["STOCK_QUANTITY"] == 30


def test_prepare_products_error_sanitization(spark: SparkSession) -> None:
    # Verify negative prices/dimensions/stock are converted to positive and empty name replaced
    catalog_df = spark.createDataFrame(
        [("PROD-2", "CAT-1", "", "Keychron", "Mech", 1, "2025-01-01 00:00:00")],
        schema=CATALOG_SCHEMA,
    )
    pricing_df = spark.createDataFrame(
        [("P-1", "PROD-2", -250.0, -100.0, "2025-01-01 00:00:00", None, 1)],
        schema=PRICING_SCHEMA,
    )
    inventory_df = spark.createDataFrame(
        [
            (
                "I-1",
                "PROD-2",
                -800.0,
                -30.0,
                -4.0,
                -12.0,
                -15,
                "2025-01-01 00:00:00",
                None,
                1,
            )
        ],
        schema=INVENTORY_SCHEMA,
    )

    result_df = prepare_products_data(
        product_catalog=catalog_df,
        product_pricing_history=pricing_df,
        product_inventory=inventory_df,
    )
    row = result_df.collect()[0]

    assert row["PRODUCT_NAME"] == "Unknown Product"
    assert row["BASE_PRICE"] == pytest.approx(250.0)
    assert row["COST_PRICE"] == pytest.approx(100.0)
    assert row["WEIGHT_G"] == pytest.approx(800.0)
    assert row["LENGTH_CM"] == pytest.approx(30.0)
    assert row["STOCK_QUANTITY"] == 15


def test_prepare_products_scd2_gap_healing(spark: SparkSession) -> None:
    # Verify 2-day gap in pricing valid_to is healed seamlessly without zero-price slices
    catalog_df = spark.createDataFrame(
        [("PROD-3", "CAT-1", "Monitor", "Dell", "4K", 1, "2025-01-01 00:00:00")],
        schema=CATALOG_SCHEMA,
    )
    pricing_df = spark.createDataFrame(
        [
            (
                "P-1",
                "PROD-3",
                300.0,
                200.0,
                "2025-01-01 00:00:00",
                "2025-01-28 00:00:00",
                0,
            ),
            ("P-2", "PROD-3", 350.0, 200.0, "2025-01-30 00:00:00", None, 1),
        ],
        schema=PRICING_SCHEMA,
    )
    inventory_df = spark.createDataFrame([], schema=INVENTORY_SCHEMA)

    result_df = prepare_products_data(
        product_catalog=catalog_df,
        product_pricing_history=pricing_df,
        product_inventory=inventory_df,
    )
    rows = sorted(result_df.collect(), key=lambda r: r["VALID_FROM"])

    assert len(rows) == 2
    assert rows[0]["VALID_TO"] == "2025-01-30 00:00:00"
    assert rows[0]["BASE_PRICE"] == pytest.approx(300.0)
    assert rows[1]["BASE_PRICE"] == pytest.approx(350.0)


def test_prepare_products_empty_dataframes(spark: SparkSession) -> None:
    # Verify behavior on empty input dataframes
    result_df = prepare_products_data(
        product_catalog=spark.createDataFrame([], schema=CATALOG_SCHEMA),
        product_pricing_history=spark.createDataFrame([], schema=PRICING_SCHEMA),
        product_inventory=spark.createDataFrame([], schema=INVENTORY_SCHEMA),
    )

    assert result_df.count() == 0
