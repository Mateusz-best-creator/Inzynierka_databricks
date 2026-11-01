import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from ecommerce.logic_ecommerce.gold.dim_product import prepare_dim_product_data

PRODUCTS_SCHEMA = StructType(
    [
        StructField("PRODUCT_SURROGATE_KEY", IntegerType(), True),
        StructField("PRODUCT_ID", StringType(), True),
        StructField("PRODUCT_NAME", StringType(), True),
        StructField("BRAND", StringType(), True),
        StructField("DESCRIPTION", StringType(), True),
        StructField("BASE_PRICE", DoubleType(), True),
        StructField("COST_PRICE", DoubleType(), True),
        StructField("WEIGHT_G", DoubleType(), True),
        StructField("LENGTH_CM", DoubleType(), True),
        StructField("HEIGHT_CM", DoubleType(), True),
        StructField("WIDTH_CM", DoubleType(), True),
        StructField("STOCK_QUANTITY", IntegerType(), True),
        StructField("CREATED_AT", StringType(), True),
        StructField("VALID_FROM", StringType(), True),
        StructField("VALID_TO", StringType(), True),
        StructField("IS_ACTIVE", IntegerType(), True),
        StructField("CATEGORY_ID", StringType(), True),
    ]
)

CATEGORIES_SCHEMA = StructType(
    [
        StructField("CATEGORY_SURROGATE_KEY", IntegerType(), True),
        StructField("CATEGORY_ID", StringType(), True),
        StructField("CATEGORY_NAME", StringType(), True),
        StructField("PARENT_CATEGORY_ID", StringType(), True),
    ]
)


def test_prepare_dim_product_schema(spark: SparkSession) -> None:
    categories_df = spark.createDataFrame([], schema=CATEGORIES_SCHEMA)
    products_df = spark.createDataFrame([], schema=PRODUCTS_SCHEMA)

    result_df = prepare_dim_product_data(
        silver_products=products_df, silver_categories=categories_df
    )

    expected_columns = [
        "PRODUCT_SURROGATE_KEY",
        "PRODUCT_ID",
        "PRODUCT_NAME",
        "BRAND",
        "DESCRIPTION",
        "BASE_PRICE",
        "COST_PRICE",
        "STOCK_QUANTITY",
        "CATEGORY_ID",
        "CATEGORY_NAME",
        "PARENT_CATEGORY_ID",
        "PARENT_CATEGORY_NAME",
        "PRICE_TIER",
        "MARGIN_TIER",
        "WEIGHT_CLASS",
        "PACKAGE_SIZE_TIER",
        "STOCK_STATUS",
        "PRODUCT_LIFECYCLE_STATUS",
        "CREATED_AT",
        "VALID_FROM",
        "VALID_TO",
        "SCD_RECORD_STATUS",
    ]
    assert result_df.columns == expected_columns


def test_prepare_dim_product_tiers_and_hierarchy(spark: SparkSession) -> None:
    # Verify price, margin, weight, volume, stock, and SCD2 classifications alongside category hierarchy
    categories_df = spark.createDataFrame(
        [
            (1, "CAT-1", "Electronics", None),
            (2, "CAT-2", "Laptops", "CAT-1"),
        ],
        schema=CATEGORIES_SCHEMA,
    )

    products_df = spark.createDataFrame(
        [
            # Budget (<50), Low Margin (<15%), Light (<500g), Small (<1000cm3), Out of Stock (0), Historical
            (
                1,
                "PROD-1",
                "Cable",
                "Baseus",
                "USB",
                20.0,
                18.0,
                100.0,
                5.0,
                2.0,
                5.0,
                0,
                "2025-01-01",
                "2025-01-01",
                "2025-02-01",
                1,
                "CAT-2",
            ),
            # Regular (50-200), Standard Margin (15-35%), Standard weight, Medium volume, Low Stock (1-10), Current
            (
                2,
                "PROD-2",
                "Mouse",
                "Logi",
                "Pro",
                100.0,
                75.0,
                800.0,
                10.0,
                10.0,
                20.0,
                5,
                "2025-01-01",
                "2025-01-01",
                None,
                1,
                "CAT-1",
            ),
            # Premium (200-500), High Margin (>35%), Heavy weight, Large volume, In Stock (11-100), Discontinued
            (
                3,
                "PROD-3",
                "Monitor",
                "Dell",
                "4K",
                400.0,
                200.0,
                8000.0,
                50.0,
                20.0,
                10.0,
                50,
                "2025-01-01",
                "2025-01-01",
                None,
                0,
                "CAT-2",
            ),
            # Luxury (>500), High Margin, Freight weight (>20kg), Oversized (>20000cm3), High Stock (>100)
            (
                4,
                "PROD-4",
                "Server",
                "HP",
                "Rack",
                1000.0,
                500.0,
                25000.0,
                50.0,
                50.0,
                20.0,
                150,
                "2025-01-01",
                "2025-01-01",
                None,
                1,
                "CAT-2",
            ),
        ],
        schema=PRODUCTS_SCHEMA,
    )

    result_df = prepare_dim_product_data(
        silver_products=products_df, silver_categories=categories_df
    )
    rows = {r["PRODUCT_ID"]: r for r in result_df.collect()}

    assert len(rows) == 4

    assert rows["PROD-1"]["CATEGORY_NAME"] == "Laptops"
    assert rows["PROD-1"]["PARENT_CATEGORY_NAME"] == "Electronics"
    assert rows["PROD-1"]["PRICE_TIER"] == "Budget (< 50)"
    assert rows["PROD-1"]["MARGIN_TIER"] == "Low Margin (< 15%)"
    assert rows["PROD-1"]["WEIGHT_CLASS"] == "Light (< 0.5 kg)"
    assert rows["PROD-1"]["PACKAGE_SIZE_TIER"] == "Small (Parcel A)"
    assert rows["PROD-1"]["STOCK_STATUS"] == "Out of Stock (0)"
    assert rows["PROD-1"]["SCD_RECORD_STATUS"] == "Historical"

    assert rows["PROD-2"]["PARENT_CATEGORY_NAME"] == "Root / None"
    assert rows["PROD-2"]["PRICE_TIER"] == "Regular (50-200)"
    assert rows["PROD-2"]["MARGIN_TIER"] == "Standard Margin (15-35%)"
    assert rows["PROD-2"]["WEIGHT_CLASS"] == "Standard (0.5-5 kg)"
    assert rows["PROD-2"]["PACKAGE_SIZE_TIER"] == "Medium (Parcel B)"
    assert rows["PROD-2"]["STOCK_STATUS"] == "Low Stock (1-10)"
    assert rows["PROD-2"]["SCD_RECORD_STATUS"] == "Current"

    assert rows["PROD-3"]["PRICE_TIER"] == "Premium (200-500)"
    assert rows["PROD-3"]["MARGIN_TIER"] == "High Margin (> 35%)"
    assert rows["PROD-3"]["WEIGHT_CLASS"] == "Heavy (5-20 kg)"
    assert rows["PROD-3"]["PACKAGE_SIZE_TIER"] == "Large (Parcel C)"
    assert rows["PROD-3"]["STOCK_STATUS"] == "In Stock (11-100)"
    assert rows["PROD-3"]["PRODUCT_LIFECYCLE_STATUS"] == "Discontinued"

    assert rows["PROD-4"]["PRICE_TIER"] == "Luxury (> 500)"
    assert rows["PROD-4"]["WEIGHT_CLASS"] == "Freight (> 20 kg)"
    assert rows["PROD-4"]["PACKAGE_SIZE_TIER"] == "Oversized"
    assert rows["PROD-4"]["STOCK_STATUS"] == "High Stock (> 100)"
    assert rows["PROD-4"]["PRODUCT_LIFECYCLE_STATUS"] == "Active Catalog"


def test_prepare_dim_product_missing_category_and_zero_price_safety(
    spark: SparkSession,
) -> None:
    # Verify fallback values when category_id is missing or base_price is zero
    categories_df = spark.createDataFrame([], schema=CATEGORIES_SCHEMA)
    products_df = spark.createDataFrame(
        [
            (
                1,
                "PROD-ZERO",
                "Freebie",
                "BrandX",
                "Promo",
                0.0,
                0.0,
                None,
                None,
                None,
                None,
                None,
                "2025-01-01",
                "2025-01-01",
                None,
                1,
                None,
            )
        ],
        schema=PRODUCTS_SCHEMA,
    )

    result_df = prepare_dim_product_data(
        silver_products=products_df, silver_categories=categories_df
    )
    row = result_df.collect()[0]

    assert row["CATEGORY_NAME"] == "Unknown"
    assert row["PARENT_CATEGORY_NAME"] == "Root / None"
    assert row["MARGIN_TIER"] == "Low Margin (< 15%)"
    assert row["WEIGHT_CLASS"] == "Light (< 0.5 kg)"
    assert row["PACKAGE_SIZE_TIER"] == "Small (Parcel A)"
    assert row["STOCK_STATUS"] == "Out of Stock (0)"
