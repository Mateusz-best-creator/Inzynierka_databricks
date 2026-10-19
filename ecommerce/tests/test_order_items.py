import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from ecommerce.logic_ecommerce.silver.order_items import prepare_order_items_data


def test_prepare_order_items_schema_and_columns(spark: SparkSession) -> None:
    # Arrange verifying output schema
    order_items_df = spark.createDataFrame(
        [("ITEM-1", "ORD-1", "PROD-1", 1)],
        ["order_item_id", "order_id", "product_id", "quantity"],
    )
    order_item_pricing_df = spark.createDataFrame(
        [("ITEM-1", 50.0, 5.0)],
        ["order_item_id", "unit_price", "discount"],
    )

    # Act
    result_df = prepare_order_items_data(
        order_items=order_items_df, order_item_pricing=order_item_pricing_df
    )

    # Assert checking if all column names are uppercase and in the expected order
    expected_columns = [
        "ORDER_ITEM_ID",
        "ORDER_ID",
        "PRODUCT_ID",
        "QUANTITY",
        "UNIT_PRICE",
        "DISCOUNT",
        "TOTAL_PRICE",
        "TOTAL_PRICE_AFTER_DISCOUNT",
    ]
    assert result_df.columns == expected_columns


def test_prepare_order_items_price_calculations(spark: SparkSession) -> None:
    # Arrange creating multiple items with standard discount zero discount and single quantity
    order_items_data = [
        ("ITEM-1", "ORD-100", "PROD-A", 3),
        ("ITEM-2", "ORD-100", "PROD-B", 2),
        ("ITEM-3", "ORD-101", "PROD-C", 1),
    ]
    order_items_df = spark.createDataFrame(
        order_items_data,
        ["order_item_id", "order_id", "product_id", "quantity"],
    )

    pricing_data = [
        ("ITEM-1", 100.0, 15.5),
        ("ITEM-2", 45.0, 0.0),
        ("ITEM-3", 19.99, 4.99),
    ]
    order_item_pricing_df = spark.createDataFrame(
        pricing_data,
        ["order_item_id", "unit_price", "discount"],
    )

    # Act
    result_df = prepare_order_items_data(
        order_items=order_items_df, order_item_pricing=order_item_pricing_df
    )
    rows = {row["ORDER_ITEM_ID"]: row for row in result_df.collect()}

    # Assert
    assert len(rows) == 3

    assert rows["ITEM-1"]["ORDER_ID"] == "ORD-100"
    assert rows["ITEM-1"]["PRODUCT_ID"] == "PROD-A"
    assert rows["ITEM-1"]["QUANTITY"] == 3
    assert rows["ITEM-1"]["UNIT_PRICE"] == pytest.approx(100.0)
    assert rows["ITEM-1"]["DISCOUNT"] == pytest.approx(15.5)
    assert rows["ITEM-1"]["TOTAL_PRICE"] == pytest.approx(300.0)
    assert rows["ITEM-1"]["TOTAL_PRICE_AFTER_DISCOUNT"] == pytest.approx(253.5)

    assert rows["ITEM-2"]["TOTAL_PRICE"] == pytest.approx(90.0)
    assert rows["ITEM-2"]["TOTAL_PRICE_AFTER_DISCOUNT"] == pytest.approx(90.0)

    assert rows["ITEM-3"]["TOTAL_PRICE"] == pytest.approx(19.99)
    assert rows["ITEM-3"]["TOTAL_PRICE_AFTER_DISCOUNT"] == pytest.approx(15.0)


def test_prepare_order_items_left_join_missing_pricing(spark: SparkSession) -> None:
    # Arrange ITEM-2 has no matching record in order_item_pricing
    order_items_df = spark.createDataFrame(
        [
            ("ITEM-1", "ORD-1", "PROD-1", 2),
            ("ITEM-2", "ORD-2", "PROD-2", 5),
        ],
        ["order_item_id", "order_id", "product_id", "quantity"],
    )
    order_item_pricing_df = spark.createDataFrame(
        [("ITEM-1", 20.0, 2.0)],
        ["order_item_id", "unit_price", "discount"],
    )

    # Act
    result_df = prepare_order_items_data(
        order_items=order_items_df, order_item_pricing=order_item_pricing_df
    )
    rows = {row["ORDER_ITEM_ID"]: row for row in result_df.collect()}

    # Assert left join save ITEM-2 with NULL values for price columns
    assert len(rows) == 2
    assert rows["ITEM-1"]["TOTAL_PRICE"] == pytest.approx(40.0)
    assert rows["ITEM-1"]["TOTAL_PRICE_AFTER_DISCOUNT"] == pytest.approx(36.0)

    assert rows["ITEM-2"]["QUANTITY"] == 5
    assert rows["ITEM-2"]["UNIT_PRICE"] is None
    assert rows["ITEM-2"]["DISCOUNT"] is None
    assert rows["ITEM-2"]["TOTAL_PRICE"] is None
    assert rows["ITEM-2"]["TOTAL_PRICE_AFTER_DISCOUNT"] is None


def test_prepare_order_items_empty_dataframes(spark: SparkSession) -> None:
    # Arrange define schemas for empty dataframes
    items_schema = StructType(
        [
            StructField("order_item_id", StringType(), True),
            StructField("order_id", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("quantity", IntegerType(), True),
        ]
    )
    pricing_schema = StructType(
        [
            StructField("order_item_id", StringType(), True),
            StructField("unit_price", DoubleType(), True),
            StructField("discount", DoubleType(), True),
        ]
    )

    empty_items_df = spark.createDataFrame([], schema=items_schema)
    empty_pricing_df = spark.createDataFrame([], schema=pricing_schema)

    # Act
    result_df = prepare_order_items_data(
        order_items=empty_items_df, order_item_pricing=empty_pricing_df
    )

    # Assert
    assert result_df.count() == 0
    assert "TOTAL_PRICE" in result_df.columns
    assert "TOTAL_PRICE_AFTER_DISCOUNT" in result_df.columns