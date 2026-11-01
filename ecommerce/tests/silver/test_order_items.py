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

ORDER_ITEMS_SCHEMA = StructType(
    [
        StructField("order_item_id", StringType(), True),
        StructField("order_id", StringType(), True),
        StructField("order_item_sequential", IntegerType(), True),
        StructField("product_id", StringType(), True),
        StructField("quantity", IntegerType(), True),
    ]
)

ORDER_PRICING_SCHEMA = StructType(
    [
        StructField("order_item_id", StringType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("discount", DoubleType(), True),
    ]
)

EXPECTED_COLUMNS = [
    "ORDER_ITEM_SURROGATE_KEY",
    "ORDER_ITEM_ID",
    "ITEM_SEQUENCE",
    "QUANTITY",
    "UNIT_PRICE",
    "DISCOUNT",
    "ORDER_ID",
    "PRODUCT_ID",
]


def test_prepare_order_items_schema_and_empty_input(spark: SparkSession) -> None:
    # Verify output schema and empty dataframe handling
    empty_items = spark.createDataFrame([], schema=ORDER_ITEMS_SCHEMA)
    empty_pricing = spark.createDataFrame([], schema=ORDER_PRICING_SCHEMA)

    result_df = prepare_order_items_data(
        order_items=empty_items, order_item_pricing=empty_pricing
    )

    assert result_df.columns == EXPECTED_COLUMNS
    assert result_df.count() == 0


def test_prepare_order_items_mapping_and_pricing_join(spark: SparkSession) -> None:
    # Verify joining order items with pricing data and mapping attributes
    order_items_df = spark.createDataFrame(
        [
            ("ITEM-1", "ORD-100", 1, "PROD-A", 3),
            ("ITEM-2", "ORD-100", 2, "PROD-B", 2),
        ],
        schema=ORDER_ITEMS_SCHEMA,
    )
    order_pricing_df = spark.createDataFrame(
        [
            ("ITEM-1", 100.0, 15.5),
            ("ITEM-2", 45.0, 0.0),
        ],
        schema=ORDER_PRICING_SCHEMA,
    )

    result_df = prepare_order_items_data(
        order_items=order_items_df, order_item_pricing=order_pricing_df
    )
    rows = {row["ORDER_ITEM_ID"]: row for row in result_df.collect()}

    assert len(rows) == 2
    assert rows["ITEM-1"]["ORDER_ID"] == "ORD-100"
    assert rows["ITEM-1"]["ITEM_SEQUENCE"] == 1
    assert rows["ITEM-1"]["QUANTITY"] == 3
    assert rows["ITEM-1"]["UNIT_PRICE"] == pytest.approx(100.0)
    assert rows["ITEM-1"]["DISCOUNT"] == pytest.approx(15.5)
    assert rows["ITEM-2"]["UNIT_PRICE"] == pytest.approx(45.0)


def test_prepare_order_items_sanitization_and_missing_pricing(
    spark: SparkSession,
) -> None:
    # Verify negative value conversion, null product_id filtering, and left join preservation
    order_items_df = spark.createDataFrame(
        [
            ("ITEM-1", "ORD-1", -1, "PROD-1", -2),
            ("ITEM-2", "ORD-1", 2, "PROD-2", 5),
            ("ITEM-3", "ORD-1", 3, None, 1),
        ],
        schema=ORDER_ITEMS_SCHEMA,
    )
    order_pricing_df = spark.createDataFrame(
        [("ITEM-1", -80.0, -10.0)],
        schema=ORDER_PRICING_SCHEMA,
    )

    result_df = prepare_order_items_data(
        order_items=order_items_df, order_item_pricing=order_pricing_df
    )
    rows = {row["ORDER_ITEM_ID"]: row for row in result_df.collect()}

    assert len(rows) == 2
    assert rows["ITEM-1"]["ITEM_SEQUENCE"] == 1
    assert rows["ITEM-1"]["QUANTITY"] == 2
    assert rows["ITEM-1"]["UNIT_PRICE"] == pytest.approx(80.0)
    assert rows["ITEM-1"]["DISCOUNT"] == pytest.approx(10.0)

    assert rows["ITEM-2"]["QUANTITY"] == 5
    assert rows["ITEM-2"]["UNIT_PRICE"] is None
    assert rows["ITEM-2"]["DISCOUNT"] is None
