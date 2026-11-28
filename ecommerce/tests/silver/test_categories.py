from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from ecommerce.logic_ecommerce.silver.categories import prepare_categories_data

CATEGORIES_SCHEMA = StructType(
    [
        StructField("category_id", StringType(), True),
        StructField("category_name", StringType(), True),
        StructField("parent_category_id", StringType(), True),
    ]
)

EXPECTED_COLUMNS = [
    "CATEGORY_SURROGATE_KEY",
    "CATEGORY_ID",
    "CATEGORY_NAME",
    "PARENT_CATEGORY_ID",
]


def test_prepare_categories_schema_and_empty_input(spark: SparkSession) -> None:
    """Checks silver categories output schema and empty input handling.
    Args:
        spark: Active SparkSession fixture.
    """
    empty_df = spark.createDataFrame([], schema=CATEGORIES_SCHEMA)

    result_df = prepare_categories_data(categories=empty_df)

    assert result_df.columns == EXPECTED_COLUMNS
    assert result_df.count() == 0


def test_prepare_categories_hierarchy_mapping(spark: SparkSession) -> None:
    """Checks parent-child category hierarchy resolution for valid links.
    Args:
        spark: Active SparkSession fixture.
    """
    categories_df = spark.createDataFrame(
        [
            ("CAT-1", "Electronics", None),
            ("CAT-2", "Laptops", "CAT-1"),
        ],
        schema=CATEGORIES_SCHEMA,
    )

    result_df = prepare_categories_data(categories=categories_df)
    rows = {row["CATEGORY_ID"]: row for row in result_df.collect()}

    assert len(rows) == 2
    assert rows["CAT-1"]["PARENT_CATEGORY_ID"] is None
    assert rows["CAT-2"]["PARENT_CATEGORY_ID"] == "CAT-1"


def test_prepare_categories_sanitization_and_deduplication(
    spark: SparkSession,
) -> None:
    """Checks ID trimming, deduplication, null key filtering, and orphan parent removal.
    Args:
        spark: Active SparkSession fixture.
    """
    categories_df = spark.createDataFrame(
        [
            (" CAT-1 ", "Electronics", None),
            ("CAT-1", "Electronics Duplicate", None),
            ("CAT-2", "Orphan Category", "CAT-99999"),
            (None, "Invalid Category", None),
        ],
        schema=CATEGORIES_SCHEMA,
    )

    result_df = prepare_categories_data(categories=categories_df)
    rows = {row["CATEGORY_ID"]: row for row in result_df.collect()}

    assert len(rows) == 2
    assert rows["CAT-1"]["CATEGORY_NAME"] == "Electronics"
    assert rows["CAT-2"]["PARENT_CATEGORY_ID"] is None
