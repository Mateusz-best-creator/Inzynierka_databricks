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


def test_prepare_categories_schema_and_hierarchy(spark: SparkSession) -> None:
    # Verify output columns and parent-child category hierarchy mapping
    categories_df = spark.createDataFrame(
        [
            ("CAT-1", "Electronics", None),
            ("CAT-2", "Laptops", "CAT-1"),
        ],
        schema=CATEGORIES_SCHEMA,
    )

    result_df = prepare_categories_data(categories=categories_df)

    expected_columns = [
        "CATEGORY_SURROGATE_KEY",
        "CATEGORY_ID",
        "CATEGORY_NAME",
        "PARENT_CATEGORY_ID",
    ]
    assert result_df.columns == expected_columns

    rows = {row["CATEGORY_ID"]: row for row in result_df.collect()}
    assert len(rows) == 2
    assert rows["CAT-1"]["PARENT_CATEGORY_ID"] is None
    assert rows["CAT-2"]["PARENT_CATEGORY_ID"] == "CAT-1"


def test_prepare_categories_deduplication_and_cleaning(spark: SparkSession) -> None:
    # Verify trimming, deduplication by category_id, and filtering out null keys
    categories_df = spark.createDataFrame(
        [
            (" CAT-1 ", "Electronics", None),
            ("CAT-1", "Electronics Duplicate", None),
            (None, "Invalid Category", None),
        ],
        schema=CATEGORIES_SCHEMA,
    )

    result_df = prepare_categories_data(categories=categories_df)
    rows = result_df.collect()

    assert len(rows) == 1
    assert rows[0]["CATEGORY_ID"] == "CAT-1"
    assert rows[0]["CATEGORY_SURROGATE_KEY"] == 1


def test_prepare_categories_empty_dataframe(spark: SparkSession) -> None:
    # Verify behavior on empty input dataframe
    empty_df = spark.createDataFrame([], schema=CATEGORIES_SCHEMA)
    result_df = prepare_categories_data(categories=empty_df)

    assert result_df.count() == 0
