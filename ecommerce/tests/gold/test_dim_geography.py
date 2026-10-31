import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from ecommerce.logic_ecommerce.gold.dim_geography import prepare_dim_geography_data


def test_prepare_dim_geography_cleaning_and_deduplication(spark: SparkSession) -> None:
    schema = StructType(
        [
            StructField("CITY", StringType(), True),
            StructField("STATE", StringType(), True),
            StructField("ZIP_CODE", StringType(), True),
            StructField("COUNTRY", StringType(), True),
        ]
    )

    users_df = spark.createDataFrame(
        [
            ("Warszawa  ", "Mazowieckie", "00-001", "Poland"),
            ("Warszawa", "Mazowieckie", "00-001", " Poland "),  # Duplicate with spaces
            ("Gdansk", "Pomorskie", None, "Poland"),  # Null zip code
            (None, "Pomorskie", "80-000", "Poland"),  # Null city (should drop)
            ("Krakow", None, "30-001", "Poland"),  # Null state (should drop)
        ],
        schema=schema,
    )

    result_df = prepare_dim_geography_data(silver_users=users_df)
    rows = {row["CITY"]: row for row in result_df.collect()}

    # Assertions
    assert len(rows) == 2  # 2 valid distinct locations
    assert "GEOGRAPHY_KEY" in result_df.columns

    # Verify trimming and deduplication
    assert "Warszawa" in rows
    assert rows["Warszawa"]["COUNTRY"] == "Poland"

    # Verify null coalescing for zip code
    assert "Gdansk" in rows
    assert rows["Gdansk"]["ZIP_CODE"] == ""


def test_prepare_dim_geography_empty(spark: SparkSession) -> None:
    schema = StructType(
        [
            StructField("CITY", StringType(), True),
            StructField("STATE", StringType(), True),
            StructField("ZIP_CODE", StringType(), True),
            StructField("COUNTRY", StringType(), True),
        ]
    )
    empty_df = spark.createDataFrame([], schema=schema)
    result_df = prepare_dim_geography_data(silver_users=empty_df)
    assert result_df.count() == 0
