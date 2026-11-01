from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from ecommerce.logic_ecommerce.gold.dim_geography import prepare_dim_geography_data

USERS_GEO_SCHEMA = StructType(
    [
        StructField("CITY", StringType(), True),
        StructField("STATE", StringType(), True),
        StructField("ZIP_CODE", StringType(), True),
        StructField("COUNTRY", StringType(), True),
    ]
)

EXPECTED_COLUMNS = [
    "GEOGRAPHY_KEY",
    "ZIP_CODE",
    "CITY",
    "STATE",
    "COUNTRY",
]


def test_prepare_dim_geography_schema_and_empty_input(spark: SparkSession) -> None:
    # Verify output schema and empty dataframe handling
    empty_df = spark.createDataFrame([], schema=USERS_GEO_SCHEMA)

    result_df = prepare_dim_geography_data(silver_users=empty_df)

    assert result_df.columns == EXPECTED_COLUMNS
    assert result_df.count() == 0


def test_prepare_dim_geography_cleaning_and_deduplication(spark: SparkSession) -> None:
    # Verify whitespace trimming, deduplication, null zip fallback, and invalid row filtering
    users_df = spark.createDataFrame(
        [
            ("Warszawa  ", "Mazowieckie", "00-001", "Poland"),
            ("Warszawa", "Mazowieckie", "00-001", " Poland "),
            ("Gdansk", "Pomorskie", None, "Poland"),
            (None, "Pomorskie", "80-000", "Poland"),
            ("Krakow", None, "30-001", "Poland"),
        ],
        schema=USERS_GEO_SCHEMA,
    )

    result_df = prepare_dim_geography_data(silver_users=users_df)
    rows = {row["CITY"]: row for row in result_df.collect()}

    assert len(rows) == 2
    assert rows["Warszawa"]["COUNTRY"] == "Poland"
    assert rows["Warszawa"]["ZIP_CODE"] == "00-001"
    assert rows["Gdansk"]["ZIP_CODE"] == ""
