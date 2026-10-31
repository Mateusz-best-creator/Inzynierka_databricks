import datetime
from pyspark.sql import SparkSession

from ecommerce.logic_ecommerce.gold.dim_date import prepare_dim_date_data


def test_prepare_dim_date_schema_and_range(spark: SparkSession) -> None:
    # Verify output columns and row count for a specific date range
    result_df = prepare_dim_date_data(
        spark=spark, start_date="2025-05-01", end_date="2025-05-07"
    )

    expected_columns = [
        "DATE_KEY",
        "FULL_DATE",
        "DAY",
        "MONTH",
        "QUARTER",
        "YEAR",
        "MONTH_NAME",
        "DAY_OF_MONTH",
        "DAY_OF_WEEK",
        "IS_WEEKEND",
    ]
    assert result_df.columns == expected_columns
    assert result_df.count() == 7


def test_prepare_dim_date_iso_weekday_and_weekend_flags(spark: SparkSession) -> None:
    # Verify ISO day of week shift (Monday=1, Sunday=7) and weekend detection
    # 2025-05-05 is Monday, 2025-05-10 is Saturday, 2025-05-11 is Sunday
    result_df = prepare_dim_date_data(
        spark=spark, start_date="2025-05-05", end_date="2025-05-11"
    )
    rows = {row["FULL_DATE"]: row for row in result_df.collect()}

    monday = rows[datetime.date(2025, 5, 5)]
    assert monday["DAY_OF_WEEK"] == 1
    assert monday["IS_WEEKEND"] == 0
    assert monday["QUARTER"] == "Q2"
    assert monday["MONTH_NAME"] == "May"

    saturday = rows[datetime.date(2025, 5, 10)]
    assert saturday["DAY_OF_WEEK"] == 6
    assert saturday["IS_WEEKEND"] == 1

    sunday = rows[datetime.date(2025, 5, 11)]
    assert sunday["DAY_OF_WEEK"] == 7
    assert sunday["IS_WEEKEND"] == 1
