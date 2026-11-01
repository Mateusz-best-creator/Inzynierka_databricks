from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from ecommerce.logic_ecommerce.gold.dim_time import prepare_dim_time_data

EXPECTED_COLUMNS = [
    "TIME_KEY",
    "HOUR",
    "MINUTE",
    "SECONDS",
    "DAY_PART",
]


def test_prepare_dim_time_schema_and_total_seconds(spark: SparkSession) -> None:
    # Verify output schema and full 24-hour grid generation (86,400 seconds)
    result_df = prepare_dim_time_data(spark=spark)

    assert result_df.columns == EXPECTED_COLUMNS
    assert result_df.count() == 86400


def test_prepare_dim_time_day_part_mapping(spark: SparkSession) -> None:
    # Verify time-of-day bucket classification across boundary hours
    result_df = prepare_dim_time_data(spark=spark)

    sample_hours = result_df.filter(
        (col("MINUTE") == 0) & (col("SECONDS") == 0) & col("HOUR").isin([3, 8, 14, 19])
    ).collect()
    rows_by_hour = {row["HOUR"]: row for row in sample_hours}

    assert rows_by_hour[3]["DAY_PART"] == "Night"
    assert rows_by_hour[8]["DAY_PART"] == "Morning"
    assert rows_by_hour[14]["DAY_PART"] == "Afternoon"
    assert rows_by_hour[19]["DAY_PART"] == "Evening"
    assert rows_by_hour[3]["TIME_KEY"] < rows_by_hour[8]["TIME_KEY"]
