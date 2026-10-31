import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

from ecommerce.logic_ecommerce.gold.dim_session import prepare_dim_session_data


def test_prepare_dim_session_duration_and_conversion(spark: SparkSession) -> None:
    sessions_schema = StructType(
        [
            StructField("SESSION_ID", StringType(), True),
            StructField("SESSION_START", StringType(), True),
            StructField("SESSION_END", StringType(), True),
            StructField("DEVICE_TYPE", StringType(), True),
            StructField("BROWSER", StringType(), True),
            StructField("TRAFFIC_SOURCE", StringType(), True),
        ]
    )
    events_schema = StructType(
        [
            StructField("SESSION_ID", StringType(), True),
            StructField("EVENT_TYPE", StringType(), True),
        ]
    )

    sessions_df = spark.createDataFrame(
        [
            (
                "S1",
                "2025-01-01 10:00:00",
                "2025-01-01 10:00:05",
                "Mobile",
                "Safari",
                "Direct",
            ),  # 5s -> Bounce
            (
                "S2",
                "2025-01-01 10:00:00",
                "2025-01-01 10:00:30",
                "Desktop",
                "Chrome",
                "Paid",
            ),  # 30s -> Brief, Converted
            (
                "S3",
                "2025-01-01 10:00:00",
                "2025-01-01 10:02:00",
                "Mobile",
                "Chrome",
                "Direct",
            ),  # 120s -> Engaged
            (
                "S4",
                "2025-01-01 10:00:00",
                "2025-01-01 10:10:00",
                "Desktop",
                "Firefox",
                "Paid",
            ),  # 600s -> Extended
        ],
        schema=sessions_schema,
    )

    events_df = spark.createDataFrame(
        [
            ("S2", "Purchase"),
            ("S2", "View"),
            ("S3", "View"),
        ],
        schema=events_schema,
    )

    result_df = prepare_dim_session_data(
        silver_sessions=sessions_df, silver_events=events_df
    )
    rows = {r["SESSION_ID"]: r for r in result_df.collect()}

    assert len(rows) == 4
    assert "SESSION_SURROGATE_KEY" in result_df.columns

    assert rows["S1"]["SESSION_DURATION_BUCKET"] == "Bounce (< 10s)"
    assert rows["S1"]["CONVERSION_STATUS"] == "Browsing Only"

    assert rows["S2"]["SESSION_DURATION_BUCKET"] == "Brief (10s-60s)"
    assert rows["S2"]["CONVERSION_STATUS"] == "Converted (Purchase)"

    assert rows["S3"]["SESSION_DURATION_BUCKET"] == "Engaged (1m-5m)"
    assert rows["S3"]["CONVERSION_STATUS"] == "Browsing Only"

    assert rows["S4"]["SESSION_DURATION_BUCKET"] == "Extended (> 5m)"
