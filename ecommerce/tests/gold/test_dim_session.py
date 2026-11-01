from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from ecommerce.logic_ecommerce.gold.dim_session import prepare_dim_session_data

SESSIONS_SCHEMA = StructType(
    [
        StructField("SESSION_ID", StringType(), True),
        StructField("SESSION_START", StringType(), True),
        StructField("SESSION_END", StringType(), True),
        StructField("DEVICE_TYPE", StringType(), True),
        StructField("BROWSER", StringType(), True),
        StructField("TRAFFIC_SOURCE", StringType(), True),
    ]
)

EVENTS_SCHEMA = StructType(
    [
        StructField("SESSION_ID", StringType(), True),
        StructField("EVENT_TYPE", StringType(), True),
    ]
)

EXPECTED_COLUMNS = [
    "SESSION_SURROGATE_KEY",
    "SESSION_ID",
    "SESSION_START",
    "SESSION_END",
    "SESSION_DURATION_BUCKET",
    "DEVICE_TYPE",
    "BROWSER",
    "TRAFFIC_SOURCE",
    "CONVERSION_STATUS",
]


def test_prepare_dim_session_schema_and_empty_input(spark: SparkSession) -> None:
    # Verify output schema and empty dataframe handling
    empty_sessions = spark.createDataFrame([], schema=SESSIONS_SCHEMA)
    empty_events = spark.createDataFrame([], schema=EVENTS_SCHEMA)

    result_df = prepare_dim_session_data(
        silver_sessions=empty_sessions, silver_events=empty_events
    )

    assert result_df.columns == EXPECTED_COLUMNS
    assert result_df.count() == 0


def test_prepare_dim_session_duration_buckets_and_conversion(
    spark: SparkSession,
) -> None:
    # Verify session duration bucketing and case-insensitive conversion status detection
    sessions_df = spark.createDataFrame(
        [
            (
                "S1",
                "2025-01-01 10:00:00",
                "2025-01-01 10:00:05",
                "Mobile",
                "Safari",
                "Direct",
            ),
            (
                "S2",
                "2025-01-01 10:00:00",
                "2025-01-01 10:00:30",
                "Desktop",
                "Chrome",
                "Paid",
            ),
            (
                "S3",
                "2025-01-01 10:00:00",
                "2025-01-01 10:02:00",
                "Mobile",
                "Chrome",
                "Direct",
            ),
            (
                "S4",
                "2025-01-01 10:00:00",
                "2025-01-01 10:10:00",
                "Desktop",
                "Firefox",
                "Paid",
            ),
        ],
        schema=SESSIONS_SCHEMA,
    )

    events_df = spark.createDataFrame(
        [
            ("S2", "Purchase"),
            ("S2", "View"),
            ("S3", "View"),
            ("S4", "checkout"),
        ],
        schema=EVENTS_SCHEMA,
    )

    result_df = prepare_dim_session_data(
        silver_sessions=sessions_df, silver_events=events_df
    )
    rows = {r["SESSION_ID"]: r for r in result_df.collect()}

    assert len(rows) == 4
    assert rows["S1"]["SESSION_DURATION_BUCKET"] == "Bounce (< 10s)"
    assert rows["S1"]["CONVERSION_STATUS"] == "Browsing Only"

    assert rows["S2"]["SESSION_DURATION_BUCKET"] == "Brief (10s-60s)"
    assert rows["S2"]["CONVERSION_STATUS"] == "Converted (Purchase)"

    assert rows["S3"]["SESSION_DURATION_BUCKET"] == "Engaged (1m-5m)"
    assert rows["S3"]["CONVERSION_STATUS"] == "Browsing Only"

    assert rows["S4"]["SESSION_DURATION_BUCKET"] == "Extended (> 5m)"
    assert rows["S4"]["CONVERSION_STATUS"] == "Converted (Purchase)"
