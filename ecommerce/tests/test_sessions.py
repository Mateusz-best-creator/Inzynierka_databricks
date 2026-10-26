import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from ecommerce.logic_ecommerce.silver.sessions import prepare_sessions_data


def test_prepare_sessions_schema_and_columns(spark: SparkSession) -> None:
    # Arrange verifying output schema and required inputs
    sessions_df = spark.createDataFrame(
        [
            (
                "SESS-1",
                "USR-1",
                "2025-01-15 10:00:00",
                "2025-01-15 10:30:00",
                "WEB",
                "MOBILE",
                "Chrome",
                "Organic",
            )
        ],
        [
            "session_id",
            "user_id",
            "session_start",
            "session_end",
            "channel",
            "device_type",
            "browser",
            "traffic_source",
        ],
    )
    users_schema = StructType(
        [
            StructField("user_surrogate_key", IntegerType(), True),
            StructField("user_id", StringType(), True),
            StructField("valid_from", StringType(), True),
            StructField("valid_to", StringType(), True),
        ]
    )
    silver_users_df = spark.createDataFrame(
        [(1, "USR-1", "2025-01-01 00:00:00", None)],
        schema=users_schema,
    )

    # Act
    result_df = prepare_sessions_data(
        sessions=sessions_df,
        silver_users=silver_users_df,
    )

    # Assert checking uppercase columns and expected order
    expected_columns = [
        "SESSION_SURROGATE_KEY",
        "SESSION_ID",
        "SESSION_START",
        "SESSION_END",
        "DEVICE_TYPE",
        "BROWSER",
        "TRAFFIC_SOURCE",
        "USER_SURROGATE_KEY",
    ]
    assert result_df.columns == expected_columns


def test_prepare_sessions_transformations_and_user_lookup(spark: SparkSession) -> None:
    # Arrange multiple sessions with valid user surrogate key lookup
    sessions_df = spark.createDataFrame(
        [
            (
                "SESS-10",
                "USR-10",
                "2025-03-10 12:00:00",
                "2025-03-10 12:15:00",
                "MOBILE",
                "iOS",
                "Safari",
                "Paid",
            ),
            (
                "SESS-11",
                "USR-11",
                "2025-03-11 14:00:00",
                "2025-03-11 14:45:00",
                "WEB",
                "Desktop",
                "Firefox",
                "Organic",
            ),
        ],
        [
            "session_id",
            "user_id",
            "session_start",
            "session_end",
            "channel",
            "device_type",
            "browser",
            "traffic_source",
        ],
    )
    users_schema = StructType(
        [
            StructField("user_surrogate_key", IntegerType(), True),
            StructField("user_id", StringType(), True),
            StructField("valid_from", StringType(), True),
            StructField("valid_to", StringType(), True),
        ]
    )
    silver_users_df = spark.createDataFrame(
        [
            (100, "USR-10", "2025-01-01 00:00:00", None),
            (101, "USR-11", "2025-01-01 00:00:00", None),
        ],
        schema=users_schema,
    )

    # Act
    result_df = prepare_sessions_data(
        sessions=sessions_df,
        silver_users=silver_users_df,
    )
    rows = {row["SESSION_ID"]: row for row in result_df.collect()}

    # Assert
    assert len(rows) == 2
    assert rows["SESS-10"]["USER_SURROGATE_KEY"] == 100
    assert rows["SESS-10"]["DEVICE_TYPE"] == "iOS"
    assert rows["SESS-11"]["USER_SURROGATE_KEY"] == 101
    assert rows["SESS-11"]["DEVICE_TYPE"] == "Desktop"


def test_prepare_sessions_missing_user_mapping(spark: SparkSession) -> None:
    # Arrange a session where user_id does not exist in silver_users
    sessions_df = spark.createDataFrame(
        [
            (
                "SESS-99",
                "UNKNOWN-USR",
                "2025-04-01 08:00:00",
                "2025-04-01 08:10:00",
                "WEB",
                "MOBILE",
                "Chrome",
                "Organic",
            )
        ],
        [
            "session_id",
            "user_id",
            "session_start",
            "session_end",
            "channel",
            "device_type",
            "browser",
            "traffic_source",
        ],
    )
    users_schema = StructType(
        [
            StructField("user_surrogate_key", IntegerType(), True),
            StructField("user_id", StringType(), True),
            StructField("valid_from", StringType(), True),
            StructField("valid_to", StringType(), True),
        ]
    )
    silver_users_df = spark.createDataFrame([], schema=users_schema)

    # Act
    result_df = prepare_sessions_data(
        sessions=sessions_df,
        silver_users=silver_users_df,
    )
    row = result_df.collect()[0]

    # Assert left join keeps the session with null surrogate key
    assert row["SESSION_ID"] == "SESS-99"
    assert row["USER_SURROGATE_KEY"] is None
