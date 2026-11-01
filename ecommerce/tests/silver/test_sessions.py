from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from ecommerce.logic_ecommerce.silver.sessions import prepare_sessions_data

SESSIONS_SCHEMA = StructType(
    [
        StructField("session_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("session_start", StringType(), True),
        StructField("session_end", StringType(), True),
        StructField("channel", StringType(), True),
        StructField("device_type", StringType(), True),
        StructField("browser", StringType(), True),
        StructField("traffic_source", StringType(), True),
    ]
)

SILVER_USERS_SCHEMA = StructType(
    [
        StructField("user_surrogate_key", IntegerType(), True),
        StructField("user_id", StringType(), True),
        StructField("valid_from", StringType(), True),
        StructField("valid_to", StringType(), True),
    ]
)

EXPECTED_COLUMNS = [
    "SESSION_SURROGATE_KEY",
    "SESSION_ID",
    "SESSION_START",
    "SESSION_END",
    "DEVICE_TYPE",
    "BROWSER",
    "TRAFFIC_SOURCE",
    "USER_SURROGATE_KEY",
]


def test_prepare_sessions_schema_and_empty_input(spark: SparkSession) -> None:
    # Verify output schema and empty dataframe handling
    empty_sessions = spark.createDataFrame([], schema=SESSIONS_SCHEMA)
    empty_users = spark.createDataFrame([], schema=SILVER_USERS_SCHEMA)

    result_df = prepare_sessions_data(sessions=empty_sessions, silver_users=empty_users)

    assert result_df.columns == EXPECTED_COLUMNS
    assert result_df.count() == 0


def test_prepare_sessions_user_scd2_lookup(spark: SparkSession) -> None:
    # Verify session mapping and temporal user surrogate key resolution
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
        schema=SESSIONS_SCHEMA,
    )
    silver_users_df = spark.createDataFrame(
        [
            (100, "USR-10", "2025-01-01 00:00:00", None),
            (101, "USR-11", "2025-01-01 00:00:00", None),
        ],
        schema=SILVER_USERS_SCHEMA,
    )

    result_df = prepare_sessions_data(
        sessions=sessions_df, silver_users=silver_users_df
    )
    rows = {row["SESSION_ID"]: row for row in result_df.collect()}

    assert len(rows) == 2
    assert rows["SESS-10"]["USER_SURROGATE_KEY"] == 100
    assert rows["SESS-10"]["DEVICE_TYPE"] == "iOS"
    assert rows["SESS-11"]["USER_SURROGATE_KEY"] == 101
    assert rows["SESS-11"]["DEVICE_TYPE"] == "Desktop"


def test_prepare_sessions_missing_user_handling(spark: SparkSession) -> None:
    # Verify that sessions with unknown users are preserved with a null surrogate key
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
        schema=SESSIONS_SCHEMA,
    )
    empty_users_df = spark.createDataFrame([], schema=SILVER_USERS_SCHEMA)

    result_df = prepare_sessions_data(sessions=sessions_df, silver_users=empty_users_df)
    row = result_df.collect()[0]

    assert row["SESSION_ID"] == "SESS-99"
    assert row["USER_SURROGATE_KEY"] is None
