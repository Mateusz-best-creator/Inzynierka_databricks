from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from ecommerce.logic_ecommerce.gold.fact_events import prepare_fact_events_data

SILVER_EVENTS_SCHEMA = StructType(
    [
        StructField("EVENT_ID", StringType(), True),
        StructField("SESSION_ID", StringType(), True),
        StructField("EVENT_TIME", StringType(), True),
        StructField("EVENT_TYPE", StringType(), True),
        StructField("PRODUCT_ID", StringType(), True),
    ]
)

SILVER_SESSIONS_SCHEMA = StructType(
    [
        StructField("SESSION_ID", StringType(), True),
        StructField("USER_SURROGATE_KEY", IntegerType(), True),
    ]
)

SILVER_USERS_SCHEMA = StructType(
    [
        StructField("USER_SURROGATE_KEY", IntegerType(), True),
        StructField("CITY", StringType(), True),
        StructField("STATE", StringType(), True),
        StructField("COUNTRY", StringType(), True),
        StructField("ZIP_CODE", StringType(), True),
    ]
)

DIM_DATE_SCHEMA = StructType(
    [
        StructField("FULL_DATE", StringType(), True),
        StructField("DATE_KEY", IntegerType(), True),
    ]
)

DIM_TIME_SCHEMA = StructType(
    [
        StructField("HOUR", IntegerType(), True),
        StructField("MINUTE", IntegerType(), True),
        StructField("SECONDS", IntegerType(), True),
        StructField("TIME_KEY", IntegerType(), True),
    ]
)

DIM_SESSION_SCHEMA = StructType(
    [
        StructField("SESSION_ID", StringType(), True),
        StructField("SESSION_SURROGATE_KEY", IntegerType(), True),
    ]
)

DIM_USER_SCHEMA = StructType(
    [
        StructField("USER_SURROGATE_KEY", IntegerType(), True),
    ]
)

DIM_GEOGRAPHY_SCHEMA = StructType(
    [
        StructField("CITY", StringType(), True),
        StructField("STATE", StringType(), True),
        StructField("COUNTRY", StringType(), True),
        StructField("ZIP_CODE", StringType(), True),
        StructField("GEOGRAPHY_KEY", IntegerType(), True),
    ]
)

DIM_EVENT_TYPE_SCHEMA = StructType(
    [
        StructField("EVENT_TYPE_NAME", StringType(), True),
        StructField("EVENT_TYPE_KEY", IntegerType(), True),
    ]
)

DIM_PRODUCT_SCHEMA = StructType(
    [
        StructField("PRODUCT_SURROGATE_KEY", IntegerType(), True),
        StructField("PRODUCT_ID", StringType(), True),
        StructField("BASE_PRICE", DoubleType(), True),
        StructField("STOCK_QUANTITY", IntegerType(), True),
        StructField("VALID_FROM", StringType(), True),
        StructField("VALID_TO", StringType(), True),
    ]
)

EXPECTED_COLUMNS = [
    "EVENT_DATE_KEY",
    "EVENT_TIME_KEY",
    "SESSION_SURROGATE_KEY",
    "USER_SURROGATE_KEY",
    "GEOGRAPHY_KEY",
    "PRODUCT_SURROGATE_KEY",
    "EVENT_TYPE_KEY",
    "EVENT_ID",
    "SESSION_ID",
    "EVENT_SEQUENCE_IN_SESSION",
    "TIME_ON_STEP_SECONDS",
    "PRODUCT_PRICE_AT_EVENT",
    "PRODUCT_STOCK_AT_EVENT",
]


def test_prepare_fact_events_schema_and_empty_input(spark: SparkSession) -> None:
    # Verify output schema and empty dataframe handling
    result_df = prepare_fact_events_data(
        silver_events=spark.createDataFrame([], schema=SILVER_EVENTS_SCHEMA),
        silver_sessions=spark.createDataFrame([], schema=SILVER_SESSIONS_SCHEMA),
        silver_users=spark.createDataFrame([], schema=SILVER_USERS_SCHEMA),
        dim_date=spark.createDataFrame([], schema=DIM_DATE_SCHEMA),
        dim_time=spark.createDataFrame([], schema=DIM_TIME_SCHEMA),
        dim_session=spark.createDataFrame([], schema=DIM_SESSION_SCHEMA),
        dim_user=spark.createDataFrame([], schema=DIM_USER_SCHEMA),
        dim_geography=spark.createDataFrame([], schema=DIM_GEOGRAPHY_SCHEMA),
        dim_product=spark.createDataFrame([], schema=DIM_PRODUCT_SCHEMA),
        dim_event_type=spark.createDataFrame([], schema=DIM_EVENT_TYPE_SCHEMA),
    )

    assert result_df.columns == EXPECTED_COLUMNS
    assert result_df.count() == 0


def test_prepare_fact_events_sequencing_and_scd2_joins(spark: SparkSession) -> None:
    # Verify event sequence numbering, step duration, and SCD2 product lookup
    silver_events = spark.createDataFrame(
        [
            ("EVT-1", "SESS-1", "2025-01-01 10:00:00", "View", "PROD-1"),
            ("EVT-2", "SESS-1", "2025-01-01 10:00:45", "Cart", "PROD-1"),
        ],
        schema=SILVER_EVENTS_SCHEMA,
    )
    silver_sessions = spark.createDataFrame(
        [("SESS-1", 99)], schema=SILVER_SESSIONS_SCHEMA
    )
    silver_users = spark.createDataFrame(
        [(99, "Warsaw", "Mazowieckie", "Poland", "00-001")],
        schema=SILVER_USERS_SCHEMA,
    )
    dim_date = spark.createDataFrame([("2025-01-01", 20250101)], schema=DIM_DATE_SCHEMA)
    dim_time = spark.createDataFrame(
        [(10, 0, 0, 100000), (10, 0, 45, 100045)], schema=DIM_TIME_SCHEMA
    )
    dim_session = spark.createDataFrame([("SESS-1", 101)], schema=DIM_SESSION_SCHEMA)
    dim_user = spark.createDataFrame([(99,)], schema=DIM_USER_SCHEMA)
    dim_geography = spark.createDataFrame(
        [("Warsaw", "Mazowieckie", "Poland", "00-001", 55)],
        schema=DIM_GEOGRAPHY_SCHEMA,
    )
    dim_event_type = spark.createDataFrame(
        [("View", 1), ("Cart", 2)], schema=DIM_EVENT_TYPE_SCHEMA
    )
    dim_product = spark.createDataFrame(
        [
            (1, "PROD-1", 100.0, 10, "2024-01-01 00:00:00", "2025-01-01 10:00:30"),
            (2, "PROD-1", 120.0, 5, "2025-01-01 10:00:30", None),
        ],
        schema=DIM_PRODUCT_SCHEMA,
    )

    result_df = prepare_fact_events_data(
        silver_events=silver_events,
        silver_sessions=silver_sessions,
        silver_users=silver_users,
        dim_date=dim_date,
        dim_time=dim_time,
        dim_session=dim_session,
        dim_user=dim_user,
        dim_geography=dim_geography,
        dim_product=dim_product,
        dim_event_type=dim_event_type,
    )
    rows = {r["EVENT_ID"]: r for r in result_df.collect()}

    assert len(rows) == 2
    assert rows["EVT-1"]["EVENT_SEQUENCE_IN_SESSION"] == 1
    assert rows["EVT-1"]["TIME_ON_STEP_SECONDS"] == 45
    assert rows["EVT-2"]["EVENT_SEQUENCE_IN_SESSION"] == 2
    assert rows["EVT-2"]["TIME_ON_STEP_SECONDS"] == 0

    assert rows["EVT-1"]["USER_SURROGATE_KEY"] == 99
    assert rows["EVT-1"]["GEOGRAPHY_KEY"] == 55
    assert rows["EVT-1"]["SESSION_SURROGATE_KEY"] == 101
    assert rows["EVT-1"]["EVENT_TYPE_KEY"] == 1
    assert rows["EVT-1"]["EVENT_DATE_KEY"] == 20250101
    assert rows["EVT-1"]["EVENT_TIME_KEY"] == 100000

    assert rows["EVT-1"]["PRODUCT_SURROGATE_KEY"] == 1
    assert rows["EVT-1"]["PRODUCT_PRICE_AT_EVENT"] == 100.0
    assert rows["EVT-2"]["PRODUCT_SURROGATE_KEY"] == 2
    assert rows["EVT-2"]["PRODUCT_PRICE_AT_EVENT"] == 120.0


def test_prepare_fact_events_no_fanout_on_zip_and_scd_boundary(
    spark: SparkSession,
) -> None:
    # Verify exact SCD2 boundary timestamp and multiple zip codes per city do not duplicate rows
    silver_events = spark.createDataFrame(
        [("EVT-BOUNDARY", "SESS-1", "2025-01-01 10:00:30", "View", "PROD-1")],
        schema=SILVER_EVENTS_SCHEMA,
    )
    silver_sessions = spark.createDataFrame(
        [("SESS-1", 99)], schema=SILVER_SESSIONS_SCHEMA
    )
    silver_users = spark.createDataFrame(
        [(99, "Warsaw", "Mazowieckie", "Poland", "00-002")],
        schema=SILVER_USERS_SCHEMA,
    )
    dim_date = spark.createDataFrame([("2025-01-01", 20250101)], schema=DIM_DATE_SCHEMA)
    dim_time = spark.createDataFrame([(10, 0, 30, 100030)], schema=DIM_TIME_SCHEMA)
    dim_session = spark.createDataFrame([("SESS-1", 101)], schema=DIM_SESSION_SCHEMA)
    dim_user = spark.createDataFrame([(99,)], schema=DIM_USER_SCHEMA)
    dim_geography = spark.createDataFrame(
        [
            ("Warsaw", "Mazowieckie", "Poland", "00-001", 55),
            ("Warsaw", "Mazowieckie", "Poland", "00-002", 56),
        ],
        schema=DIM_GEOGRAPHY_SCHEMA,
    )
    dim_event_type = spark.createDataFrame([("View", 1)], schema=DIM_EVENT_TYPE_SCHEMA)
    dim_product = spark.createDataFrame(
        [
            (1, "PROD-1", 100.0, 10, "2024-01-01 00:00:00", "2025-01-01 10:00:30"),
            (2, "PROD-1", 120.0, 5, "2025-01-01 10:00:30", None),
        ],
        schema=DIM_PRODUCT_SCHEMA,
    )

    result_df = prepare_fact_events_data(
        silver_events=silver_events,
        silver_sessions=silver_sessions,
        silver_users=silver_users,
        dim_date=dim_date,
        dim_time=dim_time,
        dim_session=dim_session,
        dim_user=dim_user,
        dim_geography=dim_geography,
        dim_product=dim_product,
        dim_event_type=dim_event_type,
    )
    rows = result_df.collect()

    assert len(rows) == 1
    assert rows[0]["PRODUCT_SURROGATE_KEY"] == 2
    assert rows[0]["GEOGRAPHY_KEY"] == 56
