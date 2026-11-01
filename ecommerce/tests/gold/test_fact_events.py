import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp
from ecommerce.logic_ecommerce.gold.fact_events import prepare_fact_events_data


def test_prepare_fact_events_schema(spark: SparkSession) -> None:
    # Verify the exact output columns and their order in the resulting fact table
    silver_events = spark.createDataFrame(
        [],
        schema="EVENT_ID string, SESSION_ID string, EVENT_TIME timestamp, EVENT_TYPE string, PRODUCT_ID string",
    )
    silver_sessions = spark.createDataFrame(
        [], schema="SESSION_ID string, USER_SURROGATE_KEY int"
    )
    silver_users = spark.createDataFrame(
        [],
        schema="USER_SURROGATE_KEY int, CITY string, STATE string, COUNTRY string, ZIP_CODE string",
    )
    dim_date = spark.createDataFrame([], schema="FULL_DATE date, DATE_KEY int")
    dim_time = spark.createDataFrame(
        [], schema="HOUR int, MINUTE int, SECONDS int, TIME_KEY int"
    )
    dim_session = spark.createDataFrame(
        [], schema="SESSION_ID string, SESSION_SURROGATE_KEY int"
    )
    dim_user = spark.createDataFrame([], schema="USER_SURROGATE_KEY int")
    dim_geography = spark.createDataFrame(
        [],
        schema="CITY string, STATE string, COUNTRY string, ZIP_CODE string, GEOGRAPHY_KEY int",
    )
    dim_event_type = spark.createDataFrame(
        [], schema="EVENT_TYPE_NAME string, EVENT_TYPE_KEY int"
    )
    dim_product = spark.createDataFrame(
        [],
        schema="PRODUCT_SURROGATE_KEY int, PRODUCT_ID string, BASE_PRICE double, STOCK_QUANTITY int, VALID_FROM timestamp, VALID_TO timestamp",
    )

    result_df = prepare_fact_events_data(
        silver_events,
        silver_sessions,
        silver_users,
        dim_date,
        dim_time,
        dim_session,
        dim_user,
        dim_geography,
        dim_product,
        dim_event_type,
    )

    expected_columns = [
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
    assert result_df.columns == expected_columns


def test_prepare_fact_events_calculations_and_joins(spark: SparkSession) -> None:
    # Setup mock data for silver layer and dimensions to test event sequence and SCD2 product logic
    silver_events = spark.createDataFrame(
        [
            ("EVT-1", "SESS-1", "2025-01-01 10:00:00", "View", "PROD-1"),
            ("EVT-2", "SESS-1", "2025-01-01 10:00:45", "Cart", "PROD-1"),
        ],
        ["EVENT_ID", "SESSION_ID", "EVENT_TIME", "EVENT_TYPE", "PRODUCT_ID"],
    ).withColumn("EVENT_TIME", to_timestamp("EVENT_TIME"))

    silver_sessions = spark.createDataFrame(
        [("SESS-1", 99)], ["SESSION_ID", "USER_SURROGATE_KEY"]
    )

    silver_users = spark.createDataFrame(
        [(99, "Warsaw", "Mazowieckie", "Poland", "00-001")],
        ["USER_SURROGATE_KEY", "CITY", "STATE", "COUNTRY", "ZIP_CODE"],
    )

    dim_date = spark.createDataFrame(
        [("2025-01-01", 20250101)], ["FULL_DATE", "DATE_KEY"]
    )
    dim_time = spark.createDataFrame(
        [(10, 0, 0, 100000), (10, 0, 45, 100045)],
        ["HOUR", "MINUTE", "SECONDS", "TIME_KEY"],
    )
    dim_session = spark.createDataFrame(
        [("SESS-1", 101)], ["SESSION_ID", "SESSION_SURROGATE_KEY"]
    )
    dim_user = spark.createDataFrame([(99,)], ["USER_SURROGATE_KEY"])
    dim_geography = spark.createDataFrame(
        [("Warsaw", "Mazowieckie", "Poland", 55)],
        ["CITY", "STATE", "COUNTRY", "GEOGRAPHY_KEY"],
    )
    dim_event_type = spark.createDataFrame(
        [("View", 1), ("Cart", 2)], ["EVENT_TYPE_NAME", "EVENT_TYPE_KEY"]
    )

    # Setup SCD2 product history to verify correct time-based join
    dim_product = (
        spark.createDataFrame(
            [
                (
                    1,
                    "PROD-1",
                    100.0,
                    10,
                    "2024-01-01 00:00:00",
                    "2025-01-01 10:00:30",
                ),  # Old price
                (2, "PROD-1", 120.0, 5, "2025-01-01 10:00:30", None),  # New price
            ],
            [
                "PRODUCT_SURROGATE_KEY",
                "PRODUCT_ID",
                "BASE_PRICE",
                "STOCK_QUANTITY",
                "VALID_FROM",
                "VALID_TO",
            ],
        )
        .withColumn("VALID_FROM", to_timestamp("VALID_FROM"))
        .withColumn("VALID_TO", to_timestamp("VALID_TO"))
    )

    # Execute fact transformation
    result_df = prepare_fact_events_data(
        silver_events,
        silver_sessions,
        silver_users,
        dim_date,
        dim_time,
        dim_session,
        dim_user,
        dim_geography,
        dim_product,
        dim_event_type,
    )

    rows = {r["EVENT_ID"]: r for r in result_df.collect()}

    # Verify event sequencing and duration calculations
    assert len(rows) == 2
    assert rows["EVT-1"]["EVENT_SEQUENCE_IN_SESSION"] == 1
    assert rows["EVT-1"]["TIME_ON_STEP_SECONDS"] == 45
    assert rows["EVT-2"]["EVENT_SEQUENCE_IN_SESSION"] == 2
    assert rows["EVT-2"]["TIME_ON_STEP_SECONDS"] == 0

    # Verify dimension surrogate key resolution
    assert rows["EVT-1"]["USER_SURROGATE_KEY"] == 99
    assert rows["EVT-1"]["GEOGRAPHY_KEY"] == 55
    assert rows["EVT-1"]["SESSION_SURROGATE_KEY"] == 101
    assert rows["EVT-1"]["EVENT_TYPE_KEY"] == 1
    assert rows["EVT-1"]["EVENT_DATE_KEY"] == 20250101
    assert rows["EVT-1"]["EVENT_TIME_KEY"] == 100000

    # Verify SCD2 logic for product properties at the exact time of the event
    assert rows["EVT-1"]["PRODUCT_SURROGATE_KEY"] == 1
    assert rows["EVT-1"]["PRODUCT_PRICE_AT_EVENT"] == 100.0

    assert rows["EVT-2"]["PRODUCT_SURROGATE_KEY"] == 2
    assert rows["EVT-2"]["PRODUCT_PRICE_AT_EVENT"] == 120.0


def test_prepare_fact_events_no_multiplication_on_zip_and_scd_boundary(
    spark: SparkSession,
) -> None:
    # Verify exact SCD2 boundary timestamp and multiple zip codes per city do not duplicate rows
    silver_events = spark.createDataFrame(
        [("EVT-BOUNDARY", "SESS-1", "2025-01-01 10:00:30", "View", "PROD-1")],
        ["EVENT_ID", "SESSION_ID", "EVENT_TIME", "EVENT_TYPE", "PRODUCT_ID"],
    ).withColumn("EVENT_TIME", to_timestamp("EVENT_TIME"))

    silver_sessions = spark.createDataFrame(
        [("SESS-1", 99)], ["SESSION_ID", "USER_SURROGATE_KEY"]
    )
    silver_users = spark.createDataFrame(
        [(99, "Warsaw", "Mazowieckie", "Poland", "00-002")],
        ["USER_SURROGATE_KEY", "CITY", "STATE", "COUNTRY", "ZIP_CODE"],
    )
    dim_date = spark.createDataFrame(
        [("2025-01-01", 20250101)], ["FULL_DATE", "DATE_KEY"]
    )
    dim_time = spark.createDataFrame(
        [(10, 0, 30, 100030)], ["HOUR", "MINUTE", "SECONDS", "TIME_KEY"]
    )
    dim_session = spark.createDataFrame(
        [("SESS-1", 101)], ["SESSION_ID", "SESSION_SURROGATE_KEY"]
    )
    dim_user = spark.createDataFrame([(99,)], ["USER_SURROGATE_KEY"])
    dim_geography = spark.createDataFrame(
        [
            ("Warsaw", "Mazowieckie", "Poland", "00-001", 55),
            ("Warsaw", "Mazowieckie", "Poland", "00-002", 56),
        ],
        ["CITY", "STATE", "COUNTRY", "ZIP_CODE", "GEOGRAPHY_KEY"],
    )
    dim_event_type = spark.createDataFrame(
        [("View", 1)], ["EVENT_TYPE_NAME", "EVENT_TYPE_KEY"]
    )
    dim_product = (
        spark.createDataFrame(
            [
                (1, "PROD-1", 100.0, 10, "2024-01-01 00:00:00", "2025-01-01 10:00:30"),
                (2, "PROD-1", 120.0, 5, "2025-01-01 10:00:30", None),
            ],
            schema="PRODUCT_SURROGATE_KEY int, PRODUCT_ID string, BASE_PRICE double, STOCK_QUANTITY int, VALID_FROM string, VALID_TO string",
        )
        .withColumn("VALID_FROM", to_timestamp("VALID_FROM"))
        .withColumn("VALID_TO", to_timestamp("VALID_TO"))
    )

    result_df = prepare_fact_events_data(
        silver_events,
        silver_sessions,
        silver_users,
        dim_date,
        dim_time,
        dim_session,
        dim_user,
        dim_geography,
        dim_product,
        dim_event_type,
    )
    rows = result_df.collect()

    assert len(rows) == 1
    assert rows[0]["PRODUCT_SURROGATE_KEY"] == 2
    assert rows[0]["GEOGRAPHY_KEY"] == 56
