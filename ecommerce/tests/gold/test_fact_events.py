import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp
from ecommerce.logic_ecommerce.gold.fact_events import prepare_fact_events_data


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
    assert rows["EVT-2"]["TIME_ON_STEP_SECONDS"] is None

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
