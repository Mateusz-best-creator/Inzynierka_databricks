import pytest
from pyspark.sql import SparkSession

from ecommerce.logic_ecommerce.silver.event_clickstream import prepare_clickstream_data


def test_prepare_clickstream_schema_and_columns(spark: SparkSession) -> None:
    # Arrange verifying output schema and required inputs
    events_df = spark.createDataFrame(
        [("EVT-1", "CLICK", "2025-01-15 10:05:00", "SESS-1", "PROD-1")],
        ["event_id", "event_type", "event_time", "session_id", "product_id"],
    )

    # Act
    result_df = prepare_clickstream_data(
        event_clickstream=events_df,
    )

    # Assert checking uppercase columns and expected order
    expected_columns = [
        "EVENT_SURROGATE_KEY",
        "EVENT_ID",
        "EVENT_TYPE",
        "EVENT_TIME",
        "SESSION_ID",
        "PRODUCT_ID",
    ]
    assert result_df.columns == expected_columns


def test_prepare_clickstream_transformations(spark: SparkSession) -> None:
    # Arrange multiple clickstream events
    events_df = spark.createDataFrame(
        [
            ("EVT-10", "VIEW", "2025-03-10 12:00:00", "SESS-10", "PROD-100"),
            ("EVT-11", "PURCHASE", "2025-03-11 14:00:00", "SESS-11", "PROD-101"),
        ],
        ["event_id", "event_type", "event_time", "session_id", "product_id"],
    )

    # Act
    result_df = prepare_clickstream_data(
        event_clickstream=events_df,
    )
    rows = {row["EVENT_ID"]: row for row in result_df.collect()}

    # Assert
    assert len(rows) == 2
    assert rows["EVT-10"]["EVENT_TYPE"] == "VIEW"
    assert rows["EVT-10"]["SESSION_ID"] == "SESS-10"
    assert rows["EVT-10"]["PRODUCT_ID"] == "PROD-100"
    assert rows["EVT-11"]["EVENT_TYPE"] == "PURCHASE"
    assert rows["EVT-11"]["SESSION_ID"] == "SESS-11"
    assert rows["EVT-11"]["PRODUCT_ID"] == "PROD-101"


def test_prepare_clickstream_surrogate_key_generation(spark: SparkSession) -> None:
    # Arrange multiple events to ensure surrogate key is generated for every row
    events_df = spark.createDataFrame(
        [
            ("EVT-20", "CLICK", "2025-04-01 08:00:00", "SESS-20", "PROD-200"),
            ("EVT-21", "HOVER", "2025-04-01 08:05:00", "SESS-20", "PROD-200"),
        ],
        ["event_id", "event_type", "event_time", "session_id", "product_id"],
    )

    # Act
    result_df = prepare_clickstream_data(
        event_clickstream=events_df,
    )
    rows = result_df.collect()

    # Assert surrogate keys exist and are unique/present
    assert len(rows) == 2
    assert rows[0]["EVENT_SURROGATE_KEY"] is not None
    assert rows[1]["EVENT_SURROGATE_KEY"] is not None
    assert rows[0]["EVENT_SURROGATE_KEY"] != rows[1]["EVENT_SURROGATE_KEY"]