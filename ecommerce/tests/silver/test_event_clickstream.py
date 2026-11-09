from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from ecommerce.logic_ecommerce.silver.event_clickstream import prepare_clickstream_data

EVENTS_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("event_time", StringType(), True),
        StructField("session_id", StringType(), True),
        StructField("product_id", StringType(), True),
    ]
)

EXPECTED_COLUMNS = [
    "EVENT_SURROGATE_KEY",
    "EVENT_ID",
    "EVENT_TYPE",
    "EVENT_TIME",
    "SESSION_ID",
    "PRODUCT_ID",
]


def test_prepare_clickstream_schema_and_empty_input(spark: SparkSession) -> None:
    # Verify output schema and empty dataframe handling
    empty_df = spark.createDataFrame([], schema=EVENTS_SCHEMA)

    result_df = prepare_clickstream_data(event_clickstream=empty_df)

    assert result_df.columns == EXPECTED_COLUMNS
    assert result_df.count() == 0


def test_prepare_clickstream_transformations_and_keys(spark: SparkSession) -> None:
    # Verify attribute mapping and unique surrogate key generation across events
    events_df = spark.createDataFrame(
        [
            ("EVT-10", "VIEW", "2025-03-10 12:00:00", "SESS-10", "PROD-100"),
            ("EVT-11", "PURCHASE", "2025-03-11 14:00:00", "SESS-11", "PROD-101"),
        ],
        schema=EVENTS_SCHEMA,
    )

    result_df = prepare_clickstream_data(event_clickstream=events_df)
    rows = {row["EVENT_ID"]: row for row in result_df.collect()}

    assert len(rows) == 2
    assert rows["EVT-10"]["EVENT_TYPE"] == "VIEW"
    assert rows["EVT-10"]["SESSION_ID"] == "SESS-10"
    assert rows["EVT-10"]["PRODUCT_ID"] == "PROD-100"
    assert rows["EVT-11"]["EVENT_TYPE"] == "PURCHASE"
    assert (
        rows["EVT-10"]["EVENT_SURROGATE_KEY"] != rows["EVT-11"]["EVENT_SURROGATE_KEY"]
    )
