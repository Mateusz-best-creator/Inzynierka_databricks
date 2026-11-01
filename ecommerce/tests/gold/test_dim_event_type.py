from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from ecommerce.logic_ecommerce.gold.dim_event_type import prepare_dim_event_type_data

EVENTS_SCHEMA = StructType([StructField("EVENT_TYPE", StringType(), True)])

EXPECTED_COLUMNS = [
    "EVENT_TYPE_KEY",
    "EVENT_TYPE_NAME",
    "FUNNEL_STEP_ORDER",
    "FUNNEL_STAGE_NAME",
]


def test_prepare_dim_event_type_schema_and_empty_input(spark: SparkSession) -> None:
    # Verify output schema and empty dataframe handling
    empty_df = spark.createDataFrame([], schema=EVENTS_SCHEMA)

    result_df = prepare_dim_event_type_data(silver_events=empty_df)

    assert result_df.columns == EXPECTED_COLUMNS
    assert result_df.count() == 0


def test_prepare_dim_event_type_funnel_mapping(spark: SparkSession) -> None:
    # Verify funnel step order and stage classification for standard event types
    events_df = spark.createDataFrame(
        [
            ("View",),
            ("AddToCart",),
            ("Purchase",),
            ("CustomEventXYZ",),
        ],
        schema=EVENTS_SCHEMA,
    )

    result_df = prepare_dim_event_type_data(silver_events=events_df)
    rows = {row["EVENT_TYPE_NAME"]: row for row in result_df.collect()}

    assert len(rows) == 4
    assert rows["View"]["FUNNEL_STEP_ORDER"] == 1
    assert "Awareness" in rows["View"]["FUNNEL_STAGE_NAME"]

    assert rows["AddToCart"]["FUNNEL_STEP_ORDER"] == 2
    assert "Consideration" in rows["AddToCart"]["FUNNEL_STAGE_NAME"]

    assert rows["Purchase"]["FUNNEL_STEP_ORDER"] == 4
    assert "Conversion" in rows["Purchase"]["FUNNEL_STAGE_NAME"]

    assert rows["CustomEventXYZ"]["FUNNEL_STEP_ORDER"] == 5
    assert "Other" in rows["CustomEventXYZ"]["FUNNEL_STAGE_NAME"]


def test_prepare_dim_event_type_generator_events_and_deduplication(
    spark: SparkSession,
) -> None:
    # Verify snake_case generator event mapping, duplicate removal, and null filtering
    events_df = spark.createDataFrame(
        [
            ("page_view",),
            ("page_view",),
            ("view_product",),
            ("search",),
            ("add_to_cart",),
            ("remove_from_cart",),
            ("checkout",),
            (None,),
        ],
        schema=EVENTS_SCHEMA,
    )

    result_df = prepare_dim_event_type_data(silver_events=events_df)
    rows = {row["EVENT_TYPE_NAME"]: row for row in result_df.collect()}

    assert len(rows) == 6
    assert rows["page_view"]["FUNNEL_STEP_ORDER"] == 1
    assert rows["view_product"]["FUNNEL_STEP_ORDER"] == 1
    assert rows["search"]["FUNNEL_STEP_ORDER"] == 1
    assert rows["add_to_cart"]["FUNNEL_STEP_ORDER"] == 2
    assert rows["remove_from_cart"]["FUNNEL_STEP_ORDER"] == 2
    assert rows["checkout"]["FUNNEL_STEP_ORDER"] == 3
    assert "Conversion" in rows["checkout"]["FUNNEL_STAGE_NAME"]
