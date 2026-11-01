import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from ecommerce.logic_ecommerce.gold.dim_event_type import prepare_dim_event_type_data


def test_prepare_dim_event_type_schema(spark: SparkSession) -> None:
    schema = StructType([StructField("EVENT_TYPE", StringType(), True)])
    events_df = spark.createDataFrame([], schema=schema)

    result_df = prepare_dim_event_type_data(silver_events=events_df)

    expected_columns = [
        "EVENT_TYPE_KEY",
        "EVENT_TYPE_NAME",
        "FUNNEL_STEP_ORDER",
        "FUNNEL_STAGE_NAME",
    ]
    assert result_df.columns == expected_columns


def test_prepare_dim_event_type_mapping_and_order(spark: SparkSession) -> None:
    schema = StructType([StructField("EVENT_TYPE", StringType(), True)])

    # Mix of known and unknown event types
    events_df = spark.createDataFrame(
        [
            ("View",),
            ("AddToCart",),
            ("Purchase",),
            ("CustomEventXYZ",),
            (None,),  # Should be filtered out
        ],
        schema=schema,
    )

    result_df = prepare_dim_event_type_data(silver_events=events_df)

    # Extract results as a dictionary for easy assertion
    rows = {row["EVENT_TYPE_NAME"]: row for row in result_df.collect()}

    # Assertions
    assert len(rows) == 4  # Null should be dropped
    assert "EVENT_TYPE_KEY" in result_df.columns

    assert rows["View"]["FUNNEL_STEP_ORDER"] == 1
    assert "Awareness" in rows["View"]["FUNNEL_STAGE_NAME"]

    assert rows["AddToCart"]["FUNNEL_STEP_ORDER"] == 2
    assert "Consideration" in rows["AddToCart"]["FUNNEL_STAGE_NAME"]

    assert rows["Purchase"]["FUNNEL_STEP_ORDER"] == 4
    assert "Conversion" in rows["Purchase"]["FUNNEL_STAGE_NAME"]

    assert rows["CustomEventXYZ"]["FUNNEL_STEP_ORDER"] == 5
    assert "Other" in rows["CustomEventXYZ"]["FUNNEL_STAGE_NAME"]


def test_prepare_dim_event_type_snake_case_generator_events(
    spark: SparkSession,
) -> None:
    # Verify snake_case event types produced by the data generator are mapped accurately
    schema = StructType([StructField("EVENT_TYPE", StringType(), True)])
    events_df = spark.createDataFrame(
        [
            ("page_view",),
            ("view_product",),
            ("search",),
            ("add_to_cart",),
            ("remove_from_cart",),
            ("checkout",),
        ],
        schema=schema,
    )

    result_df = prepare_dim_event_type_data(silver_events=events_df)
    rows = {row["EVENT_TYPE_NAME"]: row for row in result_df.collect()}

    assert rows["page_view"]["FUNNEL_STEP_ORDER"] == 1
    assert rows["view_product"]["FUNNEL_STEP_ORDER"] == 1
    assert rows["search"]["FUNNEL_STEP_ORDER"] == 1
    assert rows["add_to_cart"]["FUNNEL_STEP_ORDER"] == 2
    assert rows["remove_from_cart"]["FUNNEL_STEP_ORDER"] == 2
    assert rows["checkout"]["FUNNEL_STEP_ORDER"] == 3
    assert "Conversion" in rows["checkout"]["FUNNEL_STAGE_NAME"]


def test_prepare_dim_event_type_deduplication(spark: SparkSession) -> None:
    schema = StructType([StructField("EVENT_TYPE", StringType(), True)])
    events_df = spark.createDataFrame([("Cart",), ("Cart",), ("Cart",)], schema=schema)

    result_df = prepare_dim_event_type_data(silver_events=events_df)
    assert result_df.count() == 1
