import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from ecommerce.logic_ecommerce.gold.dim_event_type import prepare_dim_event_type_data


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


def test_prepare_dim_event_type_deduplication(spark: SparkSession) -> None:
    schema = StructType([StructField("EVENT_TYPE", StringType(), True)])
    events_df = spark.createDataFrame([("Cart",), ("Cart",), ("Cart",)], schema=schema)

    result_df = prepare_dim_event_type_data(silver_events=events_df)
    assert result_df.count() == 1
