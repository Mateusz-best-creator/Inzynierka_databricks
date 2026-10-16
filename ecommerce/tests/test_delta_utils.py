from pyspark.sql import SparkSession

import pytest
from ecommerce.common_functions.delta_utils import add_ingestion_timestamp_column

from datetime import datetime, timezone


def test_ingestion_timestamp_components(spark: SparkSession) -> None:
    data = [(1, "Mark"), (2, "Bob"), (3, "Alice")]
    input_df = spark.createDataFrame(data, "id INT, name STRING")
    result_df = add_ingestion_timestamp_column(input_df)

    rows = result_df.collect()
    for row in rows:
        ingested_at = row["INGESTED_AT"]

        expected_time = datetime.now(timezone.utc)

        assert ingested_at.year == expected_time.year
        assert ingested_at.month == expected_time.month
        assert ingested_at.day == expected_time.day
        assert ingested_at.hour == expected_time.hour
        assert ingested_at.minute == expected_time.minute
