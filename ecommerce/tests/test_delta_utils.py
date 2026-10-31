from pyspark.sql import SparkSession
from pyspark.sql.types import TimestampType

from ecommerce.common_functions.delta_utils import add_ingestion_timestamp_column


def test_add_ingestion_timestamp_column(spark: SparkSession) -> None:
    # Verify if timestamp column is added with correct data type
    df = spark.createDataFrame([("test_data",)], ["col1"])

    result_df = add_ingestion_timestamp_column(df)

    assert "INGESTED_AT" in result_df.columns
    assert isinstance(result_df.schema["INGESTED_AT"].dataType, TimestampType)
    assert result_df.count() == 1
