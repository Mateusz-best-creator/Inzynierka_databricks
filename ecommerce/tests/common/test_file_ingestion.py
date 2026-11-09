from unittest.mock import patch
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, IntegerType, StringType

from ecommerce.common_functions.file_ingestion import read_file

SCHEMA_CONFIG = [
    {
        "source_col_name": "id_src",
        "target_col_name": "ID",
        "target_datatype": "integer",
    },
    {
        "source_col_name": "name_src",
        "target_col_name": "NAME",
        "target_datatype": "string",
    },
    {
        "source_col_name": "price_src",
        "target_col_name": "PRICE",
        "target_datatype": "double",
    },
]


@patch("ecommerce.common_functions.file_ingestion.read_csv_file")
def test_read_file_schema_mapping(mock_read_csv, spark: SparkSession) -> None:
    # Verify that string datatype definitions map to proper PySpark types during ingestion
    mock_read_csv.return_value = spark.createDataFrame([], schema="dummy_source STRING")

    read_file(
        spark=spark,
        dbutils=None,
        file_cloud_loc="mock/path.csv",
        schema=SCHEMA_CONFIG,
        file_format="csv",
    )

    applied_schema = mock_read_csv.call_args[1]["schema"]

    assert isinstance(applied_schema["id_src"].dataType, IntegerType)
    assert isinstance(applied_schema["name_src"].dataType, StringType)
    assert isinstance(applied_schema["price_src"].dataType, DoubleType)


def test_read_file_invalid_format(spark: SparkSession) -> None:
    # Verify that an unsupported file format raises a ValueError
    with pytest.raises(ValueError, match="Invalid file format specified: txt"):
        read_file(
            spark=spark,
            dbutils=None,
            file_cloud_loc="mock/path.txt",
            schema=SCHEMA_CONFIG,
            file_format="txt",
        )
