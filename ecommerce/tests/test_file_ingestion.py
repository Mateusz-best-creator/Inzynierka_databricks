from unittest.mock import patch
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, IntegerType, StringType

from ecommerce.common_functions.file_ingestion import read_file


@patch("ecommerce.common_functions.file_ingestion.read_csv_file")
def test_read_file_schema_mapping(mock_read_csv, spark: SparkSession) -> None:
    # Mock CSV reader to return an empty dataframe and verify schema type mapping
    mock_read_csv.return_value = spark.createDataFrame([], schema="dummy_source STRING")

    schema_config = [
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

    read_file(
        spark=spark,
        dbutils=None,
        file_cloud_loc="mock/path.csv",
        schema=schema_config,
        file_format="csv",
    )

    # Extract the schema passed to read_csv_file
    call_args = mock_read_csv.call_args[1]
    applied_schema = call_args["schema"]

    # Verify PySpark types generated from string configuration
    assert isinstance(applied_schema["id_src"].dataType, IntegerType)
    assert isinstance(applied_schema["name_src"].dataType, StringType)
    assert isinstance(applied_schema["price_src"].dataType, DoubleType)


def test_read_file_invalid_format(spark: SparkSession) -> None:
    # Verify exception is raised for unsupported file format
    with pytest.raises(ValueError, match="Invalid file format specified: txt"):
        read_file(
            spark=spark,
            dbutils=None,
            file_cloud_loc="path",
            schema=[
                {
                    "source_col_name": "id",
                    "target_col_name": "id",
                    "target_datatype": "string",
                }
            ],
            file_format="txt",
        )
