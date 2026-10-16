import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """"""
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("pytest-pyspark-suite")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )

    yield spark

    spark.stop()
