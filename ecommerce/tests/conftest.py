import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """"""
    spark = (
            SparkSession.builder
            .master("local[*]")  # Use at least 2 cores to catch concurrency issues
            .appName("pytest-pyspark-suite")
            .config("spark.sql.shuffle.partitions", "2")  # Speed up shuffle operations for small test data
            .config("spark.ui.enabled", "false")          # Disable the web UI to save resources and avoid port clashes
            .config("spark.driver.bindAddress", "127.0.0.1") # Prevent network/firewall popup issues on local machines
            .getOrCreate()
        )

    yield spark

    spark.stop()
