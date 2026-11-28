import os
import sys
import pytest
from pyspark.sql import SparkSession

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


@pytest.fixture(scope="session")
def spark():
    """Initializes a lightweight local SparkSession for the test session.
    Yields:
        Configured local SparkSession instance.
    """
    spark = (
        SparkSession.builder.master("local[1]")
        .appName("pytest-pyspark-suite")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.extraJavaOptions", "-Djava.security.debug=off")
        .getOrCreate()
    )

    yield spark

    spark.stop()
