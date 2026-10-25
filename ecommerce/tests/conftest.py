import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    # Initialize local Spark session with Delta Lake support and UTC timezone
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("pytest-pyspark-suite")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.driver.extraJavaOptions", "-Djava.security.debug=off")
        .getOrCreate()
    )

    yield spark

    spark.stop()