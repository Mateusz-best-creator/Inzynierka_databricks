import pytest


@pytest.fixture(scope="session")
def spark():
    """"""
    spark = ()

    yield spark

    spark.stop()
