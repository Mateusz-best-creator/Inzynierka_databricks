from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from ecommerce.logic_ecommerce.gold.dim_user import prepare_dim_user_data

USERS_SCHEMA = StructType(
    [
        StructField("USER_SURROGATE_KEY", IntegerType(), True),
        StructField("USER_ID", StringType(), True),
        StructField("CUSTOMER_UNIQUE_ID", StringType(), True),
        StructField("FIRST_NAME", StringType(), True),
        StructField("LAST_NAME", StringType(), True),
        StructField("EMAIL", StringType(), True),
        StructField("PHONE", StringType(), True),
        StructField("BIRTH_DATE", StringType(), True),
        StructField("GENDER", StringType(), True),
        StructField("STATUS", StringType(), True),
        StructField("CREATED_AT", StringType(), True),
        StructField("VALID_FROM", StringType(), True),
        StructField("VALID_TO", StringType(), True),
    ]
)

EXPECTED_COLUMNS = [
    "USER_SURROGATE_KEY",
    "USER_ID",
    "CUSTOMER_UNIQUE_ID",
    "FIRST_NAME",
    "LAST_NAME",
    "EMAIL",
    "PHONE",
    "BIRTH_DATE",
    "AGE_GROUP",
    "GENDER",
    "STATUS",
    "CREATED_AT",
    "VALID_FROM",
    "VALID_TO",
    "SCD_RECORD_STATUS",
]


def test_prepare_dim_user_schema_and_empty_input(spark: SparkSession) -> None:
    # Verify output schema and empty dataframe handling
    empty_df = spark.createDataFrame([], schema=USERS_SCHEMA)

    result_df = prepare_dim_user_data(silver_users=empty_df)

    assert result_df.columns == EXPECTED_COLUMNS
    assert result_df.count() == 0


def test_prepare_dim_user_age_groups_and_scd_status(spark: SparkSession) -> None:
    # Verify age bracket derivation and SCD2 record status classification
    users_df = spark.createDataFrame(
        [
            (
                1,
                "U1",
                "C1",
                "A",
                "B",
                "a@b",
                "123",
                "2005-01-01",
                "M",
                "Active",
                "2025-01-01",
                "2025-01-01",
                None,
            ),
            (
                2,
                "U2",
                "C2",
                "C",
                "D",
                "c@d",
                "456",
                "1995-01-01",
                "F",
                "Active",
                "2025-01-01",
                "2025-01-01",
                "2025-02-01",
            ),
            (
                3,
                "U3",
                "C3",
                "E",
                "F",
                "e@f",
                "789",
                "1985-01-01",
                "M",
                "Active",
                "2025-01-01",
                "2025-01-01",
                None,
            ),
            (
                4,
                "U4",
                "C4",
                "G",
                "H",
                "g@h",
                "000",
                "1960-01-01",
                "F",
                "Active",
                "2025-01-01",
                "2025-01-01",
                None,
            ),
            (
                5,
                "U5",
                "C5",
                "I",
                "J",
                "i@j",
                "111",
                None,
                "M",
                "Active",
                "2025-01-01",
                "2025-01-01",
                None,
            ),
        ],
        schema=USERS_SCHEMA,
    )

    result_df = prepare_dim_user_data(silver_users=users_df)
    rows = {r["USER_ID"]: r for r in result_df.collect()}

    assert len(rows) == 5
    assert rows["U1"]["SCD_RECORD_STATUS"] == "Current"
    assert rows["U2"]["SCD_RECORD_STATUS"] == "Historical"

    assert rows["U1"]["AGE_GROUP"] == "18-24"
    assert rows["U2"]["AGE_GROUP"] == "25-34"
    assert rows["U3"]["AGE_GROUP"] == "35-49"
    assert rows["U4"]["AGE_GROUP"] == "50+"
    assert rows["U5"]["AGE_GROUP"] == "Unknown"
