from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from ecommerce.logic_ecommerce.silver.users import join_users_data

LOCATIONS_SCHEMA = StructType(
    [
        StructField("location_id", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("zip_code", StringType(), True),
        StructField("country", StringType(), True),
    ]
)

CONTACTS_SCHEMA = StructType(
    [
        StructField("user_id", StringType(), True),
        StructField("email", StringType(), True),
        StructField("phone", StringType(), True),
    ]
)

USER_LOCATIONS_SCHEMA = StructType(
    [
        StructField("id", IntegerType(), True),
        StructField("user_id", StringType(), True),
        StructField("location_id", StringType(), True),
        StructField("valid_from", StringType(), True),
        StructField("valid_to", StringType(), True),
        StructField("is_current", IntegerType(), True),
    ]
)

PROFILES_SCHEMA = StructType(
    [
        StructField("user_id", StringType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("birth_date", StringType(), True),
        StructField("gender", StringType(), True),
    ]
)

STATUS_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("status", StringType(), True),
        StructField("status_change_description", StringType(), True),
        StructField("valid_from", StringType(), True),
        StructField("valid_to", StringType(), True),
        StructField("is_current", IntegerType(), True),
    ]
)

USERS_SCHEMA = StructType(
    [
        StructField("user_id", StringType(), True),
        StructField("customer_unique_id", StringType(), True),
        StructField("created_at", StringType(), True),
    ]
)


def test_join_users_data_schema_and_scd2_timeline(spark: SparkSession) -> None:
    # Verify SCD2 timeline slicing when a user changes location over time
    locations_df = spark.createDataFrame(
        [
            ("LOC-1", "Gdansk", "Pomorskie", "80-001", "Poland"),
            ("LOC-2", "Warszawa", "Mazowieckie", "00-001", "Poland"),
        ],
        schema=LOCATIONS_SCHEMA,
    )
    contacts_df = spark.createDataFrame(
        [("USR-1", "jan@example.com", "123456789")],
        schema=CONTACTS_SCHEMA,
    )
    profiles_df = spark.createDataFrame(
        [("USR-1", "Jan", "Kowalski", "1999-05-10", "M")],
        schema=PROFILES_SCHEMA,
    )
    users_df = spark.createDataFrame(
        [("USR-1", "CUST-UNQ-1", "2025-01-01 00:00:00")],
        schema=USERS_SCHEMA,
    )
    user_locations_df = spark.createDataFrame(
        [
            (1, "USR-1", "LOC-1", "2025-01-01 00:00:00", "2025-06-01 00:00:00", 0),
            (2, "USR-1", "LOC-2", "2025-06-01 00:00:00", None, 1),
        ],
        schema=USER_LOCATIONS_SCHEMA,
    )
    status_df = spark.createDataFrame(
        [("ST-1", "USR-1", "ACTIVE", "Initial", "2025-01-01 00:00:00", None, 1)],
        schema=STATUS_SCHEMA,
    )

    result_df = join_users_data(
        locations=locations_df,
        user_contacts=contacts_df,
        user_locations=user_locations_df,
        user_profiles=profiles_df,
        user_status_history=status_df,
        users=users_df,
    )

    expected_columns = [
        "USER_SURROGATE_KEY",
        "USER_ID",
        "CUSTOMER_UNIQUE_ID",
        "FIRST_NAME",
        "LAST_NAME",
        "EMAIL",
        "PHONE",
        "BIRTH_DATE",
        "GENDER",
        "CITY",
        "STATE",
        "ZIP_CODE",
        "COUNTRY",
        "STATUS",
        "CREATED_AT",
        "VALID_FROM",
        "VALID_TO",
        "IS_ACTIVE",
    ]
    assert result_df.columns == expected_columns

    rows = sorted(result_df.collect(), key=lambda r: r["VALID_FROM"])
    assert len(rows) == 2

    # Historical record in Gdansk (IS_ACTIVE = 0)
    assert rows[0]["CITY"] == "Gdansk"
    assert rows[0]["VALID_FROM"] == "2025-01-01 00:00:00"
    assert rows[0]["VALID_TO"] == "2025-06-01 00:00:00"
    assert rows[0]["IS_ACTIVE"] == 0

    # Current record in Warszawa (IS_ACTIVE = 1)
    assert rows[1]["CITY"] == "Warszawa"
    assert rows[1]["VALID_FROM"] == "2025-06-01 00:00:00"
    assert rows[1]["VALID_TO"] is None
    assert rows[1]["IS_ACTIVE"] == 1


def test_join_users_data_empty_dataframes(spark: SparkSession) -> None:
    # Verify behavior on empty input dataframes
    result_df = join_users_data(
        locations=spark.createDataFrame([], schema=LOCATIONS_SCHEMA),
        user_contacts=spark.createDataFrame([], schema=CONTACTS_SCHEMA),
        user_locations=spark.createDataFrame([], schema=USER_LOCATIONS_SCHEMA),
        user_profiles=spark.createDataFrame([], schema=PROFILES_SCHEMA),
        user_status_history=spark.createDataFrame([], schema=STATUS_SCHEMA),
        users=spark.createDataFrame([], schema=USERS_SCHEMA),
    )

    assert result_df.count() == 0
