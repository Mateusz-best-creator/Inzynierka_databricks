from pyspark.sql import SparkSession, DataFrame

from pyspark.sql.functions import col, regexp_replace

from ecommerce.common_functions.config_utils import get_config, set_logger
from ecommerce.common_functions.delta_utils import (
    create_table,
    optimize_vacuum_table,
)


def join_users_data(
    locations: DataFrame,
    user_contacts: DataFrame,
    user_locations: DataFrame,
    user_profiles: DataFrame,
    user_status_history: DataFrame,
    users: DataFrame,
) -> DataFrame:
    """"""
    user_location_joined = (
        user_locations.alias("ul")
        .join(
            other=locations.alias("loc"),
            on=col("ul.LOCATION_ID") == col("loc.LOCATION_ID"),
            how="inner",
        )
        .select(
            col("ul.USER_ID").alias("USER_ID"),
            col("ul.LOCATION_ID").alias("LOCATION_ID"),
            col("loc.CITY").alias("CITY"),
            col("loc.STATE").alias("STATE"),
            col("loc.ZIP_CODE").alias("ZIP_CODE"),
            col("loc.COUNTRY").alias("COUNTRY"),
        )
    )

    joined_selected_df = (
        users.alias("users")
        .join(
            other=user_contacts.alias("uc"),
            on=col("users.USER_ID") == col("uc.USER_ID"),
            how="left",
        )
        .join(
            other=user_profiles.alias("up"),
            on=col("users.USER_ID") == col("up.USER_ID"),
            how="left",
        )
        .join(
            other=user_status_history.alias("ush"),
            on=col("users.USER_ID") == col("ush.USER_ID"),
            how="left",
        )
        .join(
            other=user_location_joined.alias("ulj"),
            on=col("users.USER_ID") == col("ulj.USER_ID"),
            how="left",
        )
        .select(
            regexp_replace(col("users.USER_ID"), r"^[0]*", "").alias("USER_ID"),
            col("up.FIRST_NAME").alias("FIRST_NAME"),
            col("up.LAST_NAME").alias("LAST_NAME"),
            col("up.BIRTH_DATE").alias("BIRTH_DATE"),
            col("up.GENDER").alias("GENDER"),
            col("uc.EMAIL").alias("EMAIL"),
            col("uc.PHONE").alias("PHONE"),
            col("ulj.LOCATION_ID").alias("LOCATION_ID"),
            col("ulj.CITY").alias("CITY"),
            col("ulj.STATE").alias("STATE"),
            col("ulj.ZIP_CODE").alias("ZIP_CODE"),
            col("ulj.COUNTRY").alias("COUNTRY"),
        )
    )

    return joined_selected_df


def entry_point_users(spark: SparkSession, env: str, log_level: int) -> None:
    """"""
    logger = set_logger(log_level=log_level)
    logger.info(f"Running entry_point for users silver table.")

    config = get_config(env=env, layer="silver", config_filename="users.json")
    table_name_uc = config["table_name_uc"]
    partition_key = config["partition_key"]

    locations_df = spark.read.table(
        f"databricks_{env}_domain.ecommmerce_bronze.locations"
    )

    user_contacts_df = spark.read.table(
        f"databricks_{env}_domain.ecommmerce_bronze.user_contacts"
    )

    user_locations_df = spark.read.table(
        f"databricks_{env}_domain.ecommmerce_bronze.user_locations"
    ).filter(col("IS_CURRENT"))

    user_profiles_df = spark.read.table(
        f"databricks_{env}_domain.ecommmerce_bronze.user_profiles"
    )

    user_status_history_df = spark.read.table(
        f"databricks_{env}_domain.ecommmerce_bronze.user_status_history"
    ).filter(col("IS_CURRENT"))

    users_mapping_df = spark.read.table(
        f"databricks_{env}_domain.ecommmerce_bronze.users"
    )

    users_silver_df = join_users_data(
        locations=locations_df,
        user_contacts=user_contacts_df,
        user_locations=user_locations_df,
        user_profiles=user_profiles_df,
        user_status_history=user_status_history_df,
        users=users_mapping_df,
    )

    create_table(
        source_data=users_silver_df,
        table_name_uc=table_name_uc,
        partition_key=partition_key,
    )

    optimize_vacuum_table(spark=spark, table_name_uc=table_name_uc)
