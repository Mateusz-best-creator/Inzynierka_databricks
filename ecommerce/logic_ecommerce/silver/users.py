from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lead, lit, row_number, when
from pyspark.sql.window import Window

from ecommerce.common_functions.config_utils import get_config, set_logger
from ecommerce.common_functions.delta_utils import create_table, optimize_vacuum_table


def join_users_data(
    locations: DataFrame,
    user_contacts: DataFrame,
    user_locations: DataFrame,
    user_profiles: DataFrame,
    user_status_history: DataFrame,
    users: DataFrame,
) -> DataFrame:
    # Step 1: Collect all unique boundary dates from location history, status history, and user creation
    loc_from = user_locations.select(
        col("user_id"), col("valid_from").alias("timeline_point")
    )
    loc_to = user_locations.filter(col("valid_to").isNotNull()).select(
        col("user_id"), col("valid_to").alias("timeline_point")
    )
    status_from = user_status_history.select(
        col("user_id"), col("valid_from").alias("timeline_point")
    )
    status_to = user_status_history.filter(col("valid_to").isNotNull()).select(
        col("user_id"), col("valid_to").alias("timeline_point")
    )
    user_created = users.select(
        col("user_id"), col("created_at").alias("timeline_point")
    )

    user_timelines = (
        loc_from.union(loc_to)
        .union(status_from)
        .union(status_to)
        .union(user_created)
        .filter(col("timeline_point").isNotNull())
        .distinct()
    )

    # Step 2: Create continuous, non-overlapping time intervals per user
    timeline_window = Window.partitionBy("user_id").orderBy("timeline_point")
    ordered_user_timelines = user_timelines.select(
        col("user_id"),
        col("timeline_point").alias("valid_from"),
        lead("timeline_point").over(timeline_window).alias("valid_to"),
    )

    # Step 3: Join user tables and match location and status active in each time window
    surrogate_window = Window.orderBy(col("ot.user_id"), col("ot.valid_from"))

    joined_users_df = (
        ordered_user_timelines.alias("ot")
        .join(
            other=users.alias("u"),
            on=col("ot.user_id") == col("u.user_id"),
            how="inner",
        )
        .join(
            other=user_profiles.alias("pr"),
            on=col("ot.user_id") == col("pr.user_id"),
            how="left",
        )
        .join(
            other=user_contacts.alias("co"),
            on=col("ot.user_id") == col("co.user_id"),
            how="left",
        )
        .join(
            other=user_locations.alias("ul"),
            on=(col("ot.user_id") == col("ul.user_id"))
            & (col("ot.valid_from") >= col("ul.valid_from"))
            & (
                (col("ot.valid_from") < col("ul.valid_to"))
                | col("ul.valid_to").isNull()
            ),
            how="left",
        )
        .join(
            other=locations.alias("l"),
            on=col("ul.location_id") == col("l.location_id"),
            how="left",
        )
        .join(
            other=user_status_history.alias("s"),
            on=(col("ot.user_id") == col("s.user_id"))
            & (col("ot.valid_from") >= col("s.valid_from"))
            & (
                (col("ot.valid_from") < col("s.valid_to"))
                | col("s.valid_to").isNull()
            ),
            how="left",
        )
        .select(
            row_number().over(surrogate_window).alias("USER_SURROGATE_KEY"),
            col("u.user_id").alias("USER_ID"),
            col("u.customer_unique_id").alias("CUSTOMER_UNIQUE_ID"),
            col("pr.first_name").alias("FIRST_NAME"),
            col("pr.last_name").alias("LAST_NAME"),
            col("co.email").alias("EMAIL"),
            col("co.phone").alias("PHONE"),
            col("pr.birth_date").alias("BIRTH_DATE"),
            col("pr.gender").alias("GENDER"),
            col("l.city").alias("CITY"),
            col("l.state").alias("STATE"),
            col("l.zip_code").alias("ZIP_CODE"),
            col("l.country").alias("COUNTRY"),
            col("s.status").alias("STATUS"),
            col("u.created_at").alias("CREATED_AT"),
            col("ot.valid_from").alias("VALID_FROM"),
            col("ot.valid_to").alias("VALID_TO"),
            when(col("ot.valid_to").isNull(), lit(1))
            .otherwise(lit(0))
            .alias("IS_ACTIVE"),
        )
    )

    return joined_users_df


def entry_point_users(spark: SparkSession, env: str, log_level: int) -> None:
    logger = set_logger(log_level=log_level)
    logger.info("Running entry_point for silver users table.")

    config = get_config(env=env, layer="silver", config_filename="users.json")[0]
    table_name_uc = config["table_name_uc"]
    partition_key = config["partition_key"]

    locations_df = spark.read.table(
        f"databricks_{env}_domain.ecommerce_bronze.locations"
    )
    user_contacts_df = spark.read.table(
        f"databricks_{env}_domain.ecommerce_bronze.user_contacts"
    )
    user_locations_df = spark.read.table(
        f"databricks_{env}_domain.ecommerce_bronze.user_locations"
    )
    user_profiles_df = spark.read.table(
        f"databricks_{env}_domain.ecommerce_bronze.user_profiles"
    )
    user_status_history_df = spark.read.table(
        f"databricks_{env}_domain.ecommerce_bronze.user_status_history"
    )
    users_mapping_df = spark.read.table(
        f"databricks_{env}_domain.ecommerce_bronze.users"
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