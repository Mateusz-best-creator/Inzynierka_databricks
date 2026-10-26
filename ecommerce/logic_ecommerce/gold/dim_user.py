from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_date, floor, lit, months_between, when

from ecommerce.common_functions.config_utils import get_config, set_logger
from ecommerce.common_functions.delta_utils import create_table, optimize_vacuum_table


def prepare_dim_user_data(silver_users: DataFrame) -> DataFrame:
    # Calculate age based on birth date and current date
    age_in_years = floor(months_between(current_date(), col("BIRTH_DATE")) / 12)

    # Map age to analytical groups
    age_group_logic = (
        when(col("BIRTH_DATE").isNull(), lit("Unknown"))
        .when(age_in_years.between(18, 24), lit("18-24"))
        .when(age_in_years.between(25, 34), lit("25-34"))
        .when(age_in_years.between(35, 49), lit("35-49"))
        .when(age_in_years >= 50, lit("50+"))
        .otherwise(lit("Unknown"))
    )

    # Determine SCD2 record status
    scd_status_logic = when(col("VALID_TO").isNull(), lit("Current")).otherwise(
        lit("Historical")
    )

    return silver_users.select(
        col("USER_SURROGATE_KEY"),
        col("USER_ID"),
        col("CUSTOMER_UNIQUE_ID"),
        col("FIRST_NAME"),
        col("LAST_NAME"),
        col("EMAIL"),
        col("PHONE"),
        col("BIRTH_DATE"),
        age_group_logic.alias("AGE_GROUP"),
        col("GENDER"),
        col("STATUS"),
        col("CREATED_AT"),
        col("VALID_FROM"),
        col("VALID_TO"),
        scd_status_logic.alias("SCD_RECORD_STATUS"),
    )


def entry_point_dim_user(spark: SparkSession, env: str, log_level: int) -> None:
    logger = set_logger(log_level=log_level)
    logger.info("Running entry_point for gold dim_user table.")

    config = get_config(env=env, layer="gold", config_filename="dim_user.json")[0]
    table_name_uc = str(
        config.get("table_name_uc") or config.get("table_name") or "dim_user"
    )
    partition_key = config.get("partition_key", [])

    silver_users_df = spark.read.table(
        f"databricks_{env}_domain.ecommerce_silver.users"
    )

    dim_user_df = prepare_dim_user_data(silver_users=silver_users_df)

    create_table(
        source_data=dim_user_df,
        table_name_uc=table_name_uc,
        partition_key=partition_key,
    )

    optimize_vacuum_table(spark=spark, table_name_uc=table_name_uc)
