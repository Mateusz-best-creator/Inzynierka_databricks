from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, trim, upper, monotonically_increasing_id

from ecommerce.common_functions.config_utils import get_config, set_logger
from ecommerce.common_functions.delta_utils import create_table, optimize_vacuum_table


def prepare_sessions_data(
    sessions: DataFrame, silver_users: DataFrame
) -> DataFrame:
    """Prepare and join sessions data with silver users to resolve user surrogate keys."""
    
    joined_sessions = (
        sessions.alias("s")
        .join(
            silver_users.alias("u"),
            (upper(trim(col("s.USER_ID"))) == upper(trim(col("u.USER_ID"))))
            & (col("s.SESSION_START") >= col("u.VALID_FROM"))
            & ((col("s.SESSION_START") < col("u.VALID_TO")) | col("u.VALID_TO").isNull()),
            how="left",
        )
        .select(
            monotonically_increasing_id().alias("SESSION_SURROGATE_KEY"),
            col("s.SESSION_ID").alias("SESSION_ID"),
            col("s.SESSION_START").alias("SESSION_START"),
            col("s.SESSION_END").alias("SESSION_END"),
            col("s.DEVICE_TYPE").alias("DEVICE_TYPE"),
            col("s.BROWSER").alias("BROWSER"),
            col("s.TRAFFIC_SOURCE").alias("TRAFFIC_SOURCE"),
            col("u.USER_SURROGATE_KEY").alias("USER_SURROGATE_KEY"),
        )
    )

    return joined_sessions


def entry_point_sessions(spark: SparkSession, env: str, log_level: int) -> None:
    """Entry point for silver sessions table ETL."""
    logger = set_logger(log_level)
    logger.info("Running entry_point for silver sessions table.")

    config = get_config(env=env, layer="silver", config_filename="sessions.json")[0]
    table_name_uc = config["table_name"]
    partition_key = config["partition_key"]

    sessions = spark.read.table(f"databricks_{env}_domain.ecommerce_bronze.sessions")
    silver_users = spark.read.table(f"databricks_{env}_domain.ecommerce_silver.silver_users")

    sessions_silver = prepare_sessions_data(
        sessions=sessions,
        silver_users=silver_users,
    )

    create_table(
        source_data=sessions_silver,
        table_name_uc=table_name_uc,
        partition_key=partition_key,
    )

    optimize_vacuum_table(spark=spark, table_name_uc=table_name_uc)