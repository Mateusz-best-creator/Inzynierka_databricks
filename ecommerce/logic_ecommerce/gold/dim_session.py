from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit, row_number, unix_timestamp, when
from pyspark.sql.window import Window

from ecommerce.common_functions.config_utils import get_config, set_logger
from ecommerce.common_functions.delta_utils import create_table, optimize_vacuum_table


def prepare_dim_session_data(
    silver_sessions: DataFrame, silver_events: DataFrame
) -> DataFrame:
    # Identify sessions that resulted in a conversion
    converted_sessions = (
        silver_events.filter(col("EVENT_TYPE").isin("Purchase", "Checkout"))
        .select("SESSION_ID")
        .dropDuplicates()
        .withColumn("is_converted", lit(1))
    )

    # Join sessions with conversion flags
    sessions_joined = silver_sessions.join(
        converted_sessions, on="SESSION_ID", how="left"
    )

    # Calculate session duration in seconds
    duration_sec = unix_timestamp(col("SESSION_END")) - unix_timestamp(
        col("SESSION_START")
    )

    duration_bucket_logic = (
        when(col("SESSION_END").isNull() | (duration_sec < 10), lit("Bounce (< 10s)"))
        .when(duration_sec.between(10, 59), lit("Brief (10s-60s)"))
        .when(duration_sec.between(60, 300), lit("Engaged (1m-5m)"))
        .otherwise(lit("Extended (> 5m)"))
    )

    conversion_status_logic = when(
        col("is_converted") == 1, lit("Converted (Purchase)")
    ).otherwise(lit("Browsing Only"))

    surrogate_window = Window.orderBy("SESSION_START")

    return sessions_joined.select(
        row_number().over(surrogate_window).alias("SESSION_SURROGATE_KEY"),
        col("SESSION_ID"),
        col("SESSION_START"),
        col("SESSION_END"),
        duration_bucket_logic.alias("SESSION_DURATION_BUCKET"),
        col("DEVICE_TYPE"),
        col("BROWSER"),
        col("TRAFFIC_SOURCE"),
        conversion_status_logic.alias("CONVERSION_STATUS"),
    )


def entry_point_dim_session(spark: SparkSession, env: str, log_level: int) -> None:
    logger = set_logger(log_level=log_level)
    logger.info("Running entry_point for gold dim_session table.")

    config = get_config(env=env, layer="gold", config_filename="dim_session.json")[0]
    table_name_uc = str(
        config.get("table_name_uc") or config.get("table_name") or "dim_session"
    )
    partition_key = config.get("partition_key", [])

    silver_sessions_df = spark.read.table(
        f"databricks_{env}_domain.ecommerce_silver.sessions"
    )
    silver_events_df = spark.read.table(
        f"databricks_{env}_domain.ecommerce_silver.event_clickstream"
    )

    dim_session_df = prepare_dim_session_data(
        silver_sessions=silver_sessions_df, silver_events=silver_events_df
    )

    create_table(
        source_data=dim_session_df,
        table_name_uc=table_name_uc,
        partition_key=partition_key,
    )

    optimize_vacuum_table(spark=spark, table_name_uc=table_name_uc)
