from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    hour,
    lead,
    minute,
    row_number,
    second,
    to_date,
    unix_timestamp,
    coalesce,
    current_timestamp,
)
from pyspark.sql.window import Window

from ecommerce.common_functions.config_utils import get_config, set_logger
from ecommerce.common_functions.delta_utils import create_table, optimize_vacuum_table


def prepare_fact_events_data(
    silver_events: DataFrame,
    silver_sessions: DataFrame,
    silver_users: DataFrame,
    dim_date: DataFrame,
    dim_time: DataFrame,
    dim_session: DataFrame,
    dim_user: DataFrame,
    dim_geography: DataFrame,
    dim_product: DataFrame,
    dim_event_type: DataFrame,
) -> DataFrame:
    # 1. Calculate event sequence and time spent on step using window functions
    window_session = Window.partitionBy("SESSION_ID").orderBy("EVENT_TIME")

    events_seq = (
        silver_events.withColumn(
            "EVENT_SEQUENCE_IN_SESSION", row_number().over(window_session)
        )
        .withColumn("next_event_time", lead("EVENT_TIME").over(window_session))
        .withColumn(
            "TIME_ON_STEP_SECONDS",
            (unix_timestamp("next_event_time") - unix_timestamp("EVENT_TIME")).cast(
                "integer"
            ),
        )
        .withColumn("event_date", to_date("EVENT_TIME"))
        .withColumn("event_hour", hour("EVENT_TIME"))
        .withColumn("event_minute", minute("EVENT_TIME"))
        .withColumn("event_second", second("EVENT_TIME"))
    )

    # 2. Enrich events with user surrogate key and geography mapping from sessions and users
    events_enriched = (
        events_seq.alias("e")
        .join(
            silver_sessions.alias("ss"),
            col("e.SESSION_ID") == col("ss.SESSION_ID"),
            "left",
        )
        .join(
            silver_users.alias("su"),
            col("ss.USER_SURROGATE_KEY") == col("su.USER_SURROGATE_KEY"),
            "left",
        )
        .select(
            "e.*",
            col("su.USER_SURROGATE_KEY").alias("raw_user_key"),
            col("su.CITY"),
            col("su.STATE"),
            col("su.COUNTRY"),
            col("su.ZIP_CODE"),
        )
    )

    # 3. Join with all dimension tables to fetch surrogate keys
    fact_df = (
        events_enriched.alias("f")
        .join(dim_date.alias("dd"), col("f.event_date") == col("dd.FULL_DATE"), "left")
        .join(
            dim_time.alias("dt"),
            (col("f.event_hour") == col("dt.HOUR"))
            & (col("f.event_minute") == col("dt.MINUTE"))
            & (col("f.event_second") == col("dt.SECONDS")),
            "left",
        )
        .join(
            dim_session.alias("ds"), col("f.SESSION_ID") == col("ds.SESSION_ID"), "left"
        )
        .join(
            dim_user.alias("du"),
            col("f.raw_user_key") == col("du.USER_SURROGATE_KEY"),
            "left",
        )
        .join(
            dim_geography.alias("dg"),
            (col("f.CITY") == col("dg.CITY"))
            & (col("f.STATE") == col("dg.STATE"))
            & (col("f.COUNTRY") == col("dg.COUNTRY")),
            "left",
        )
        .join(
            dim_event_type.alias("det"),
            col("f.EVENT_TYPE") == col("det.EVENT_TYPE_NAME"),
            "left",
        )
        .join(
            dim_product.alias("dp"),
            (col("f.PRODUCT_ID") == col("dp.PRODUCT_ID"))
            & (col("f.EVENT_TIME") >= col("dp.VALID_FROM"))
            & (
                col("f.EVENT_TIME") <= coalesce(col("dp.VALID_TO"), current_timestamp())
            ),
            "left",
        )
    )

    # 4. Select and map columns for the final fact table
    return fact_df.select(
        col("dd.DATE_KEY").alias("EVENT_DATE_KEY"),
        col("dt.TIME_KEY").alias("EVENT_TIME_KEY"),
        col("ds.SESSION_SURROGATE_KEY"),
        col("du.USER_SURROGATE_KEY"),
        col("dg.GEOGRAPHY_KEY"),
        col("dp.PRODUCT_SURROGATE_KEY"),
        col("det.EVENT_TYPE_KEY"),
        col("f.EVENT_ID"),
        col("f.SESSION_ID"),
        col("f.EVENT_SEQUENCE_IN_SESSION"),
        col("f.TIME_ON_STEP_SECONDS"),
        col("dp.BASE_PRICE").alias("PRODUCT_PRICE_AT_EVENT"),
        col("dp.STOCK_QUANTITY").alias("PRODUCT_STOCK_AT_EVENT"),
    )


def entry_point_fact_events(spark: SparkSession, env: str, log_level: int) -> None:
    logger = set_logger(log_level=log_level)
    logger.info("Running entry_point for gold fact_events table.")

    config = get_config(env=env, layer="gold", config_filename="fact_events.json")[0]
    table_name_uc = str(
        config.get("table_name_uc") or config.get("table_name") or "fact_events"
    )
    partition_key = config.get("partition_key", [])

    # Load Source Tables
    silver_events_df = spark.read.table(
        f"databricks_{env}_domain.ecommerce_silver.event_clickstream"
    )
    silver_sessions_df = spark.read.table(
        f"databricks_{env}_domain.ecommerce_silver.sessions"
    )
    silver_users_df = spark.read.table(
        f"databricks_{env}_domain.ecommerce_silver.users"
    )

    # Load Dimensions
    dim_date_df = spark.read.table(f"databricks_{env}_domain.ecommerce_gold.dim_date")
    dim_time_df = spark.read.table(f"databricks_{env}_domain.ecommerce_gold.dim_time")
    dim_session_df = spark.read.table(
        f"databricks_{env}_domain.ecommerce_gold.dim_session"
    )
    dim_user_df = spark.read.table(f"databricks_{env}_domain.ecommerce_gold.dim_user")
    dim_geography_df = spark.read.table(
        f"databricks_{env}_domain.ecommerce_gold.dim_geography"
    )
    dim_product_df = spark.read.table(
        f"databricks_{env}_domain.ecommerce_gold.dim_product"
    )
    dim_event_type_df = spark.read.table(
        f"databricks_{env}_domain.ecommerce_gold.dim_event_type"
    )

    fact_events_df = prepare_fact_events_data(
        silver_events=silver_events_df,
        silver_sessions=silver_sessions_df,
        silver_users=silver_users_df,
        dim_date=dim_date_df,
        dim_time=dim_time_df,
        dim_session=dim_session_df,
        dim_user=dim_user_df,
        dim_geography=dim_geography_df,
        dim_product=dim_product_df,
        dim_event_type=dim_event_type_df,
    )

    create_table(
        source_data=fact_events_df,
        table_name_uc=table_name_uc,
        partition_key=partition_key,
    )

    optimize_vacuum_table(spark=spark, table_name_uc=table_name_uc)
