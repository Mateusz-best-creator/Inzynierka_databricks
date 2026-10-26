from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit, row_number, when
from pyspark.sql.types import IntegerType
from pyspark.sql.window import Window

from ecommerce.common_functions.config_utils import get_config, set_logger
from ecommerce.common_functions.delta_utils import create_table, optimize_vacuum_table


def prepare_dim_time_data(spark: SparkSession) -> DataFrame:
    # Generate all 86,400 seconds of the day via cross join of ranges
    hours_df = spark.range(0, 24).select(col("id").cast(IntegerType()).alias("hour"))
    minutes_df = spark.range(0, 60).select(
        col("id").cast(IntegerType()).alias("minute")
    )
    seconds_df = spark.range(0, 60).select(
        col("id").cast(IntegerType()).alias("seconds")
    )

    time_grid_df = hours_df.crossJoin(minutes_df).crossJoin(seconds_df)

    day_part_expr = (
        when(col("hour").between(6, 11), lit("Morning"))
        .when(col("hour").between(12, 16), lit("Afternoon"))
        .when(col("hour").between(17, 21), lit("Evening"))
        .otherwise(lit("Night"))
    )

    surrogate_window = Window.orderBy(col("hour"), col("minute"), col("seconds"))

    return time_grid_df.select(
        row_number().over(surrogate_window).alias("TIME_KEY"),
        col("hour").alias("HOUR"),
        col("minute").alias("MINUTE"),
        col("seconds").alias("SECONDS"),
        day_part_expr.alias("DAY_PART"),
    )


def entry_point_dim_time(spark: SparkSession, env: str, log_level: int) -> None:
    logger = set_logger(log_level=log_level)
    logger.info("Running entry_point for gold dim_time table.")

    config = get_config(env=env, layer="gold", config_filename="dim_time.json")[0]
    table_name_uc = str(
        config.get("table_name_uc") or config.get("table_name") or "default_table_name"
    )
    partition_key = config.get("partition_key", [])

    dim_time_df = prepare_dim_time_data(spark=spark)

    create_table(
        source_data=dim_time_df,
        table_name_uc=table_name_uc,
        partition_key=partition_key,
    )

    optimize_vacuum_table(spark=spark, table_name_uc=table_name_uc)
