from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    concat,
    date_format,
    dayofmonth,
    dayofweek,
    expr,
    lit,
    month,
    quarter,
    row_number,
    when,
    year,
)
from pyspark.sql.window import Window

from ecommerce.common_functions.config_utils import get_config, set_logger
from ecommerce.common_functions.delta_utils import create_table, optimize_vacuum_table


def prepare_dim_date_data(
    spark: SparkSession,
    start_date: str = "2022-01-01",
    end_date: str = "2026-12-31",
) -> DataFrame:
    # Generate a continuous daily date sequence without reading source tables
    dates_df = spark.sql(
        f"SELECT explode(sequence(to_date('{start_date}'), to_date('{end_date}'), interval 1 day)) AS full_date"
    )

    # Spark dayofweek returns 1 for Sunday and 7 for Saturday.
    # Shift values so Monday = 1 and Sunday = 7 to match SQL SET DATEFIRST 1 logic.
    iso_day_of_week = ((dayofweek(col("full_date")) + 5) % 7) + 1
    surrogate_window = Window.orderBy(col("full_date"))

    return dates_df.select(
        row_number().over(surrogate_window).alias("DATE_KEY"),
        col("full_date").alias("FULL_DATE"),
        dayofmonth(col("full_date")).alias("DAY"),
        month(col("full_date")).alias("MONTH"),
        concat(lit("Q"), quarter(col("full_date"))).alias("QUARTER"),
        year(col("full_date")).alias("YEAR"),
        date_format(col("full_date"), "MMMM").alias("MONTH_NAME"),
        dayofmonth(col("full_date")).alias("DAY_OF_MONTH"),
        iso_day_of_week.alias("DAY_OF_WEEK"),
        when(iso_day_of_week.isin([6, 7]), lit(1))
        .otherwise(lit(0))
        .alias("IS_WEEKEND"),
    )


def entry_point_dim_date(spark: SparkSession, env: str, log_level: int) -> None:
    logger = set_logger(log_level=log_level)
    logger.info("Running entry_point for gold dim_date table.")

    config = get_config(env=env, layer="gold", config_filename="dim_date.json")[0]
    table_name_uc = str(
        config.get("table_name_uc") or config.get("table_name") or "default_table_name"
    )
    partition_key = config.get("partition_key", [])

    dim_date_df = prepare_dim_date_data(spark=spark)

    create_table(
        source_data=dim_date_df,
        table_name_uc=table_name_uc,
        partition_key=partition_key,
    )

    optimize_vacuum_table(spark=spark, table_name_uc=table_name_uc)
