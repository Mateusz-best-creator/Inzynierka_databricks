from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import coalesce, col, lit, row_number, trim
from pyspark.sql.window import Window

from ecommerce.common_functions.config_utils import get_config, set_logger
from ecommerce.common_functions.delta_utils import create_table, optimize_vacuum_table


def prepare_dim_geography_data(silver_users: DataFrame) -> DataFrame:
    # Filter valid locations and deduplicate unique geographic combinations
    unique_locations = (
        silver_users.filter(col("CITY").isNotNull() & col("STATE").isNotNull())
        .select(
            trim(coalesce(col("ZIP_CODE"), lit(""))).alias("ZIP_CODE"),
            trim(col("CITY")).alias("CITY"),
            trim(col("STATE")).alias("STATE"),
            trim(col("COUNTRY")).alias("COUNTRY"),
        )
        .dropDuplicates(["ZIP_CODE", "CITY", "STATE", "COUNTRY"])
    )

    surrogate_window = Window.orderBy(
        col("COUNTRY"), col("STATE"), col("CITY"), col("ZIP_CODE")
    )

    return unique_locations.select(
        row_number().over(surrogate_window).alias("GEOGRAPHY_KEY"),
        col("ZIP_CODE"),
        col("CITY"),
        col("STATE"),
        col("COUNTRY"),
    )


def entry_point_dim_geography(spark: SparkSession, env: str, log_level: int) -> None:
    logger = set_logger(log_level=log_level)
    logger.info("Running entry_point for gold dim_geography table.")

    config = get_config(env=env, layer="gold", config_filename="dim_geography.json")[0]
    table_name_uc = str(
        config.get("table_name_uc") or config.get("table_name") or "default_table_name"
    )
    partition_key = config.get("partition_key", [])

    silver_users_df = spark.read.table(
        f"databricks_{env}_domain.ecommerce_silver.users"
    )

    dim_geography_df = prepare_dim_geography_data(silver_users=silver_users_df)

    create_table(
        source_data=dim_geography_df,
        table_name_uc=table_name_uc,
        partition_key=partition_key,
    )

    optimize_vacuum_table(spark=spark, table_name_uc=table_name_uc)
