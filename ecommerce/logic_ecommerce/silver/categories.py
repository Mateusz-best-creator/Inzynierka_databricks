from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit, row_number, trim, when
from pyspark.sql.window import Window

from ecommerce.common_functions.config_utils import get_config, set_logger
from ecommerce.common_functions.delta_utils import create_table, optimize_vacuum_table


def prepare_categories_data(categories: DataFrame) -> DataFrame:
    # Filter out missing primary keys, trim identifiers, and deduplicate categories
    cleaned_categories = (
        categories.filter(col("category_id").isNotNull())
        .withColumn("category_id_clean", trim(col("category_id")))
        .withColumn(
            "parent_id_clean",
            when(
                col("parent_category_id").isNotNull()
                & (trim(col("parent_category_id")) != ""),
                trim(col("parent_category_id")),
            ).otherwise(lit(None)),
        )
        .dropDuplicates(["category_id_clean"])
    )

    # Verify that parent_category_id exists as a valid category_id to remove orphan links
    valid_parents = cleaned_categories.select(
        col("category_id_clean").alias("valid_parent_id")
    )

    verified_categories = cleaned_categories.join(
        valid_parents,
        cleaned_categories["parent_id_clean"] == valid_parents["valid_parent_id"],
        how="left",
    )

    surrogate_window = Window.orderBy(col("category_id_clean"))

    return verified_categories.select(
        row_number().over(surrogate_window).alias("CATEGORY_SURROGATE_KEY"),
        col("category_id_clean").alias("CATEGORY_ID"),
        col("category_name").alias("CATEGORY_NAME"),
        col("valid_parent_id").alias("PARENT_CATEGORY_ID"),
    )


def entry_point_categories(spark: SparkSession, env: str, log_level: int) -> None:
    logger = set_logger(log_level=log_level)
    logger.info("Running entry_point for silver categories table.")

    config = get_config(env=env, layer="silver", config_filename="categories.json")[0]
    table_name_uc = config["table_name_uc"]
    partition_key = config["partition_key"]

    categories_df = spark.read.table(
        f"databricks_{env}_domain.ecommerce_bronze.categories"
    )

    categories_silver_df = prepare_categories_data(categories=categories_df)

    create_table(
        source_data=categories_silver_df,
        table_name_uc=table_name_uc,
        partition_key=partition_key,
    )

    optimize_vacuum_table(spark=spark, table_name_uc=table_name_uc)
