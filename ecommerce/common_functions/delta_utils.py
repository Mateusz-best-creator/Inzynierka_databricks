from pyspark.sql import DataFrame, SparkSession
from ecommerce.common_functions.config_utils import set_logger
from delta.tables import DeltaTable


def create_table(
    source_data: DataFrame,
    table_name_uc: str,
    overwrite_schema: bool = True,
    partition_key: str | None = None,
    path: str | None = None,
    external_table: bool = False,
) -> None:
    """"""
    logger = set_logger()
    partition_key_value = [] if not partition_key else [str(partition_key)]

    if external_table:
        if path is None:
            raise ValueError(
                f"You need to provide external path to create external table."
            )
        (
            source_data.write.format("delta")
            .option("path", path)
            .option("overwriteSchema", overwrite_schema)
            .mode("overwrite")
            .partitionBy(*partition_key_value)
            .saveAsTable(table_name_uc)
        )
        logger.info(f"Creating external delta table ath path: {path}.")
    else:
        (
            source_data.write.format("delta")
            .option("overwriteSchema", overwrite_schema)
            .mode("overwrite")
            .partitionBy(*partition_key_value)
            .saveAsTable(table_name_uc)
        )
        logger.info("Creating managed delta table.")

    logger.info(f"Created table: {table_name_uc}")


def optimize_vacuum_table(
    spark: SparkSession, table_name_uc: str, retention_hours: int = 168
) -> None:
    """"""
    table = DeltaTable.forName(spark, table_name_uc)
    table.optimize().executeCompaction()
    table.vacuum(retentionHours=retention_hours)
    logger = set_logger()
    logger.info(
        f"Optimized and vacuumed table: {table_name_uc}, retention hours: {retention_hours}"
    )
