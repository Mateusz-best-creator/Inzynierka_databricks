from pyspark.sql import DataFrame, SparkSession
from ecommerce.common_functions.config_utils import set_logger
from delta.tables import DeltaTable


def create_external_table(
    source_data: DataFrame,
    path: str,
    table_name_uc: str,
    overwrite_schema: bool = True,
    partition_key: str | None = None,
) -> None:
    """"""
    partition_key_value = [] if not partition_key else [str(partition_key)]

    (
        source_data.write.format("delta")
        .option("path", path)
        .option("overwriteSchema", overwrite_schema)
        .partitionBy(*partition_key_value)
        .saveAsTable(table_name_uc)
    )

    logger = set_logger()
    logger.info(f"Created external table: {table_name_uc} on path: {path}")


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
