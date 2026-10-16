from pyspark.sql import SparkSession, DataFrame

from ecommerce.common_functions.config_utils import get_config, set_logger
from ecommerce.common_functions.delta_utils import create_table, optimize_vacuum_table


def prepare_orders_data() -> DataFrame:
    """"""
    pass


def entry_point_orders(spark: SparkSession, env: str, log_level: int) -> None:
    """"""
    logger = set_logger(log_level)
    logger.info(f"Running entry_point for silver order_items table.")

    config = get_config(env=env, layer="silver", config_filename="orders.json")[0]
    table_name_uc = config["table_name_uc"]
    partition_key = config["partition_key"]

    df = prepare_orders_data()

    create_table(
        source_data=df, table_name_uc=table_name_uc, partition_key=partition_key
    )

    optimize_vacuum_table(spark=spark, table_name_uc=table_name_uc)
