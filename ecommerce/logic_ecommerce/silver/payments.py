from pyspark.sql import SparkSession, DataFrame

from ecommerce.common_functions.config_utils import get_config, set_logger
from ecommerce.common_functions.delta_utils import create_table, optimize_vacuum_table

from pyspark.sql.functions import lit, col, upper, trim


def prepare_payments_data(
    payment_details: DataFrame, payment_status_history: DataFrame
) -> DataFrame:
    """"""
    payment_joined = (
        payment_details.alias("pd")
        .join(
            other=payment_status_history.alias("psh"),
            on=upper(trim(col("pd.payment_id"))) == upper(trim(col("psh.payment_id"))),
            how="left",
        )
        .select(
            col("pd.payment_id").alias("PAYMENT_ID"),
            col("pd.order_id").alias("ORDER_ID"),
            col("pd.payment_sequential").alias("PAYMENT_SEQUENTIAL"),
            col("pd.PAYMENT_TYPE").alias("PAYMENT_TYPE"),
            col("pd.payment_installments").alias("PAYMENT_INSTALLMENTS"),
            col("pd.PAYMENT_VALUE").alias("PAYMENT_VALUE"),
            col("psh.log_id").alias("LOG_ID"),
            col("psh.STATUS").alias("PAYMENT_CURRENT_STATUS"),
            col("psh.GATEWAY_RESPONSE").alias("GATEWAY_RESPONSE"),
            col("psh.VALID_FROM").alias("VALID_FROM"),
        )
    )

    return payment_joined


def entry_point_payments(spark: SparkSession, env: str, log_level: int) -> None:
    """"""
    logger = set_logger(log_level)
    logger.info(f"Running entry_point for silver order_items table.")

    config = get_config(env=env, layer="silver", config_filename="payments.json")[0]
    table_name_uc = config["table_name_uc"]
    partition_key = config["partition_key"]

    payment_details = spark.read.table(
        f"databricks_{env}_domain.ecommerce_bronze.payment_details"
    )
    payment_status_history = spark.read.table(
        f"databricks_{env}_domain.ecommerce_bronze.payment_status_history"
    ).filter(col("IS_CURRENT") == lit(1))

    payments_silver = prepare_payments_data(
        payment_details=payment_details, payment_status_history=payment_status_history
    )

    create_table(
        source_data=payments_silver,
        table_name_uc=table_name_uc,
        partition_key=partition_key,
    )

    optimize_vacuum_table(spark=spark, table_name_uc=table_name_uc)
