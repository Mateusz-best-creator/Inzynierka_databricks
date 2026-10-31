from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, coalesce, lit, row_number, when
from pyspark.sql.window import Window

from ecommerce.common_functions.config_utils import get_config, set_logger
from ecommerce.common_functions.delta_utils import create_table, optimize_vacuum_table


def prepare_dim_payment_type_data(silver_payments: DataFrame) -> DataFrame:
    payment_with_tier = silver_payments.withColumn(
        "installment_tier",
        when(coalesce(col("PAYMENT_INSTALLMENTS"), lit(1)) <= 1, lit("Single Payment"))
        .when(col("PAYMENT_INSTALLMENTS").between(2, 3), lit("2-3 Installments"))
        .otherwise(lit("4+ Installments")),
    )

    # Extract distinct combinations of payment types and their respective installment tiers
    unique_payments = payment_with_tier.select(
        col("PAYMENT_TYPE").alias("payment_type_name"), col("installment_tier")
    ).dropDuplicates(["payment_type_name", "installment_tier"])

    surrogate_window = Window.orderBy("payment_type_name", "installment_tier")

    return unique_payments.select(
        row_number().over(surrogate_window).alias("PAYMENT_TYPE_KEY"),
        col("payment_type_name").alias("PAYMENT_TYPE_NAME"),
        col("installment_tier").alias("INSTALLMENT_TIER"),
    )


def entry_point_dim_payment_type(spark: SparkSession, env: str, log_level: int) -> None:
    logger = set_logger(log_level=log_level)
    logger.info("Running entry_point for gold dim_payment_type table.")

    config = get_config(env=env, layer="gold", config_filename="dim_payment_type.json")[
        0
    ]
    table_name_uc = str(
        config.get("table_name_uc") or config.get("table_name") or "dim_payment_type"
    )
    partition_key = config.get("partition_key", [])

    silver_payments_df = spark.read.table(
        f"databricks_{env}_domain.ecommerce_silver.payments"
    )

    dim_payment_type_df = prepare_dim_payment_type_data(
        silver_payments=silver_payments_df
    )

    create_table(
        source_data=dim_payment_type_df,
        table_name_uc=table_name_uc,
        partition_key=partition_key,
    )

    optimize_vacuum_table(spark=spark, table_name_uc=table_name_uc)
