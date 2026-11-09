from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    abs as spark_abs,
    coalesce,
    col,
    lit,
    row_number,
    trim,
    upper,
    when,
)
from pyspark.sql.types import IntegerType
from pyspark.sql.window import Window

from ecommerce.common_functions.config_utils import get_config, set_logger
from ecommerce.common_functions.delta_utils import create_table, optimize_vacuum_table


def prepare_payments_data(payment_details: DataFrame) -> DataFrame:
    # Sanitize payment attributes, fix invalid installment counts, and deduplicate sequences
    cleaned_payments = (
        payment_details.filter(col("payment_id").isNotNull())
        .withColumn(
            "payment_type_clean",
            when(
                col("payment_type").isNull() | (trim(col("payment_type")) == ""),
                lit("UNKNOWN"),
            ).otherwise(upper(trim(col("payment_type")))),
        )
        .withColumn(
            "seq_clean",
            coalesce(spark_abs(col("payment_sequential").cast(IntegerType())), lit(1)),
        )
        .withColumn(
            "installments_raw",
            coalesce(
                spark_abs(col("payment_installments").cast(IntegerType())), lit(1)
            ),
        )
        .withColumn(
            "installments_clean",
            when(col("payment_type_clean") != "CREDIT_CARD", lit(1))
            .when(col("installments_raw") < 1, lit(1))
            .otherwise(col("installments_raw")),
        )
    )

    # Remove duplicate payment sequences within the same order
    dedup_window = Window.partitionBy("order_id", "seq_clean").orderBy("payment_id")
    deduped_payments = (
        cleaned_payments.withColumn("rn", row_number().over(dedup_window))
        .filter(col("rn") == 1)
        .drop("rn")
    )

    surrogate_window = Window.orderBy(col("payment_id"), col("seq_clean"))

    return deduped_payments.select(
        row_number().over(surrogate_window).alias("PAYMENT_SURROGATE_KEY"),
        col("payment_id").alias("PAYMENT_ID"),
        col("seq_clean").alias("PAYMENT_SEQUENTIAL"),
        col("payment_type_clean").alias("PAYMENT_TYPE"),
        col("installments_clean").alias("PAYMENT_INSTALLMENTS"),
        spark_abs(coalesce(col("payment_value").cast("double"), lit(0.0))).alias(
            "PAYMENT_VALUE"
        ),
        col("order_id").alias("ORDER_ID"),
    )


def entry_point_payments(spark: SparkSession, env: str, log_level: int) -> None:
    logger = set_logger(log_level=log_level)
    logger.info("Running entry_point for silver payments table.")

    config = get_config(env=env, layer="silver", config_filename="payments.json")[0]
    table_name_uc = config["table_name_uc"]
    partition_key = config["partition_key"]

    payment_details_df = spark.read.table(
        f"databricks_{env}_domain.ecommerce_bronze.payment_details"
    )

    payments_silver_df = prepare_payments_data(payment_details=payment_details_df)

    create_table(
        source_data=payments_silver_df,
        table_name_uc=table_name_uc,
        partition_key=partition_key,
    )

    optimize_vacuum_table(spark=spark, table_name_uc=table_name_uc)
