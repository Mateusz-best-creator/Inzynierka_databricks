from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    DateType,
    TimestampType,
)

from ecommerce.common_functions.config_utils import get_config, set_logger
from ecommerce.common_functions.delta_utils import (
    create_table,
    optimize_vacuum_table,
)


def read_file(spark: SparkSession, file_cloud_loc: str, schema: dict) -> DataFrame:
    """"""
    final_schema = StructType()
    for struct in schema:
        if struct["target_datatype"] == "string":
            target_datatype = StringType()
        elif struct["target_datatype"] == "integer":
            target_datatype = IntegerType()
        elif struct["target_datatype"] == "double":
            target_datatype = DoubleType()
        elif struct["target_datatype"] == "date":
            target_datatype = DateType()
        elif struct["target_datatype"] == "timestamp":
            target_datatype = TimestampType()
        else:
            raise ValueError(
                f"Invalid target_datatype specified: {struct['target_datatype']}"
            )
        final_schema.add(StructField(struct["source_col_name"], target_datatype, True))

    df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("sep", ",")
        .schema(final_schema)
        .load(file_cloud_loc)
    )

    for struct in schema:
        df = df.withColumnRenamed(struct["source_col_name"], struct["target_col_name"])

    return df


def entry_point_file_ingestion(
    spark: SparkSession, env: str, layer: str, config_filename: str
) -> None:
    """"""
    logger = set_logger()
    logger.info(
        f"Running entry_point for the bronze table with given config: {config_filename}"
    )

    env = env.lower()
    layer = layer.lower()
    config_filename = config_filename.lower()

    config = get_config(env=env, layer=layer, config_filename=config_filename)

    table_name_uc = config["table_name_uc"]
    file_cloud_loc = config["file_cloud_loc"]
    # Path if we would like to create an external table
    # table_cloud_loc = config["table_cloud_loc"]
    partition_key = config["partition_key"]

    df = read_file(spark=spark, file_cloud_loc=file_cloud_loc, schema=config["schema"])

    create_table(
        source_data=df,
        table_name_uc=table_name_uc,
        partition_key=partition_key,
    )

    optimize_vacuum_table(spark=spark, table_name_uc=table_name_uc)
