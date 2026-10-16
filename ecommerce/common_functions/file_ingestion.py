from pandas import read_excel as pd_read_excel
import io
from typing import Any

from azure.storage.blob import BlobServiceClient

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
    add_ingestion_timestamp_column,
)

from pyspark.sql.functions import lit


def read_csv_file(spark: SparkSession, schema: StructType, path: str) -> DataFrame:
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("sep", ",")
        .schema(schema)
        .load(path)
    )


def read_json_file(spark: SparkSession, schema: StructType, path: str) -> DataFrame:
    return (
        spark.read.format("json").option("multiline", "true").schema(schema).load(path)
    )


def read_parquet_file(spark: SparkSession, schema: StructType, path: str) -> DataFrame:
    return spark.read.format("parquet").schema(schema).load(path)


def read_excel_file(
    spark: SparkSession,
    dbutils: Any,
    schema: StructType,
    excel_properties: dict,
) -> DataFrame:
    """"""

    STORAGE_ACCOUNT_NAME = excel_properties["STORAGE_ACCOUNT_NAME"]
    ACCESS_KEY = dbutils.secrets.get(scope="secret-keys", key="storageaccountaccesskey")
    CONTAINER_NAME = excel_properties["CONTAINER_NAME"]
    BLOB_NAME = excel_properties["BLOB_NAME"]

    CONNECTION_STRING = (
        f"DefaultEndpointsProtocol=https;"
        f"AccountName={STORAGE_ACCOUNT_NAME};"
        f"AccountKey={ACCESS_KEY};"
        f"EndpointSuffix=core.windows.net"
    )

    blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)

    blob_client = blob_service_client.get_blob_client(
        container=CONTAINER_NAME, blob=BLOB_NAME
    )
    stream = io.BytesIO(blob_client.download_blob().readall())

    df = pd_read_excel(stream)
    return spark.createDataFrame(data=df, schema=schema)


def read_file(
    spark: SparkSession,
    dbutils: Any,
    file_cloud_loc: str,
    schema: dict,
    file_format: str,
    excel_properties: dict | None = None,
) -> DataFrame:
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

    if file_format == "csv":
        df = read_csv_file(spark=spark, schema=final_schema, path=file_cloud_loc)
    elif file_format == "json":
        df = read_json_file(spark=spark, schema=final_schema, path=file_cloud_loc)
    elif file_format == "parquet":
        df = read_parquet_file(spark=spark, schema=final_schema, path=file_cloud_loc)
    elif file_format == "excel":
        if not excel_properties:
            raise ValueError(
                f"Properties needed for reading excel files are not specified."
            )

        df = read_excel_file(
            spark=spark,
            dbutils=dbutils,
            schema=final_schema,
            excel_properties=excel_properties,
        )
    else:
        raise ValueError(f"Invalid file format specified: {file_format}")

    for struct in schema:
        df = df.withColumnRenamed(struct["source_col_name"], struct["target_col_name"])

    return df


def entry_point_file_ingestion(
    spark: SparkSession, dbutils: Any, env: str, layer: str, config_filename: str
) -> None:
    """"""
    logger = set_logger()

    env = env.lower()
    layer = layer.lower()
    config_filename = config_filename.lower()

    config_list = get_config(env=env, layer=layer, config_filename=config_filename)

    for config_set in config_list:
        table_name_uc = config_set["table_name_uc"]
        logger.info(f"Ingestion of {layer} table: {table_name_uc}")

        file_cloud_loc = config_set["file_cloud_loc"]
        # Path if we would like to create an external table
        # table_cloud_loc = config_set["table_cloud_loc"]
        partition_key = config_set["partition_key"]
        file_format = config_set["file_format"]
        excel_properties = None
        filename = config_set["filename"]
        if file_format == "excel":
            excel_properties = config_set["excel_properties"]

        df = read_file(
            spark=spark,
            dbutils=dbutils,
            file_cloud_loc=file_cloud_loc,
            schema=config_set["schema"],
            file_format=file_format,
            excel_properties=excel_properties,
        )

        df = add_ingestion_timestamp_column(df)

        df = df.withColumn("INGESTION_FILENAME", lit(filename))

        create_table(
            source_data=df,
            table_name_uc=table_name_uc,
            partition_key=partition_key,
        )

        optimize_vacuum_table(spark=spark, table_name_uc=table_name_uc)
