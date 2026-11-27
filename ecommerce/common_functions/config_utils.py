from pyspark.sql import SparkSession
import json
import logging
import os
from pathlib import Path


def read_json(config_filename: str) -> dict:
    """Reads and parses a JSON file into a dictionary.

    Args:
        config_filename: Path to the JSON file to read.

    Returns:
        Parsed JSON content as a dictionary.
    """
    with open(config_filename) as f:
        content = json.load(f)
    return content


def set_logger(log_level: int = logging.INFO) -> logging.Logger:
    """Configures and returns a module-level logger with a stream handler.

    Args:
        log_level: Logging threshold level to apply to the logger and console handler.

    Returns:
        Configured logging.Logger instance.
    """
    logger = logging.getLogger(__name__)
    logger.setLevel(log_level)
    logging.getLogger("py4j").setLevel(logging.WARN)

    if not logger.handlers:
        log_format = (
            "[%(levelname)-8s] %(asctime)s [%(filename)s:%(lineno)d]: %(message)s"
        )

        console_handler = logging.StreamHandler()
        console_handler.setLevel(log_level)

        formatter = logging.Formatter(fmt=log_format)
        console_handler.setFormatter(formatter)

        logger.addHandler(console_handler)

    return logger


def get_root_path(start_path: str, marker: str = "databricks.yml") -> str:
    """Finds the project root directory containing the specified marker file.

    Args:
        start_path: Starting file or directory path from which to search upward.
        marker: Target file or directory name identifying the project root.

    Returns:
        Resolved path to the project root directory as a string.

    Raises:
        ValueError: If marker is not found in start_path or any parent
            directory.
    """
    path = Path(start_path).resolve()
    while not (path / marker).exists():
        if path.parent == path:
            raise ValueError(f"Marker file not found: {marker}")
        path = path.parent
    return str(path)


def get_config_path(root_path: str, layer: str, env: str, config_filename: str):
    """Builds the file path to a layer and environment specific table config.

    Args:
        root_path: Path to the project root directory.
        layer: Data lakehouse layer name (for example bronze, silver or gold).
        env: Target deployment environment (for example dev, test or prod).
        config_filename: Name of the configuration file.

    Returns:
        Full path to the environment and layer specific configuration file.
    """
    return os.path.join(
        root_path, "ecommerce", "table_properties", env, layer, config_filename
    )


def get_default_config_path(root_path: str, config_filename: str):
    """Builds the file path to a shared default table properties config.

    Args:
        root_path: Path to the project root directory.
        config_filename: Name of the default configuration file.

    Returns:
        Full path to the default configuration file.
    """
    return os.path.join(root_path, "ecommerce", "table_properties", config_filename)


def get_bronze_config(
    env: str,
    layer: str,
    file_config: dict,
    default_config: dict,
    logger: logging.Logger,
) -> list[dict]:
    """Builds Unity Catalog and cloud storage configurations for bronze tables.

    Args:
        env: Target deployment environment key used in default_config.
        layer: Data lakehouse layer key used in default_config.
        file_config: Dictionary containing table definitions and source storage settings.
        default_config: Dictionary containing shared schema, endpoint, and environment storage settings.
        logger: Logger instance used to record configuration progress.

    Returns:
        List of dictionaries containing Unity Catalog table names, source and
        target cloud locations, partition keys, schemas, file formats, Excel
        properties, and source filenames.

    Raises:
        ValueError: If a table'ss file_format is not one of csv, json, parquet, or excel.
    """
    table_list = file_config["table_list"]
    config = []

    for table_name in table_list:
        logger.info(f"Processing configuration for table: {table_name}")

        bronze_schema = default_config["all_layers"]["bronze_schema"]
        endpoint = default_config["all_layers"]["endpoint"]
        blob_file_system_azure = default_config["all_layers"]["blob_file_system_azure"]
        catalog = default_config[layer][env]["catalog"]
        external_container = default_config[layer][env]["external_container"]
        external_storage_account = default_config[layer][env][
            "external_storage_account"
        ]

        storage_type = file_config[table_name]["storage_type"]
        container_name = file_config[table_name]["container_name"]
        storage_account_name = file_config[table_name]["storage_account_name"]
        container_folder_path = file_config[table_name]["container_folder_path"]
        file_name = file_config[table_name]["file_name"]
        partition_key = file_config[table_name].get("partition_key", None)
        table_name_uc = f"{catalog}.{bronze_schema}.{table_name}"
        schema = file_config[table_name]["schema"]
        file_format = (file_config[table_name]["file_format"]).lower()

        if storage_type == "azure_storage_account":
            file_cloud_loc = f"{blob_file_system_azure}://{container_name}@{storage_account_name}.{endpoint}/{container_folder_path}/{file_name}"

        table_cloud_loc = f"{blob_file_system_azure}://{external_container}@{external_storage_account}.{endpoint}/{table_name}"

        if file_format not in ["csv", "json", "parquet", "excel"]:
            raise ValueError(f"Specified file format is not supported: {file_format}.")

        excel_properties = None
        if file_format == "excel":
            sheet_name = file_config[table_name]["sheet_name"]
            blob_path = file_config[table_name]["blob_name"]
            excel_properties = {
                "sheet_name": sheet_name,
                "STORAGE_ACCOUNT_NAME": "ecommerce4kllac",
                "CONTAINER_NAME": container_name,
                "BLOB_NAME": blob_path,
            }

        config.append(
            {
                # Table name in UC: catalog.schema.table_name
                "table_name_uc": table_name_uc,
                # File path in storage account from where we download the data
                "file_cloud_loc": file_cloud_loc,
                # External location where table will be saved
                "table_cloud_loc": table_cloud_loc,
                # Optional partition key
                "partition_key": partition_key,
                # Schema of the bronze table
                "schema": schema,
                # File format of the input file
                "file_format": file_format,
                # Special properties for reading excel files
                "excel_properties": excel_properties,
                # Ingestion filename
                "filename": file_name,
            }
        )

    return config


def get_silver_gold_config(
    env: str,
    layer: str,
    json_configuration: dict,
    default_json_configuration: dict,
    logger: logging.Logger,
) -> list[dict]:
    """Builds Unity Catalog table configuration for a silver or gold layer.

    Args:
        env: Target deployment environment key used in default_json_configuration.
        layer: Data lakehouse layer name (such as silver or gold).
        json_configuration: Dictionary containing target table settings and optional partition key.
        default_json_configuration: Dictionary containing catalog and schema defaults by layer and environment.
        logger: Logger instance used to record configuration progress.

    Returns:
        Single element list containing a dictionary with table_name_uc and partition_key.
    """

    catalog = default_json_configuration[layer][env]["catalog"]
    schema = default_json_configuration["all_layers"][f"{layer}_schema"]
    partition_key = json_configuration.get("partition_key", None)
    table_name = json_configuration["table_name"]

    logger.info(f"Processing configuration for {layer} table: {table_name}")

    table_name_uc = f"{catalog}.{schema}.{table_name}"

    return [{"table_name_uc": table_name_uc, "partition_key": partition_key}]


def get_config(env: str, layer: str, config_filename: str) -> list[dict]:
    """Loads table configurations for the requested environment and layer.

    Args:
        env: Target deployment environment (for example, dev or prod).
        layer: Data lakehouse layer name (for example, bronze, silver or gold).
        config_filename: Name of the layer configuration JSON file.

    Returns:
        List of configuration dictionaries generated for the specified layer.
    """

    logger = set_logger()
    root_project_path = get_root_path(__name__)
    config_path = get_config_path(root_project_path, layer, env, config_filename)
    default_config_path = get_default_config_path(
        root_project_path, "default_properties.json"
    )

    json_configuration: dict = read_json(config_path)
    default_json_configuration: dict = read_json(default_config_path)

    if layer == "bronze":
        config = get_bronze_config(
            env=env,
            layer=layer,
            file_config=json_configuration,
            default_config=default_json_configuration,
            logger=logger,
        )
    else:
        config = get_silver_gold_config(
            env=env,
            layer=layer,
            json_configuration=json_configuration,
            default_json_configuration=default_json_configuration,
            logger=logger,
        )

    return config
