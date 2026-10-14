from pyspark.sql import SparkSession
import json
import logging
import os
from pathlib import Path


def read_json(config_filename: str) -> dict:
    """"""
    with open(config_filename) as f:
        content = json.load(f)
    return content


def set_logger(log_level: int = logging.INFO) -> logging.Logger:
    """"""
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
    path = Path(start_path).resolve()
    while not (path / marker).exists():
        if path.parent == path:
            raise ValueError(f"Marker file not found: {marker}")
        path = path.parent
    return str(path)


def get_config_path(root_path: str, layer: str, env: str, config_filename: str):
    return os.path.join(
        root_path, "ecommerce", "table_properties", env, layer, config_filename
    )


def get_default_config_path(root_path: str, config_filename: str):
    return os.path.join(root_path, "ecommerce", "table_properties", config_filename)


def get_bronze_config(
    env: str, layer: str, file_config: dict, default_config: dict
) -> dict:
    """"""
    storage_type = file_config["storage_type"]
    bronze_schema = default_config["all_layers"]["bronze_schema"]
    endpoint = default_config["all_layers"]["endpoint"]
    blob_file_system_azure = default_config["all_layers"]["blob_file_system_azure"]
    catalog = default_config[layer][env]["catalog"]
    external_container = default_config[layer][env]["external_container"]
    external_storage_account = default_config[layer][env]["external_storage_account"]

    container_name = file_config["container_name"]
    storage_account_name = file_config["storage_account_name"]
    container_folder_path = file_config["container_folder_path"]
    file_name = file_config["file_name"]
    partition_key = file_config.get("partition_key", None)
    table_name = file_config["table_name"]
    table_name_uc = f"{catalog}.{bronze_schema}.{table_name}"
    schema = file_config["schema"]

    if storage_type == "azure_storage_account":
        file_cloud_loc = f"{blob_file_system_azure}://{container_name}@{storage_account_name}.{endpoint}/{container_folder_path}/{file_name}"

    table_cloud_loc = f"{blob_file_system_azure}://{external_container}@{external_storage_account}.{endpoint}/{table_name}"

    return {
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
    }


def get_config(env: str, layer: str, config_filename: str) -> dict:
    """"""

    root_project_path = get_root_path(__name__)
    config_path = get_config_path(root_project_path, layer, env, config_filename)
    default_config_path = get_default_config_path(
        root_project_path, "default_properties.json"
    )

    logger = set_logger()

    json_configuration: dict = read_json(config_path)
    default_json_configuration: dict = read_json(default_config_path)

    if layer == "bronze":
        config = get_bronze_config(
            env=env,
            layer=layer,
            file_config=json_configuration,
            default_config=default_json_configuration,
        )
    # else:
    #     config = get_silver_gold_config(json_configuration, default_json_configuration)

    return config
