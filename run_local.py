import json
import os
import shutil
import sys
from pathlib import Path

# --- 1. KONFIGURACJA ŚRODOWISKA WINDOWS (JAVA 17 + HADOOP WINUTILS) ---
jdk17_path = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.20.101-hotspot"
os.environ["JAVA_HOME"] = jdk17_path
os.environ["PATH"] = (
    os.path.join(jdk17_path, "bin") + os.pathsep + os.environ.get("PATH", "")
)

hadoop_home = os.path.abspath("./.hadoop")
if os.path.exists(hadoop_home):
    os.environ["HADOOP_HOME"] = hadoop_home
    os.environ["PATH"] = (
        os.path.join(hadoop_home, "bin") + os.pathsep + os.environ["PATH"]
    )

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from ecommerce.common_functions.config_utils import read_json
from ecommerce.common_functions.delta_utils import add_ingestion_timestamp_column

from pyspark.sql import DataFrame, DataFrameReader, DataFrameWriter
import ecommerce.common_functions.delta_utils as delta_utils

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, DataFrameReader, DataFrameWriter

# Import Silver layer functions
from ecommerce.logic_ecommerce.silver.categories import entry_point_categories
from ecommerce.logic_ecommerce.silver.products import entry_point_products
from ecommerce.logic_ecommerce.silver.users import entry_point_users
from ecommerce.logic_ecommerce.silver.orders import entry_point_orders
from ecommerce.logic_ecommerce.silver.order_items import entry_point_order_items
from ecommerce.logic_ecommerce.silver.payments import entry_point_payments
from ecommerce.logic_ecommerce.silver.sessions import entry_point_sessions
from ecommerce.logic_ecommerce.silver.event_clickstream import (
    entry_point_event_clickstream,
)

# Import Gold layer functions
from ecommerce.logic_ecommerce.gold.dim_date import entry_point_dim_date
from ecommerce.logic_ecommerce.gold.dim_time import entry_point_dim_time
from ecommerce.logic_ecommerce.gold.dim_geography import entry_point_dim_geography
from ecommerce.logic_ecommerce.gold.dim_user import entry_point_dim_user
from ecommerce.logic_ecommerce.gold.dim_product import entry_point_dim_product
from ecommerce.logic_ecommerce.gold.dim_session import entry_point_dim_session
from ecommerce.logic_ecommerce.gold.dim_event_type import entry_point_dim_event_type
from ecommerce.logic_ecommerce.gold.dim_payment_type import entry_point_dim_payment_type
from ecommerce.logic_ecommerce.gold.dim_sales_context import (
    entry_point_dim_sales_context,
)
from ecommerce.logic_ecommerce.gold.fact_sales import entry_point_fact_sales
from ecommerce.logic_ecommerce.gold.fact_events import entry_point_fact_events


def setup_local_spark(env: str = "dev") -> SparkSession:
    warehouse_dir = os.path.abspath("./local_warehouse")
    if os.path.exists(warehouse_dir):
        shutil.rmtree(warehouse_dir)

    catalog_prefix = f"databricks_{env}_domain."

    # Patch Spark read/write and DeltaTable.forName to strip Unity Catalog prefix locally
    orig_table = DataFrameReader.table
    DataFrameReader.table = lambda self, name: orig_table(
        self, name.replace(catalog_prefix, "")
    )

    orig_save_as_table = DataFrameWriter.saveAsTable
    DataFrameWriter.saveAsTable = lambda self, name, *a, **kw: orig_save_as_table(
        self, name.replace(catalog_prefix, ""), *a, **kw
    )

    orig_for_name = DeltaTable.forName
    DeltaTable.forName = staticmethod(
        lambda spark_session, table_or_view_name: orig_for_name(
            spark_session, table_or_view_name.replace(catalog_prefix, "")
        )
    )

    builder = (
        SparkSession.builder.appName("EcommerceLocalETL")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.warehouse.dir", warehouse_dir)
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        .config("spark.driver.memory", "4g")
    )

    return configure_spark_with_delta_pip(builder).getOrCreate()


def ensure_gold_configs_exist(env: str = "dev") -> None:
    gold_config_dir = Path(f"./ecommerce/table_properties/{env}/gold")
    gold_config_dir.mkdir(parents=True, exist_ok=True)

    gold_tables = [
        "dim_date",
        "dim_time",
        "dim_geography",
        "dim_user",
        "dim_product",
        "dim_session",
        "dim_event_type",
        "dim_payment_type",
        "dim_sales_context",
        "fact_sales",
        "fact_events",
    ]

    for table in gold_tables:
        cfg_file = gold_config_dir / f"{table}.json"
        if not cfg_file.exists():
            with open(cfg_file, "w", encoding="utf-8") as f:
                json.dump({"table_name": table, "partition_key": []}, f, indent=4)


def mock_bronze_ingestion(
    spark: SparkSession, generated_data_path: str, env: str
) -> None:
    spark.sql("CREATE DATABASE IF NOT EXISTS ecommerce_bronze")
    spark.sql("CREATE DATABASE IF NOT EXISTS ecommerce_silver")
    spark.sql("CREATE DATABASE IF NOT EXISTS ecommerce_gold")

    bronze_props_dir = Path(f"./ecommerce/table_properties/{env}/bronze")
    print("--- INGESTING BRONZE DATA USING TABLE_PROPERTIES ---")

    for json_path in bronze_props_dir.glob("*.json"):
        file_config = read_json(str(json_path))
        for table_name in file_config.get("table_list", []):
            t_cfg = file_config[table_name]
            file_name = t_cfg["file_name"]
            file_format = t_cfg["file_format"].lower()
            schema_list = t_cfg["schema"]

            file_path = os.path.join(generated_data_path, file_name)

            # Fallback if generator added '_table' suffix or saved as parquet instead of json
            if not os.path.exists(file_path):
                base_name = os.path.splitext(file_name)[0]
                for candidate_ext in [f".{file_format}", ".parquet", ".json", ".csv"]:
                    for suffix in ["_table", ""]:
                        alt_name = f"{base_name}{suffix}{candidate_ext}"
                        alt_path = os.path.join(generated_data_path, alt_name)
                        if os.path.exists(alt_path):
                            file_path = alt_path
                            file_name = alt_name
                            file_format = candidate_ext.lstrip(".")
                            break
                    if os.path.exists(file_path):
                        break

            if not os.path.exists(file_path):
                print(
                    f"WARNING: Source file {file_path} not found. Skipping {table_name}."
                )
                continue

            if file_format == "csv":
                df = spark.read.format("csv").option("header", "true").load(file_path)
            elif file_format == "json":
                df = (
                    spark.read.format("json")
                    .option("multiLine", "true")
                    .load(file_path)
                )
                if "_corrupt_record" in df.columns:
                    df = spark.read.format("json").load(file_path)
            else:
                df = spark.read.format(file_format).load(file_path)

            select_exprs = [
                col(col_def["source_col_name"])
                .cast(col_def["target_datatype"])
                .alias(col_def["target_col_name"])
                for col_def in schema_list
            ]
            df = df.select(*select_exprs)
            df = add_ingestion_timestamp_column(df)

            table_uc = f"ecommerce_bronze.{table_name}"
            df.write.format("delta").option("overwriteSchema", "true").mode(
                "overwrite"
            ).saveAsTable(table_uc)
            print(f"Ingested {file_name} -> {table_uc}")


def main() -> None:
    env = "dev"
    log_level = 20

    ensure_gold_configs_exist(env)
    spark = setup_local_spark(env)

    generated_data_path = os.path.abspath(
        "./ecommerce/data_generation/generated_combined_data"
    )
    mock_bronze_ingestion(spark, generated_data_path, env)

    print("\n--- RUNNING SILVER LAYER ---")
    entry_point_categories(spark, env, log_level)
    entry_point_products(spark, env, log_level)
    entry_point_users(spark, env, log_level)
    entry_point_orders(spark, env, log_level)
    entry_point_order_items(spark, env, log_level)
    entry_point_payments(spark, env, log_level)
    entry_point_sessions(spark, env, log_level)
    entry_point_event_clickstream(spark, env, log_level)

    print("\n--- RUNNING GOLD LAYER ---")
    entry_point_dim_date(spark, env, log_level)
    entry_point_dim_time(spark, env, log_level)
    entry_point_dim_geography(spark, env, log_level)
    entry_point_dim_user(spark, env, log_level)
    entry_point_dim_product(spark, env, log_level)
    entry_point_dim_session(spark, env, log_level)
    entry_point_dim_event_type(spark, env, log_level)
    entry_point_dim_payment_type(spark, env, log_level)
    entry_point_dim_sales_context(spark, env, log_level)

    entry_point_fact_sales(spark, env, log_level)
    entry_point_fact_events(spark, env, log_level)

    print("\nPipeline executed successfully! Data is available in ./local_warehouse")


if __name__ == "__main__":
    main()
