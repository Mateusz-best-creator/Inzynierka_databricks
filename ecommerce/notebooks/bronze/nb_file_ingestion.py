# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

# MAGIC %run ../../set_root_python_path

# COMMAND ----------

dbutils.widgets.dropdown("env", "dev", ["dev"])
env = dbutils.widgets.get("env")

dbutils.widgets.dropdown("layer", "bronze", ["bronze", "silver", "gold"])
layer = dbutils.widgets.get("layer")

dbutils.widgets.text("config_filename", "")
config_filename = dbutils.widgets.get("config_filename")

# COMMAND ----------

from ecommerce.common_functions.file_ingestion import entry_point_file_ingestion

# COMMAND ----------

storage_account = "ecommercevj4gm7"
access_key = dbutils.secrets.get(scope="secret-keys", key="storageaccountaccesskey")

spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net", access_key
)

# COMMAND ----------

entry_point_file_ingestion(
    spark=spark, env=env, layer=layer, config_filename=config_filename
)
