# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

# MAGIC %run ../../set_root_python_path

# COMMAND ----------

from ecommerce.logic_ecommerce.silver.products import entry_point_products

# COMMAND ----------

# Retrieve runtime parameters from Databricks widgets
dbutils.widgets.dropdown("env", "dev", ["dev"])
env = dbutils.widgets.get("env")

dbutils.widgets.dropdown("log_level", "INFO", ["INFO", "DEBUG", "WARNING", "ERROR"])
log_level = dbutils.widgets.get("log_level")

# COMMAND ----------

# Execute silver products transformation pipeline
entry_point_products(spark=spark, env=env, log_level=log_level)
