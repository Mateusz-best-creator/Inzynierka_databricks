# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

# MAGIC %run ../../set_root_python_path

# COMMAND ----------

from ecommerce.logic_ecommerce.gold.dim_session import entry_point_dim_session

# COMMAND ----------

dbutils.widgets.dropdown("env", "dev", ["dev"])
env = dbutils.widgets.get("env")

dbutils.widgets.dropdown("log_level", "INFO", ["INFO", "DEBUG", "WARNING", "ERROR"])
log_level = dbutils.widgets.get("log_level")

# COMMAND ----------

entry_point_dim_session(spark=spark, env=env, log_level=log_level)
