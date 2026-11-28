# Databricks notebook source
import os
import sys


def get_project_root_directory(path: str):
    """Finds the workspace root directory above the ecommerce folder.
    Args:
        path: Databricks notebook workspace path.
    Returns:
        Parent directory path containing the ecommerce folder.
    Raises:
        Exception: If the ecommerce folder is not found in path.
    """
    folders = path.split(os.sep)
    root_directory = "ecommerce"

    if root_directory in folders:
        root_index = folders.index(root_directory)
        return os.sep.join(folders[:root_index])
    else:
        raise Exception(f"Root directory {root_directory} not found in path {path}")


# COMMAND ----------

current_notebook_path = (
    dbutils.notebook.entry_point.getDbutils()
    .notebook()
    .getContext()
    .notebookPath()
    .get()
)
root_directory = get_project_root_directory(current_notebook_path)
sys.path.append("/Workspace" + root_directory)
