output "databricks_host" {
  # This output is used by the provider in the root directory
  value = "https://${azurerm_databricks_workspace.dev.workspace_url}/"
}

# output "cluster_url" {
#  value = databricks_cluster.cluster.url
# }
