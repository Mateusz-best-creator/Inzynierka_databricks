# We keep this block so the module knows where to find the provider if we add the cluster back later
terraform {
  required_providers {
    databricks = {
      source = "databricks/databricks"
    }
  }
}

resource "random_string" "naming" {
  special = false
  upper   = false
  length  = 6
}

# Create the Azure Databricks Workspace only
resource "azurerm_databricks_workspace" "dev" {
  name                = "${var.databricks_workspace_name}_${random_string.naming.result}-workspace"
  resource_group_name = var.rg_name
  location            = var.region
  sku                 = "premium" # Fixed SKU error

  # Managed resource group for internal Databricks resources
  managed_resource_group_name = "${var.rg_name}-db-managed-${random_string.naming.result}"
}