# We keep this block so the module knows where to find the provider when we add the cluster
terraform {
  required_providers {
    databricks = {
      source = "databricks/databricks"
    }
  }
}

# Create the Azure Databricks Workspace only
resource "azurerm_databricks_workspace" "dev" {
  name                = "${var.databricks_workspace_name}_jfv8j2-workspace"
  resource_group_name = var.rg_name
  location            = var.region
  sku                 = "premium"

  # Managed resource group for internal Databricks resources
  managed_resource_group_name = "${var.rg_name}-db-managed-jfv8j2"
}

resource "time_sleep" "wait_5_minutes" {
  depends_on = [azurerm_databricks_workspace.dev]

  create_duration = "300s"
}

resource "databricks_cluster" "cluster" {
  # Use it to prevent race conditions when databricks workspace is initializing
  # we still need to wait some time before creating a cluster
  depends_on = [time_sleep.wait_5_minutes]

  cluster_name            = var.cluster_name
  node_type_id            = "Standard_D4ds_v4"
  spark_version           = "16.4.x-scala2.13"
  autotermination_minutes = var.cluster_autotermination_minutes

  # To meet quota limits we will use single node, without autoscaling
  kind           = "CLASSIC_PREVIEW"
  is_single_node = true

  # We will use spot instances for the workers to minimize costs
  azure_attributes {
    availability = "SPOT_WITH_FALLBACK_AZURE"
  }
  # To minimize costs we disable the option to run jobs, we will use job cluster for that,
  # not this interactive cluster for that, job cluster is simply cheaper.
  workload_type {
    clients {
      jobs      = false
      notebooks = true
    }
  }

  custom_tags = {
    type : "interactive"
    project : "eng_thesis_e_commerce"
  }
}
