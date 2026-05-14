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

# Create the cluster with the "smallest" amount of resources allowed.
data "databricks_node_type" "smallest" {
  local_disk = true

  category = "General Purpose"
  min_cores = 1
  min_memory_gb = 8
}

# Use the latest Databricks Runtime Long Term Support (LTS) version.
data "databricks_spark_version" "latest_lts" {
  long_term_support = true
}


# resource "databricks_cluster" "cluster" {
#   cluster_name            = var.cluster_name
#   node_type_id            = data.databricks_node_type.smallest.id
#   spark_version           = data.databricks_spark_version.latest_lts.id
#   autotermination_minutes = var.cluster_autotermination_minutes

#   # We will create autoscaling cluster
#   autoscale {
#     min_workers = var.cluster_min_num_workers
#     max_workers = var.cluster_max_num_workers
#   }
#   # We will use spot instances for the workers to minimize costs, but for the driver we will always use on-demand instance
#   azure_attributes {
#     availability       = "SPOT_WITH_FALLBACK_AZURE"
#     first_on_demand    = 1
#   }
#   # To minimize costs we disable the option to run jobs, we will use job cluster for thta, 
#   # not this interactive cluster for that, job cluster is simply cheaper.
#   workload_type {
#     clients {
#       jobs      = false
#       notebooks = true
#     }
#   }

#   custom_tags = {
#     type: "interactive"
#     project: "eng_thesis_e_commerce"
#   }
# }
