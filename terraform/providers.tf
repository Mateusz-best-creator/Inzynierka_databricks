terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.0"
    }
    # Keep the definition but we won't use the provider block below yet
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.0"
    }
  }
}

provider "azurerm" {
  features {}
}

# Comment this out for now since no cluster is being created
# provider "databricks" {
#   host = module.databricks.databricks_host
# }