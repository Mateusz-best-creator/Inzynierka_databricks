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

  # backend "azurerm" {
  #   resource_group_name  = "rg_engthesis_gjnb4"
  #   storage_account_name = "tfstategjnv3b4o1"
  #   container_name       = "tfstate"
  #   key                  = "terraform.tfstate"
  # }
}

provider "azurerm" {
  features {}
}

# Comment this out for now since no cluster is being created
# provider "databricks" {
#   host = module.databricks.databricks_host
# }
