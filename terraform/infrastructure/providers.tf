terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.0"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.0"
    }
  }

  backend "azurerm" {
    resource_group_name  = "eng_state_quzcn"
    storage_account_name = "tfstateryajhl1n"
    container_name       = "tfstate"
    key                  = "terraform.tfstate"
  }
}

provider "azurerm" {
  features {}
}

provider "databricks" {
  host = module.databricks.databricks_host
}
