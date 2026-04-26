terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~>4.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~>3.0"
    }
  }

  backend "azurerm" {
      resource_group_name  = "rg_engthesis_gjnb4"
      storage_account_name = "tfstategjnb4"
      container_name       = "tfstate"
      key                  = "terraform.tfstate"
  }

}

provider "azurerm" {
  features {}
}