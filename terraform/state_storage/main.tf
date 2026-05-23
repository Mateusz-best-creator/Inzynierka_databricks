provider "azurerm" {
  features {}
}

provider "random" {}

variable "region" {
  type = string
  # Region was chosen to be compliant with the azure students policy: "listOfAllowedLocations"
  default = "norwayeast"
}

resource "random_string" "random_rg_prefix" {
  length           = 5
  special          = false
  upper = false
  numeric = false
}

resource "random_string" "random_sa_prefix" {
  length           = 8
  special          = false
  upper = false
}

resource "azurerm_resource_group" "rg_tf_state" {
  name     = "eng_state_${random_string.random_rg_prefix.result}"
  location = var.region
}

# Implicit dependency there, terraform will know which resources to create first
resource "azurerm_storage_account" "tfstate" {
  name                            = "tfstate${random_string.random_sa_prefix.result}"
  resource_group_name             = azurerm_resource_group.rg_tf_state.name
  location                        = var.region
  account_tier                    = "Standard"
  account_replication_type        = "LRS"
  allow_nested_items_to_be_public = false

    blob_properties {
      versioning_enabled = true
    }

  tags = {
    environment = "dev"
    purpose     = "state_storage"
  }
}

resource "azurerm_storage_container" "tfstate" {
  name                  = "tfstate"
  storage_account_id    = azurerm_storage_account.tfstate.id
  container_access_type = "private"
}

resource "azurerm_storage_management_policy" "state_policy" {
  storage_account_id = azurerm_storage_account.tfstate.id

  rule {
    name    = "tf_state_rules"
    enabled = true
    filters {
      # The prefix inside a container lifecycle policy must just be the blob path/name,
      # it should NOT include the container name prefix itself.
      prefix_match = ["tfstate/terraform.tfstate"]
      blob_types   = ["blockBlob"]
    }

    actions {
      # do not use base_blob because the active state file must always be in hot tier

      version {
        delete_after_days_since_creation = 30
      }
    }
  }
}

output "rg_name" {
  value = azurerm_resource_group.rg_tf_state.name
}

output "sa_name" {
  value = azurerm_storage_account.tfstate.name
}