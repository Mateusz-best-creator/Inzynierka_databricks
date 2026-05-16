provider "azurerm" {
  features {}
}

variable "region" {
  type = string
  # Region was chosen to be compliant with the azure students policy: "listOfAllowedLocations"
  default = "norwayeast"
}

variable "state_resource_group_name" {
  type    = string
  default = "eng_state_km4s2"
}


resource "azurerm_storage_account" "tfstate" {
  name                            = "tfstategjnv3b4o1"
  resource_group_name             = var.state_resource_group_name
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
      prefix_match = ["terraform.tfstate"]
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
