resource "azurerm_storage_account" "tfstate" {
  name                            = "tfstategjnv3b4o1"
  resource_group_name             = var.rg_name
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

resource "azurerm_storage_management_policy" "example" {
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
      # REMOVED base_blob: The active state file must always stay Hot and Live.

      # Cleans up old state history versions after 30 days to save space/costs
      version {
        delete_after_days_since_creation = 30
      }
    }
  }
}
