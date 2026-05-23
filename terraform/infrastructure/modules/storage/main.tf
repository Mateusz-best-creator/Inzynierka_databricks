resource "azurerm_storage_account" "data" {
  name                            = "ecommercevj4gm7"
  resource_group_name             = var.rg_name
  location                        = var.region
  account_tier                    = "Standard"
  account_replication_type        = "LRS"
  allow_nested_items_to_be_public = false


  tags = {
    environment = "dev"
    purpose     = "data_storage"
  }
}

resource "azurerm_storage_container" "table_data" {
  # We will create one container for each layer
  for_each = toset(["bronze", "silver", "gold"])
  name                  = "${each.key}layer"
  storage_account_id    = azurerm_storage_account.data.id
  container_access_type = "private"
}

resource "azurerm_storage_management_policy" "state_policy" {
  storage_account_id = azurerm_storage_account.data.id

  rule {
    name    = "bronze_ingestion_files_rules"
    enabled = true
    filters {
      prefix_match = ["bronzelayer/"]
      blob_types   = ["blockBlob"]
    }

    actions {
        # Here we define how to store blob data to minimize costs
      base_blob {
        tier_to_cool_after_days_since_modification_greater_than    = 14
        tier_to_archive_after_days_since_modification_greater_than = 30
        delete_after_days_since_modification_greater_than          = 50
      }
      # In case we take snapshots we want to delete them afer one month
      snapshot {
        delete_after_days_since_creation_greater_than = 30
      }
    }
  }
}