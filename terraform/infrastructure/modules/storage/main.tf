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

resource "azurerm_storage_container" "data" {
  name                  = "data"
  storage_account_id    = azurerm_storage_account.data.id
  container_access_type = "private"
}
