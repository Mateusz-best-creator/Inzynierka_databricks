resource "azurerm_storage_account" "tfstate" {
  name                            = "tfstategjnv3b4o1"
  resource_group_name             = var.rg_name
  location                        = var.region
  account_tier                    = "Standard"
  account_replication_type        = "LRS"
  allow_nested_items_to_be_public = false

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