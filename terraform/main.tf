
resource "azurerm_resource_group" "thesis_rg" {
  location = var.region
  name     = "${var.resource_group_name_prefix}_engthesis_gjnb4"
}

resource "azurerm_storage_account" "tfstate" {
  name                     = "tfstategjnb4"
  resource_group_name      = azurerm_resource_group.thesis_rg.name
  location                 = azurerm_resource_group.thesis_rg.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  allow_nested_items_to_be_public = false

  tags = {
    environment = "dev"
  }
}

resource "azurerm_storage_container" "tfstate" {
  name                  = "tfstate"
  storage_account_id = azurerm_storage_account.tfstate.id
  container_access_type = "private"
}