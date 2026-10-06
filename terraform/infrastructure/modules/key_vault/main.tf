data "azurerm_client_config" "current" {}


resource "azurerm_key_vault" "secrets_storage" {
  name                        = "kv-secretsbkm3g5"
  location                    = var.region
  resource_group_name         = var.rg_name
  rbac_authorization_enabled  = false
  enabled_for_disk_encryption = true
  tenant_id                   = data.azurerm_client_config.current.tenant_id
  soft_delete_retention_days  = 7
  purge_protection_enabled    = false

  sku_name = "standard"

  access_policy {
    tenant_id = data.azurerm_client_config.current.tenant_id
    object_id = data.azurerm_client_config.current.object_id

    key_permissions = [
      "Get",
      "List",
      "Update",
      "Rotate",
      "Create",
      "Import",
    ]

    secret_permissions = [
      "Get",
      "List",
      "Set",
    ]

    storage_permissions = [
      "Get",
      "List",
      "Update",
    ]
  }
}
