resource "azurerm_resource_group" "thesis_rg" {
  location = var.region
  name     = "engthesis_gjnb4"
}

module "storage" {
  source  = "./modules/storage"
  region  = var.region
  rg_name = azurerm_resource_group.thesis_rg.name
}

module "databricks" {
  source                    = "./modules/databricks"
  databricks_workspace_name = "dev_ecommerce"
  region                    = var.region
  rg_name                   = azurerm_resource_group.thesis_rg.name
}
