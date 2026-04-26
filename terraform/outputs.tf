output "resource_group_name" {
  description = "Name of the resource group where we deploy our resources."
  value = azurerm_resource_group.thesis_rg.name
}