variable "region" {
  type        = string
  default     = "polandcentral"
  description = "Region name in which we will deploy all of our resources."
}

variable "resource_group_name_prefix" {
  type        = string
  default     = "rg"
  description = "Prefix of the resource group name that's combined with a random ID so name is unique in our Azure subscription."
}