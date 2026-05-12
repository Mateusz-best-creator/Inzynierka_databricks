variable "region" {
  type    = string
  # Region was chosen to be compliant with the azure students policy: "listOfAllowedLocations"
  default = "norwayeast"
}

variable "resource_group_name_prefix" {
  type    = string
  default = "rg"
}