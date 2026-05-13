variable "region" {}
variable "rg_name" {}
variable "databricks_workspace_name" {}

variable "cluster_name" {
  description = "A name for the cluster."
  type        = string
  default     = "eng_thesis_e_commeerce"
}

variable "cluster_autotermination_minutes" {
  description = "How many minutes before automatically terminating due to inactivity."
  type        = number
  default     = 15
}

variable "cluster_min_num_workers" {
  description = "The minimum number of workers."
  type        = number
  default     = 1
}

variable "cluster_max_num_workers" {
  description = "The maximum number of workers."
  type        = number
  default     = 2
}
