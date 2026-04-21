variable "project_name" {
  type        = string
  description = "Project name prefix."
  default     = "healthcare-data-pipeline"
}

variable "location" {
  type        = string
  description = "Azure region for all resources."
  default     = "eastus2"
}

variable "resource_group_name" {
  type        = string
  description = "Resource group name."
  default     = "rg-healthcare-data-platform"
}

variable "storage_account_name" {
  type        = string
  description = "Globally unique ADLS storage account name."
  default     = "sthealthcareanalytics"
}

variable "key_vault_name" {
  type        = string
  description = "Key Vault name."
  default     = "kv-healthcare-data-platform"
}

variable "subscription_id" {
  type        = string
  description = "Azure subscription id."
}

variable "tenant_id" {
  type        = string
  description = "Azure tenant id."
}

variable "databricks_workspace_url" {
  type        = string
  description = "Azure Databricks workspace URL."
  default     = "https://adb-1234567890123456.7.azuredatabricks.net"
}
