output "resource_group_name" {
  value = azurerm_resource_group.main.name
}

output "storage_account_name" {
  value = azurerm_storage_account.main.name
}

output "dashboard_url" {
  value = "https://${azurerm_linux_web_app.streamlit.default_hostname}"
}

output "key_vault_name" {
  value = azurerm_key_vault.main.name
}

output "pipeline_identity_principal_id" {
  value = azurerm_user_assigned_identity.pipeline.principal_id
}
