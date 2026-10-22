output "identity" {
  description = "The domain identity, as the environment contract publishes it."
  value = {
    resource_id    = azurerm_user_assigned_identity.this.id
    client_id      = azurerm_user_assigned_identity.this.client_id
    principal_name = azurerm_user_assigned_identity.this.name
  }
}

output "principal_id" {
  value = azurerm_user_assigned_identity.this.principal_id
}

output "snowflake" {
  value = var.access.snowflake == null ? null : {
    role      = snowflake_account_role.this[0].name
    warehouse = var.access.snowflake.warehouse
  }
}
