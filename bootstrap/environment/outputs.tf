output "values" {
  description = "Copy into environment/envs/<env>.tfvars and the CI variables."
  value = {
    resource_group_name        = azurerm_resource_group.this.name
    image_policy_definition_id = azurerm_policy_definition.images.id
    infra_deployer_client_id   = azuread_application.deployer["infra"].client_id
    app_deployer_client_id     = azuread_application.deployer["app"].client_id
    app_deployer_principal_id  = azuread_service_principal.deployer["app"].object_id
  }
}

output "credentials" {
  description = "Federated credentials: which pipeline may log in as which deployer."
  value       = local.credentials
}
