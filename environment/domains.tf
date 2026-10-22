# One domain per file in domains/<env>/; the file name is the code location's name.
locals {
  domain_dir = "${path.module}/../domains/${var.environment}"
  domains = {
    for file in fileset(local.domain_dir, "*.yaml") :
    trimsuffix(file, ".yaml") => yamldecode(file("${local.domain_dir}/${file}"))
  }
}

module "domain" {
  source   = "../modules/domain_access"
  for_each = local.domains

  name                     = each.key
  environment              = var.environment
  location                 = var.location
  resource_group_name      = var.resource_group_name
  tenant_id                = var.tenant_id
  access                   = each.value
  acr_id                   = azurerm_container_registry.this.id
  compute_log_container_id = azurerm_storage_container.compute_logs.id
  identity_operator_principal_ids = {
    daemon       = azurerm_user_assigned_identity.platform.principal_id
    app_deployer = var.app_deployer_principal_id
  }
}
