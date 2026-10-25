# Plan-only checks of the security settings of one environment, with mocked providers and
# the real domain files of dev.

mock_provider "azurerm" {
  mock_resource "azurerm_container_registry" {
    override_during = plan
    defaults        = { id = "/subscriptions/00000000-0000-0000-0000-000000000001/resourceGroups/rg-mpo-dev/providers/Microsoft.ContainerRegistry/registries/acrmpodevx7k2", login_server = "acrmpodevx7k2.azurecr.io" }
  }
  mock_resource "azurerm_storage_account" {
    override_during = plan
    defaults        = { id = "/subscriptions/00000000-0000-0000-0000-000000000001/resourceGroups/rg-mpo-dev/providers/Microsoft.Storage/storageAccounts/stmpodevx7k2" }
  }
  mock_resource "azurerm_storage_container" {
    override_during = plan
    defaults        = { id = "/subscriptions/00000000-0000-0000-0000-000000000001/resourceGroups/rg-mpo-dev/providers/Microsoft.Storage/storageAccounts/stmpodevx7k2/blobServices/default/containers/compute-logs" }
  }
  mock_resource "azurerm_user_assigned_identity" {
    override_during = plan
    defaults        = { id = "/subscriptions/00000000-0000-0000-0000-000000000001/resourceGroups/rg-mpo-dev/providers/Microsoft.ManagedIdentity/userAssignedIdentities/id-mock", principal_id = "00000000-0000-0000-0000-0000000000f1", client_id = "00000000-0000-0000-0000-0000000000f2" }
  }
  mock_resource "azurerm_virtual_network" {
    override_during = plan
    defaults        = { id = "/subscriptions/00000000-0000-0000-0000-000000000001/resourceGroups/rg-mpo-dev/providers/Microsoft.Network/virtualNetworks/vnet-mpo-dev" }
  }
  mock_resource "azurerm_subnet" {
    override_during = plan
    defaults        = { id = "/subscriptions/00000000-0000-0000-0000-000000000001/resourceGroups/rg-mpo-dev/providers/Microsoft.Network/virtualNetworks/vnet-mpo-dev/subnets/snet" }
  }
  mock_resource "azurerm_private_dns_zone" {
    override_during = plan
    defaults        = { id = "/subscriptions/00000000-0000-0000-0000-000000000001/resourceGroups/rg-mpo-dev/providers/Microsoft.Network/privateDnsZones/mpo-dev.private.postgres.database.azure.com" }
  }
  mock_resource "azurerm_log_analytics_workspace" {
    override_during = plan
    defaults        = { id = "/subscriptions/00000000-0000-0000-0000-000000000001/resourceGroups/rg-mpo-dev/providers/Microsoft.OperationalInsights/workspaces/log-mpo-dev" }
  }
  mock_resource "azurerm_container_app_environment" {
    override_during = plan
    defaults        = { id = "/subscriptions/00000000-0000-0000-0000-000000000001/resourceGroups/rg-mpo-dev/providers/Microsoft.App/managedEnvironments/cae-mpo-dev", default_domain = "internal.example.westeurope.azurecontainerapps.io" }
  }
  mock_resource "azurerm_postgresql_flexible_server" {
    override_during = plan
    defaults        = { id = "/subscriptions/00000000-0000-0000-0000-000000000001/resourceGroups/rg-mpo-dev/providers/Microsoft.DBforPostgreSQL/flexibleServers/psql-mpo-dev-x7k2", fqdn = "psql-mpo-dev-x7k2.postgres.database.azure.com" }
  }
  mock_resource "azurerm_cdn_frontdoor_endpoint" {
    override_during = plan
    defaults        = { id = "/subscriptions/00000000-0000-0000-0000-000000000001/resourceGroups/rg/providers/Microsoft.Cdn/profiles/afd/afdEndpoints/ep", host_name = "ep.azurefd.net" }
  }
  mock_resource "azurerm_cdn_frontdoor_origin_group" {
    override_during = plan
    defaults        = { id = "/subscriptions/00000000-0000-0000-0000-000000000001/resourceGroups/rg/providers/Microsoft.Cdn/profiles/afd/originGroups/og" }
  }
  mock_resource "azurerm_cdn_frontdoor_origin" {
    override_during = plan
    defaults        = { id = "/subscriptions/00000000-0000-0000-0000-000000000001/resourceGroups/rg/providers/Microsoft.Cdn/profiles/afd/originGroups/og/origins/o" }
  }
  mock_resource "azurerm_cdn_frontdoor_firewall_policy" {
    override_during = plan
    defaults        = { id = "/subscriptions/00000000-0000-0000-0000-000000000001/resourceGroups/rg-mpo-dev/providers/Microsoft.Network/frontDoorWebApplicationFirewallPolicies/wafmpodev" }
  }
  mock_resource "azurerm_role_definition" {
    override_during = plan
    defaults        = { role_definition_resource_id = "/subscriptions/00000000-0000-0000-0000-000000000001/providers/Microsoft.Authorization/roleDefinitions/00000000-0000-0000-0000-0000000000aa" }
  }
  mock_data "azurerm_resource_group" {
    defaults = { id = "/subscriptions/00000000-0000-0000-0000-000000000001/resourceGroups/rg-mpo-dev" }
  }
}
mock_provider "azuread" {}
mock_provider "databricks" {}
mock_provider "snowflake" {}
mock_provider "fabric" {}

variables {
  environment                = "dev"
  tenant_id                  = "00000000-0000-0000-0000-000000000000"
  subscription_id            = "00000000-0000-0000-0000-000000000001"
  resource_group_name        = "rg-mpo-dev"
  unique_suffix              = "x7k2"
  frontdoor_profile_id       = "/subscriptions/s/resourceGroups/rg/providers/Microsoft.Cdn/profiles/afd"
  app_deployer_principal_id  = "00000000-0000-0000-0000-00000000000a"
  contract_container_id      = "/subscriptions/s/resourceGroups/rg/providers/Microsoft.Storage/storageAccounts/st/blobServices/default/containers/contract-dev"
  image_policy_definition_id = "/subscriptions/s/providers/Microsoft.Authorization/policyDefinitions/mpo-images"
  admin_group_ids            = ["00000000-0000-0000-0000-00000000000b"]
  viewer_group_ids           = ["00000000-0000-0000-0000-00000000000c"]
  databricks_host            = "https://adb-1.azuredatabricks.net"
  snowflake = {
    organization = "myorg"
    account      = "sales_dev"
    infra_user   = "MPO_INFRA_DEV"
    infra_role   = "MPO_INFRA_DEV"
  }
}

run "nothing_is_reachable_except_through_front_door" {
  command = plan

  assert {
    condition     = azurerm_container_app_environment.this.public_network_access == "Disabled" && azurerm_container_app_environment.this.internal_load_balancer_enabled
    error_message = "The ACA environment must be internal with public access disabled."
  }
  assert {
    condition     = !azurerm_postgresql_flexible_server.this.public_network_access_enabled
    error_message = "Postgres must be reachable from the VNet only."
  }
  assert {
    condition     = azurerm_storage_account.logs.network_rules[0].default_action == "Deny"
    error_message = "Log storage must deny everything but the ACA subnet."
  }
  assert {
    condition     = alltrue([for o in azurerm_cdn_frontdoor_origin.this : o.private_link[0].target_type == "managedEnvironments"])
    error_message = "Front Door must reach the webservers over Private Link."
  }
  assert {
    condition     = alltrue([for g in azurerm_cdn_frontdoor_origin_group.this : length(g.health_probe) == 0])
    error_message = "Health probes must stay off (PLAN R7)."
  }
}

run "no_passwords_or_keys_anywhere" {
  command = plan

  assert {
    condition     = !azurerm_postgresql_flexible_server.this.authentication[0].password_auth_enabled && azurerm_postgresql_flexible_server.this.authentication[0].active_directory_auth_enabled
    error_message = "Postgres must accept Entra logins only."
  }
  assert {
    condition     = !azurerm_storage_account.logs.shared_access_key_enabled
    error_message = "Storage keys must be disabled."
  }
  assert {
    condition     = !azurerm_container_registry.this.admin_enabled
    error_message = "The ACR admin user must be disabled."
  }
}

run "control_plane_rights_stay_narrow" {
  command = plan

  assert {
    condition     = azurerm_role_assignment.webserver_logs.role_definition_name == "Storage Blob Data Reader"
    error_message = "Webservers only read logs."
  }
  assert {
    condition     = toset(azurerm_role_definition.run_cleanup.permissions[0].actions) == toset(["Microsoft.App/jobs/read", "Microsoft.App/jobs/delete", "Microsoft.App/jobs/executions/read"])
    error_message = "The cleanup job may only read and delete Jobs."
  }
  assert {
    condition = alltrue([for a in azurerm_role_definition.app_deployer.permissions[0].actions : startswith(a, "Microsoft.App/") || contains([
      "Microsoft.Resources/subscriptions/resourceGroups/read",
      "Microsoft.ContainerRegistry/registries/read",
      "Microsoft.ContainerRegistry/registries/importImage/action",
    ], a)])
    error_message = "The app deployer may only manage Container Apps and bring images into its registry."
  }
  assert {
    condition     = jsondecode(azurerm_resource_group_policy_assignment.images.parameters).registry.value == "acrmpodevx7k2.azurecr.io"
    error_message = "Images must be limited to this environment's registry."
  }
}

run "roles_go_only_to_the_groups_named_for_them" {
  command = plan

  assert {
    condition = toset(keys(azuread_app_role_assignment.ui)) == toset([
      "Dagster.Viewer/00000000-0000-0000-0000-00000000000c",
      "Dagster.Admin/00000000-0000-0000-0000-00000000000b",
      "Dagster.Launcher.example_sales/00000000-0000-0000-0000-0000000000d1",
      "Dagster.Editor.example_sales/00000000-0000-0000-0000-0000000000d2",
    ])
    error_message = "Each group gets exactly the roles its variable or domain file names."
  }
  assert {
    condition     = toset([for r in azuread_application.ui.app_role : r.value]) == toset(["Dagster.Viewer", "Dagster.Admin", "Dagster.Launcher.example_sales", "Dagster.Editor.example_sales"])
    error_message = "The sign-in app defines the four kinds of roles, per domain where scoped."
  }
  assert {
    condition     = azuread_service_principal.ui.app_role_assignment_required
    error_message = "Only assigned groups may sign in."
  }
}

run "one_public_entry_point" {
  command = plan

  assert {
    condition     = keys(azurerm_cdn_frontdoor_origin.this) == ["ui"] && azurerm_cdn_frontdoor_origin.this["ui"].host_name == "dagster-web.internal.example.westeurope.azurecontainerapps.io"
    error_message = "Front Door must only reach the one webserver."
  }
}

run "contract_lists_every_domain" {
  command = plan

  assert {
    condition     = toset(keys(local.contract.identities.domains)) == toset(["example_sales"]) && toset(keys(local.contract.platforms.snowflake.domains)) == toset(["example_sales"])
    error_message = "Each domain file must appear in the contract."
  }
  assert {
    condition     = local.contract.schema_version == 2 && local.contract.platforms.snowflake.account == "myorg-sales_dev"
    error_message = "Contract shape changed; bump schema_version in both repos."
  }
}
