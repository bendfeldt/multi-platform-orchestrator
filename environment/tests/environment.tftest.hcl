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
  mock_resource "azurerm_monitor_action_group" {
    override_during = plan
    defaults        = { id = "/subscriptions/00000000-0000-0000-0000-000000000001/resourceGroups/rg-mpo-dev/providers/Microsoft.Insights/actionGroups/ag-mock" }
  }
  mock_resource "azurerm_monitor_data_collection_rule" {
    override_during = plan
    defaults        = { id = "/subscriptions/00000000-0000-0000-0000-000000000001/resourceGroups/rg-mpo-dev/providers/Microsoft.Insights/dataCollectionRules/dcr-mpo-dev-insights", immutable_id = "dcr-00000000000000000000000000000000", logs_ingestion_endpoint = "https://dcr-mpo-dev-insights.westeurope-1.ingest.monitor.azure.com" }
  }
  mock_data "azurerm_key_vault_secret" {
    defaults = { value = "https://example.invalid/hook?sig=from-key-vault" }
  }
  mock_data "azurerm_resource_group" {
    defaults = { id = "/subscriptions/00000000-0000-0000-0000-000000000001/resourceGroups/rg-mpo-dev" }
  }
}
mock_provider "azuread" {}
mock_provider "databricks" {
  mock_resource "databricks_sql_endpoint" {
    override_during = plan
    defaults        = { id = "costwh0000000001" }
  }
}
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
    condition     = toset([for r in azuread_application.ui.app_role : r.value]) == toset(["Dagster.Viewer", "Dagster.Admin", "Dagster.Launcher.example_sales", "Dagster.Editor.example_sales", "Dagster.Launcher.mpo_ops", "Dagster.Editor.mpo_ops"])
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
    condition     = toset(keys(local.contract.identities.domains)) == toset(["example_sales", "mpo_ops"]) && toset(keys(local.contract.platforms.snowflake.domains)) == toset(["example_sales", "mpo_ops"])
    error_message = "Each domain file must appear in the contract."
  }
  assert {
    condition     = local.contract.schema_version == 2 && local.contract.platforms.snowflake.account == "myorg-sales_dev"
    error_message = "Contract shape changed; bump schema_version in both repos."
  }
}

run "ops_location_gets_a_metadata_only_snowflake_role" {
  command = plan

  assert {
    condition     = module.domain["mpo_ops"].snowflake == { role = "MPO_MPO_OPS_DEV", warehouse = "MPO_OPS_WH_DEV" }
    error_message = "mpo_ops gets a Snowflake role for its warehouse only (module tests: no data grants)."
  }
}

run "every_receiver_type_and_route" {
  command = plan

  variables {
    alerts_file = "tests/alerts_all.yaml"
  }

  assert {
    condition = alltrue([
      length(azurerm_monitor_action_group.this["platform"].email_receiver) == 1,
      length(azurerm_monitor_action_group.this["platform"].sms_receiver) == 1,
      length(azurerm_monitor_action_group.this["platform"].voice_receiver) == 1,
      length(azurerm_monitor_action_group.this["platform"].azure_app_push_receiver) == 1,
      length(azurerm_monitor_action_group.this["platform"].arm_role_receiver) == 1,
      length(azurerm_monitor_action_group.this["platform"].event_hub_receiver) == 1,
      length(azurerm_monitor_action_group.this["platform"].itsm_receiver) == 1,
      length(azurerm_monitor_action_group.this["platform"].webhook_receiver) == 2,
      length(azurerm_monitor_action_group.this["platform"].logic_app_receiver) == 1,
      length(azurerm_monitor_action_group.this["platform"].azure_function_receiver) == 1,
      length(azurerm_monitor_action_group.this["platform"].automation_runbook_receiver) == 1,
    ])
    error_message = "Every receiver type in the file must reach the action group."
  }
  assert {
    condition     = toset([for w in azurerm_monitor_action_group.this["platform"].webhook_receiver : length(w.aad_auth)]) == toset([0, 1])
    error_message = "Only the secure webhook uses Entra auth."
  }
  assert {
    condition     = azurerm_monitor_action_group.this["platform"].logic_app_receiver[0].callback_url == "https://example.invalid/hook?sig=from-key-vault"
    error_message = "Keyed URLs must come from Key Vault."
  }
  assert {
    condition     = toset(keys(data.azurerm_key_vault_secret.receiver)) == toset(["platform/webhook/pager", "platform/logic_app/teams", "platform/function/enrich", "platform/runbook/restart"])
    error_message = "Exactly the four keyed receivers read Key Vault."
  }
  assert {
    condition     = toset(keys(azurerm_monitor_scheduled_query_rules_alert_v2.route)) == toset(["0-error", "1-error", "1-warning"])
    error_message = "One rule per route and severity."
  }
  assert {
    condition     = azurerm_monitor_scheduled_query_rules_alert_v2.route["0-error"].severity == 1 && azurerm_monitor_scheduled_query_rules_alert_v2.route["1-warning"].severity == 2
    error_message = "error is Sev1, warning Sev2."
  }
  assert {
    condition = alltrue([
      strcontains(azurerm_monitor_scheduled_query_rules_alert_v2.route["1-error"].criteria[0].query, "| where alert_location in (\"example_sales\")"),
      strcontains(azurerm_monitor_scheduled_query_rules_alert_v2.route["1-error"].criteria[0].query, "| where alert_kind in (\"run_failure\", \"check_failed\")"),
      !strcontains(azurerm_monitor_scheduled_query_rules_alert_v2.route["0-error"].criteria[0].query, "alert_location in"),
    ])
    error_message = "Routes filter by their locations and kinds, and only when given."
  }
  assert {
    condition     = toset(azurerm_monitor_scheduled_query_rules_alert_v2.route["1-error"].action[0].action_groups) == toset([azurerm_monitor_action_group.this["sales"].id])
    error_message = "A route notifies only its own groups."
  }
}

run "alert_rules_read_only_logs_and_cannot_be_faked" {
  command = plan

  assert {
    condition     = azurerm_role_assignment.alerts_logs.role_definition_name == "Log Analytics Reader" && azurerm_role_assignment.alerts_logs.scope == azurerm_log_analytics_workspace.this.id
    error_message = "The alert identity may only read this environment's logs."
  }
  assert {
    condition     = alltrue([for r in azurerm_monitor_scheduled_query_rules_alert_v2.route : r.identity[0].type == "UserAssigned"]) && azurerm_monitor_scheduled_query_rules_alert_v2.health_missing[0].identity[0].type == "UserAssigned"
    error_message = "Rules must query as the alert identity, not as their last editor."
  }
  assert {
    condition     = strcontains(azurerm_monitor_scheduled_query_rules_alert_v2.health_missing[0].criteria[0].query, "ContainerJobName_s == \"dagster-health\"") && azurerm_monitor_scheduled_query_rules_alert_v2.health_missing[0].criteria[0].operator == "LessThan"
    error_message = "Only the health job's own lines may keep the absence alert quiet."
  }
}

run "unknown_receiver_types_are_refused" {
  command = plan

  variables {
    alerts_file = "tests/alerts_bad.yaml"
  }

  expect_failures = [azurerm_monitor_action_group.this["bad"]]
}

run "unknown_kinds_and_odd_location_names_are_refused" {
  command = plan

  variables {
    alerts_file = "tests/alerts_bad_route.yaml"
  }

  expect_failures = [azurerm_monitor_scheduled_query_rules_alert_v2.route["0-error"], azurerm_monitor_scheduled_query_rules_alert_v2.route["0-warning"]]
}

run "ops_rows_go_through_one_rule_only_mpo_ops_may_use" {
  command = plan

  assert {
    condition     = toset(keys(azurerm_log_analytics_workspace_table_custom_log.insights)) == toset(["MpoCost_CL", "MpoInsights_CL"])
    error_message = "The two tables of the cost and insight reports."
  }
  assert {
    condition     = azurerm_monitor_data_collection_rule.insights[0].kind == "Direct" && toset([for f in azurerm_monitor_data_collection_rule.insights[0].data_flow : f.output_stream]) == toset(["Custom-MpoCost_CL", "Custom-MpoInsights_CL"])
    error_message = "A direct rule into exactly those tables."
  }
  assert {
    condition = alltrue([
      azurerm_role_assignment.ops_ingestion[0].role_definition_name == "Monitoring Metrics Publisher",
      azurerm_role_assignment.ops_ingestion[0].scope == azurerm_monitor_data_collection_rule.insights[0].id,
      azurerm_role_assignment.ops_ingestion[0].principal_id == module.domain["mpo_ops"].principal_id,
    ])
    error_message = "Only mpo_ops may send, and only through this rule."
  }
  assert {
    condition     = local.contract.insights.rule_id == "dcr-00000000000000000000000000000000" && startswith(local.contract.insights.ingestion_endpoint, "https://")
    error_message = "The contract must hand mpo_ops the rule's endpoint and immutable ID."
  }
  assert {
    condition     = length(local.workbook_queries) == 4 && strcontains(azurerm_application_insights_workbook.insights[0].data_json, "MpoCost_CL")
    error_message = "The workbook shows the report queries."
  }
}

run "cost_warehouse_is_created_and_only_mpo_ops_may_use_it" {
  command = plan

  assert {
    condition     = databricks_sql_endpoint.cost[0].enable_serverless_compute && databricks_sql_endpoint.cost[0].cluster_size == "2X-Small" && databricks_sql_endpoint.cost[0].auto_stop_mins == 10
    error_message = "A small serverless warehouse that stops soon after the daily query."
  }
  assert {
    condition     = [for a in databricks_permissions.cost_warehouse[0].access_control : a.permission_level] == ["CAN_USE"]
    error_message = "mpo_ops may only use the warehouse."
  }
  assert {
    condition     = local.contract.platforms.databricks.cost_warehouse_id == "costwh0000000001"
    error_message = "The contract names the cost warehouse."
  }
}

run "an_existing_cost_warehouse_is_used_as_given" {
  command = plan

  variables {
    databricks_cost_warehouse_id = "existing123"
  }

  assert {
    condition     = length(databricks_sql_endpoint.cost) == 0 && length(databricks_permissions.cost_warehouse) == 0 && local.contract.platforms.databricks.cost_warehouse_id == "existing123"
    error_message = "With a given warehouse Terraform creates none and changes no permissions."
  }
}

run "fabric_capacity_events_reach_the_contract_as_given" {
  command = plan

  variables {
    fabric_capacity_events = {
      query_uri         = "https://trd-1.z0.kusto.fabric.microsoft.com"
      database          = "capacity"
      cu_hour_price_usd = 0.2
    }
  }

  assert {
    condition     = local.contract.platforms.fabric.capacity_events.database == "capacity" && local.contract.platforms.fabric.capacity_events.cu_hour_price_usd == 0.2
    error_message = "The contract hands mpo_ops the Eventhouse it reads."
  }
}

run "no_fabric_cost_settings_without_an_eventhouse" {
  command = plan

  assert {
    condition     = local.contract.platforms.fabric == null
    error_message = "Without fabric_capacity_events the contract has no Fabric cost settings."
  }
}
