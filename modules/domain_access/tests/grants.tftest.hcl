# Plan-only tests of the permission logic (docs/PLAN.md §9.1), against mocked providers.

mock_provider "azurerm" {
  mock_resource "azurerm_user_assigned_identity" {
    override_during = plan
    defaults = {
      id           = "/subscriptions/s/resourceGroups/rg/providers/Microsoft.ManagedIdentity/userAssignedIdentities/id-cl-sales-dev"
      client_id    = "11111111-1111-1111-1111-111111111111"
      principal_id = "22222222-2222-2222-2222-222222222222"
    }
  }
}
mock_provider "databricks" {}
mock_provider "snowflake" {}
mock_provider "fabric" {}

variables {
  name                     = "sales"
  environment              = "dev"
  location                 = "westeurope"
  resource_group_name      = "rg-mpo-dev"
  tenant_id                = "33333333-3333-3333-3333-333333333333"
  acr_id                   = "/subscriptions/s/resourceGroups/rg/providers/Microsoft.ContainerRegistry/registries/acr"
  compute_log_container_id = "/subscriptions/s/resourceGroups/rg/providers/Microsoft.Storage/storageAccounts/st/blobServices/default/containers/compute-logs"
  identity_operator_principal_ids = {
    daemon       = "44444444-4444-4444-4444-444444444444"
    app_deployer = "55555555-5555-5555-5555-555555555555"
  }
  access = {
    databricks = { schemas = [
      { name = "main.silver", access = "write" },
      { name = "main.gold", access = "read" },
    ] }
    fabric = { workspaces = [
      { id = "aaaaaaaa-0000-0000-0000-000000000001", access = "write" },
      { id = "bbbbbbbb-0000-0000-0000-000000000002", access = "read" },
    ] }
    snowflake = {
      warehouse = "SALES_WH"
      schemas = [
        { name = "SALES.MART", access = "write" },
        { name = "SALES.EXT", access = "read" },
      ]
    }
  }
}

run "read_access_never_writes" {
  command = plan

  assert {
    condition     = !contains(databricks_grant.schema["main.gold"].privileges, "MODIFY")
    error_message = "A read schema must not get MODIFY."
  }
  assert {
    condition     = contains(databricks_grant.schema["main.silver"].privileges, "MODIFY")
    error_message = "A write schema needs MODIFY."
  }
  assert {
    condition     = fabric_workspace_role_assignment.this["bbbbbbbb-0000-0000-0000-000000000002"].role == "Viewer"
    error_message = "A read workspace must get Viewer."
  }
  assert {
    condition     = fabric_workspace_role_assignment.this["aaaaaaaa-0000-0000-0000-000000000001"].role == "Contributor"
    error_message = "A write workspace gets Contributor."
  }
  assert {
    condition     = snowflake_grant_privileges_to_account_role.tables["SALES.EXT"].privileges == toset(["SELECT"])
    error_message = "A read Snowflake schema gets SELECT only."
  }
  assert {
    condition     = snowflake_grant_privileges_to_account_role.future_tables["SALES.EXT"].privileges == toset(["SELECT"])
    error_message = "Future tables in a read schema get SELECT only."
  }
  assert {
    condition     = snowflake_grant_privileges_to_account_role.schema["SALES.EXT"].privileges == toset(["USAGE"])
    error_message = "A read Snowflake schema must not allow CREATE."
  }
}

run "grants_only_what_the_file_lists" {
  command = plan

  assert {
    condition     = toset(keys(databricks_grant.schema)) == toset(["main.gold", "main.silver"]) && toset(keys(databricks_grant.catalog)) == toset(["main"])
    error_message = "Databricks grants must match the listed schemas and their catalogs."
  }
  assert {
    condition     = toset(keys(snowflake_grant_privileges_to_account_role.schema)) == toset(["SALES.EXT", "SALES.MART"]) && toset(keys(snowflake_grant_privileges_to_account_role.database)) == toset(["SALES"])
    error_message = "Snowflake grants must match the listed schemas and their databases."
  }
  assert {
    condition     = toset(keys(fabric_workspace_role_assignment.this)) == toset(["bbbbbbbb-0000-0000-0000-000000000002", "aaaaaaaa-0000-0000-0000-000000000001"])
    error_message = "Fabric roles must match the listed workspaces."
  }
}

run "domain_identity_gets_no_azure_admin_rights" {
  command = plan

  assert {
    condition     = azurerm_role_assignment.acr_pull.role_definition_name == "AcrPull" && azurerm_role_assignment.compute_logs.role_definition_name == "Storage Blob Data Contributor"
    error_message = "The domain identity may only pull images and write run logs."
  }
  assert {
    condition = alltrue([
      for assignment in azurerm_role_assignment.identity_operator :
      assignment.principal_id != azurerm_user_assigned_identity.this.principal_id
      && assignment.scope == azurerm_user_assigned_identity.this.id
    ])
    error_message = "Only the daemon and deployer may attach this identity, and only this identity."
  }
  assert {
    condition     = snowflake_service_user.this[0].default_workload_identity[0].azure[0].subject == azurerm_user_assigned_identity.this.principal_id
    error_message = "The Snowflake user must trust exactly this managed identity."
  }
}

run "unused_platforms_get_nothing" {
  command = plan

  variables {
    access = { fabric = { workspaces = [{ id = "cccccccc-0000-0000-0000-000000000003", access = "read" }] } }
  }

  assert {
    condition     = length(databricks_service_principal.this) == 0 && length(snowflake_service_user.this) == 0
    error_message = "A domain without Databricks or Snowflake must get no user there."
  }
}

run "unknown_access_level_is_refused" {
  command = plan

  variables {
    access = { fabric = { workspaces = [{ id = "cccccccc-0000-0000-0000-000000000003", access = "admin" }] } }
  }

  expect_failures = [var.access]
}
