locals {
  identity_name = "id-cl-${replace(var.name, "_", "-")}-${var.environment}"
  # Snowflake object names: unquoted identifiers, upper case.
  snowflake_name = upper("mpo_${var.name}_${var.environment}")

  databricks_schemas  = { for s in try(var.access.databricks.schemas, []) : s.name => s }
  databricks_catalogs = toset([for s in local.databricks_schemas : split(".", s.name)[0]])
  fabric_workspaces   = { for w in try(var.access.fabric.workspaces, []) : w.id => w }
  snowflake_schemas = {
    for s in try(var.access.snowflake.schemas, []) : s.name => merge(s, {
      database = split(".", s.name)[0]
      quoted   = join(".", [for part in split(".", s.name) : "\"${part}\""])
    })
  }
  snowflake_databases = toset([for s in local.snowflake_schemas : s.database])
}

resource "azurerm_user_assigned_identity" "this" {
  name                = local.identity_name
  location            = var.location
  resource_group_name = var.resource_group_name
}

# ---- Azure: pull its image, write its run logs. Nothing else.

resource "azurerm_role_assignment" "acr_pull" {
  scope                = var.acr_id
  role_definition_name = "AcrPull"
  principal_id         = azurerm_user_assigned_identity.this.principal_id
  principal_type       = "ServicePrincipal"
}

resource "azurerm_role_assignment" "compute_logs" {
  scope                = var.compute_log_container_id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_user_assigned_identity.this.principal_id
  principal_type       = "ServicePrincipal"
}

resource "azurerm_role_assignment" "identity_operator" {
  for_each             = var.identity_operator_principal_ids
  scope                = azurerm_user_assigned_identity.this.id
  role_definition_name = "Managed Identity Operator"
  principal_id         = each.value
  principal_type       = "ServicePrincipal"
}

# ---- Databricks: a workspace service principal with Unity Catalog grants.
# Job permissions are not managed here: databricks_permissions replaces a job's whole ACL.
# Job owners give the service principal CAN_MANAGE_RUN on the jobs the domain runs.

resource "databricks_service_principal" "this" {
  count                = var.access.databricks == null ? 0 : 1
  application_id       = azurerm_user_assigned_identity.this.client_id
  display_name         = local.identity_name
  allow_cluster_create = false
}

resource "databricks_grant" "catalog" {
  for_each   = local.databricks_catalogs
  catalog    = each.key
  principal  = azurerm_user_assigned_identity.this.client_id
  privileges = ["USE_CATALOG"]
  depends_on = [databricks_service_principal.this]
}

resource "databricks_grant" "schema" {
  for_each  = local.databricks_schemas
  schema    = each.key
  principal = azurerm_user_assigned_identity.this.client_id
  privileges = each.value.access == "write" ? [
    "USE_SCHEMA", "SELECT", "MODIFY", "CREATE_TABLE",
  ] : ["USE_SCHEMA", "SELECT"]
  depends_on = [databricks_service_principal.this]
}

# ---- Fabric: a workspace role per workspace.

resource "fabric_workspace_role_assignment" "this" {
  for_each     = local.fabric_workspaces
  workspace_id = each.key
  principal = {
    id   = azurerm_user_assigned_identity.this.principal_id
    type = "ServicePrincipal"
  }
  role = each.value.access == "write" ? "Contributor" : "Viewer"
}

# ---- Snowflake: a service user that trusts exactly this managed identity (workload identity
# federation), with one role holding the domain's grants.

resource "snowflake_account_role" "this" {
  count = var.access.snowflake == null ? 0 : 1
  name  = local.snowflake_name
}

resource "snowflake_service_user" "this" {
  count             = var.access.snowflake == null ? 0 : 1
  name              = local.snowflake_name
  default_role      = snowflake_account_role.this[0].name
  default_warehouse = var.access.snowflake.warehouse

  default_workload_identity {
    azure {
      issuer  = "https://login.microsoftonline.com/${var.tenant_id}/v2.0"
      subject = azurerm_user_assigned_identity.this.principal_id
    }
  }
}

resource "snowflake_grant_account_role" "user" {
  count     = var.access.snowflake == null ? 0 : 1
  role_name = snowflake_account_role.this[0].name
  user_name = snowflake_service_user.this[0].name
}

resource "snowflake_grant_privileges_to_account_role" "warehouse" {
  count             = var.access.snowflake == null ? 0 : 1
  account_role_name = snowflake_account_role.this[0].name
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "WAREHOUSE"
    object_name = var.access.snowflake.warehouse
  }
}

resource "snowflake_grant_privileges_to_account_role" "database" {
  for_each          = local.snowflake_databases
  account_role_name = snowflake_account_role.this[0].name
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "DATABASE"
    object_name = each.key
  }
}

resource "snowflake_grant_privileges_to_account_role" "schema" {
  for_each          = local.snowflake_schemas
  account_role_name = snowflake_account_role.this[0].name
  privileges        = each.value.access == "write" ? ["USAGE", "CREATE TABLE", "CREATE VIEW"] : ["USAGE"]
  on_schema {
    schema_name = each.value.quoted
  }
}

resource "snowflake_grant_privileges_to_account_role" "tables" {
  for_each          = local.snowflake_schemas
  account_role_name = snowflake_account_role.this[0].name
  privileges        = each.value.access == "write" ? ["SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE"] : ["SELECT"]
  on_schema_object {
    all {
      object_type_plural = "TABLES"
      in_schema          = each.value.quoted
    }
  }
}

resource "snowflake_grant_privileges_to_account_role" "future_tables" {
  for_each          = local.snowflake_schemas
  account_role_name = snowflake_account_role.this[0].name
  privileges        = each.value.access == "write" ? ["SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE"] : ["SELECT"]
  on_schema_object {
    future {
      object_type_plural = "TABLES"
      in_schema          = each.value.quoted
    }
  }
}
