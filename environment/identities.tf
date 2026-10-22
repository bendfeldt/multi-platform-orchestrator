# Control-plane identities (docs/PLAN.md §5.1). Domain identities live in modules/domain_access.

resource "azurerm_user_assigned_identity" "platform" {
  name                = "id-dagster-platform-${var.environment}"
  location            = var.location
  resource_group_name = var.resource_group_name
}

resource "azurerm_user_assigned_identity" "webserver" {
  name                = "id-dagster-web-${var.environment}"
  location            = var.location
  resource_group_name = var.resource_group_name
}

resource "azurerm_user_assigned_identity" "admin" {
  name                = "id-dagster-admin-${var.environment}"
  location            = var.location
  resource_group_name = var.resource_group_name
}

locals {
  control_plane = {
    platform  = azurerm_user_assigned_identity.platform
    webserver = azurerm_user_assigned_identity.webserver
    admin     = azurerm_user_assigned_identity.admin
  }
}

resource "azurerm_role_assignment" "acr_pull" {
  for_each             = local.control_plane
  scope                = azurerm_container_registry.this.id
  role_definition_name = "AcrPull"
  principal_id         = each.value.principal_id
  principal_type       = "ServicePrincipal"
}

# The daemon creates one Job per run; nothing else in the resource group.
resource "azurerm_role_definition" "run_launcher" {
  name  = "mpo-run-launcher-${var.environment}"
  scope = data.azurerm_resource_group.this.id
  permissions {
    actions = [
      "Microsoft.App/jobs/*",
      "Microsoft.App/managedEnvironments/read",
      "Microsoft.App/managedEnvironments/join/action",
    ]
  }
}

resource "azurerm_role_assignment" "run_launcher" {
  scope              = data.azurerm_resource_group.this.id
  role_definition_id = azurerm_role_definition.run_launcher.role_definition_resource_id
  principal_id       = azurerm_user_assigned_identity.platform.principal_id
  principal_type     = "ServicePrincipal"
}

# The cleanup job lists and deletes finished run Jobs.
resource "azurerm_role_definition" "run_cleanup" {
  name  = "mpo-run-cleanup-${var.environment}"
  scope = data.azurerm_resource_group.this.id
  permissions {
    actions = [
      "Microsoft.App/jobs/read",
      "Microsoft.App/jobs/delete",
      "Microsoft.App/jobs/executions/read",
    ]
  }
}

resource "azurerm_role_assignment" "run_cleanup" {
  scope              = data.azurerm_resource_group.this.id
  role_definition_id = azurerm_role_definition.run_cleanup.role_definition_resource_id
  principal_id       = azurerm_user_assigned_identity.admin.principal_id
  principal_type     = "ServicePrincipal"
}

resource "azurerm_role_assignment" "webserver_logs" {
  scope                = azurerm_storage_container.compute_logs.id
  role_definition_name = "Storage Blob Data Reader"
  principal_id         = azurerm_user_assigned_identity.webserver.principal_id
  principal_type       = "ServicePrincipal"
}

# ---- The solution repo's deployer: images and container apps in this resource group only.

resource "azurerm_role_definition" "app_deployer" {
  name  = "mpo-app-deployer-${var.environment}"
  scope = data.azurerm_resource_group.this.id
  permissions {
    actions = [
      "Microsoft.App/containerApps/*",
      "Microsoft.App/jobs/*",
      "Microsoft.App/managedEnvironments/read",
      "Microsoft.App/managedEnvironments/join/action",
      "Microsoft.Resources/subscriptions/resourceGroups/read",
      # az acr login reads the registry; az acr import copies dev's images in (AcrPush can't).
      "Microsoft.ContainerRegistry/registries/read",
      "Microsoft.ContainerRegistry/registries/importImage/action",
    ]
  }
}

resource "azurerm_role_assignment" "app_deployer" {
  scope              = data.azurerm_resource_group.this.id
  role_definition_id = azurerm_role_definition.app_deployer.role_definition_resource_id
  principal_id       = var.app_deployer_principal_id
  principal_type     = "ServicePrincipal"
}

resource "azurerm_role_assignment" "app_deployer_acr" {
  for_each             = toset(["AcrPush", "AcrPull"])
  scope                = azurerm_container_registry.this.id
  role_definition_name = each.key
  principal_id         = var.app_deployer_principal_id
  principal_type       = "ServicePrincipal"
}

resource "azurerm_role_assignment" "app_deployer_identities" {
  for_each             = local.control_plane
  scope                = each.value.id
  role_definition_name = "Managed Identity Operator"
  principal_id         = var.app_deployer_principal_id
  principal_type       = "ServicePrincipal"
}

resource "azurerm_role_assignment" "promotion_pull" {
  for_each             = var.acr_pull_principal_ids
  scope                = azurerm_container_registry.this.id
  role_definition_name = "AcrPull"
  principal_id         = each.value
  principal_type       = "ServicePrincipal"
}
