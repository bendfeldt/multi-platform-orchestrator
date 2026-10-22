resource "azurerm_log_analytics_workspace" "this" {
  name                = "log-${local.name}"
  location            = var.location
  resource_group_name = var.resource_group_name
  retention_in_days   = var.log_retention_days
}

resource "azurerm_container_app_environment" "this" {
  name                           = "cae-${local.name}"
  location                       = var.location
  resource_group_name            = var.resource_group_name
  infrastructure_subnet_id       = azurerm_subnet.aca.id
  internal_load_balancer_enabled = true
  # Front Door Premium reaches the webservers over Private Link; nothing else gets in.
  public_network_access      = "Disabled"
  logs_destination           = "log-analytics"
  log_analytics_workspace_id = azurerm_log_analytics_workspace.this.id

  workload_profile {
    name                  = "Consumption"
    workload_profile_type = "Consumption"
  }
}

resource "azurerm_postgresql_flexible_server" "this" {
  name                          = "psql-${local.name}-${var.unique_suffix}"
  location                      = var.location
  resource_group_name           = var.resource_group_name
  version                       = "16"
  sku_name                      = var.postgres_sku
  storage_mb                    = 32768
  backup_retention_days         = 7
  delegated_subnet_id           = azurerm_subnet.postgres.id
  private_dns_zone_id           = azurerm_private_dns_zone.postgres.id
  public_network_access_enabled = false

  authentication {
    active_directory_auth_enabled = true
    password_auth_enabled         = false
    tenant_id                     = var.tenant_id
  }

  lifecycle {
    ignore_changes = [zone] # Azure picks it; changing it would recreate the server.
  }

  depends_on = [azurerm_private_dns_zone_virtual_network_link.postgres]
}

# Log every login attempt (audit who connects) and slow down repeated failed logins.
resource "azurerm_postgresql_flexible_server_configuration" "this" {
  for_each = {
    log_connections              = "on"
    log_checkpoints              = "on"
    "connection_throttle.enable" = "on"
  }
  name      = each.key
  server_id = azurerm_postgresql_flexible_server.this.id
  value     = each.value
}

resource "azurerm_postgresql_flexible_server_database" "dagster" {
  name      = "dagster"
  server_id = azurerm_postgresql_flexible_server.this.id
}

# The admin migrate job is the only Entra admin: it creates every other login.
resource "azurerm_postgresql_flexible_server_active_directory_administrator" "admin" {
  server_name         = azurerm_postgresql_flexible_server.this.name
  resource_group_name = var.resource_group_name
  tenant_id           = var.tenant_id
  object_id           = azurerm_user_assigned_identity.admin.principal_id
  principal_name      = azurerm_user_assigned_identity.admin.name
  principal_type      = "ServicePrincipal"
}

resource "azurerm_container_registry" "this" {
  name                = "acr${local.compact}"
  location            = var.location
  resource_group_name = var.resource_group_name
  sku                 = "Basic"
  admin_enabled       = false
}

resource "azurerm_storage_account" "logs" {
  name                              = "st${local.compact}"
  location                          = var.location
  resource_group_name               = var.resource_group_name
  account_tier                      = "Standard"
  account_replication_type          = "LRS"
  min_tls_version                   = "TLS1_2"
  infrastructure_encryption_enabled = true
  shared_access_key_enabled         = false
  default_to_oauth_authentication   = true
  allow_nested_items_to_be_public   = false

  network_rules {
    default_action             = "Deny"
    virtual_network_subnet_ids = [azurerm_subnet.aca.id]
    bypass                     = ["AzureServices"]
  }
}

resource "azurerm_storage_container" "compute_logs" {
  name               = "compute-logs"
  storage_account_id = azurerm_storage_account.logs.id
}
