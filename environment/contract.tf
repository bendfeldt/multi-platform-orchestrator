# The environment contract the solution repo deploys from (its contract.py is the schema).
locals {
  identity = { for key, id in local.control_plane : key => {
    resource_id    = id.id
    client_id      = id.client_id
    principal_name = id.name
  } }

  contract = {
    schema_version     = 2 # 2: one webserver with Entra roles
    environment        = var.environment
    location           = var.location
    subscription_id    = var.subscription_id
    resource_group     = var.resource_group_name
    aca_environment_id = azurerm_container_app_environment.this.id
    acr_login_server   = azurerm_container_registry.this.login_server
    postgres = {
      host     = azurerm_postgresql_flexible_server.this.fqdn
      database = azurerm_postgresql_flexible_server_database.dagster.name
    }
    compute_logs = {
      account   = azurerm_storage_account.logs.name
      container = azurerm_storage_container.compute_logs.name
    }
    identities = merge(local.identity, {
      domains = { for name, domain in module.domain : name => domain.identity }
    })
    auth = {
      tenant_id        = var.tenant_id
      ui_app_client_id = azuread_application.ui.client_id
    }
    ingress = {
      hostname = azurerm_cdn_frontdoor_endpoint.ui.host_name
    }
    platforms = {
      databricks = var.databricks_host == null ? null : {
        host = var.databricks_host
        cost_warehouse_id = !local.ops_databricks ? null : coalesce(
          var.databricks_cost_warehouse_id, try(databricks_sql_endpoint.cost[0].id, null)
        )
      }
      snowflake = var.snowflake == null ? null : {
        account          = "${var.snowflake.organization}-${var.snowflake.account}"
        domains          = { for name, domain in module.domain : name => domain.snowflake if domain.snowflake != null }
        credit_price_usd = var.snowflake.credit_price_usd
      }
      fabric = var.fabric_capacity_events == null || !local.ops_domain ? null : {
        capacity_events = var.fabric_capacity_events
      }
    }
    catalog = var.purview_endpoint == null || !local.ops_domain ? null : {
      purview_endpoint = var.purview_endpoint
    }
    insights = !local.ops_domain ? null : {
      ingestion_endpoint = azurerm_monitor_data_collection_rule.insights[0].logs_ingestion_endpoint
      rule_id            = azurerm_monitor_data_collection_rule.insights[0].immutable_id
    }
  }
}

resource "azurerm_storage_blob" "contract" {
  name                 = "${var.environment}.json"
  storage_container_id = var.contract_container_id
  type                 = "Block"
  content_type         = "application/json"
  source_content       = jsonencode(local.contract)
}

output "contract" {
  value = local.contract
}
