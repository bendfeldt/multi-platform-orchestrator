# Cost and insight rows from mpo_ops (solution repo: docs/PRO_FEATURES_PLAN.md §5): two
# Log Analytics tables, a data collection rule mpo_ops sends them through with its managed
# identity (no key), and a workbook over them. Only created when the mpo_ops domain exists.

locals {
  ops_domain = contains(keys(local.domains), "mpo_ops")
  # Column types as Log Analytics queries name them; the table API spells datetime "dateTime".
  insight_tables = jsondecode(file("${path.module}/insights/tables.json"))
  workbook_queries = {
    for f in fileset("${path.module}/insights", "*.kql") :
    trimsuffix(f, ".kql") => file("${path.module}/insights/${f}")
  }
}

resource "azurerm_log_analytics_workspace_table_custom_log" "insights" {
  for_each = local.ops_domain ? local.insight_tables : {}

  name         = each.key
  workspace_id = azurerm_log_analytics_workspace.this.id

  dynamic "column" {
    for_each = each.value
    content {
      name = column.key
      type = column.value == "datetime" ? "dateTime" : column.value
    }
  }
}

resource "azurerm_monitor_data_collection_rule" "insights" {
  count = local.ops_domain ? 1 : 0

  name                = "dcr-${local.name}-insights"
  location            = var.location
  resource_group_name = var.resource_group_name
  # Direct: the rule has its own logs ingestion endpoint, no data collection endpoint needed.
  kind = "Direct"

  destinations {
    log_analytics {
      name                  = "workspace"
      workspace_resource_id = azurerm_log_analytics_workspace.this.id
    }
  }

  dynamic "stream_declaration" {
    for_each = local.insight_tables
    content {
      stream_name = "Custom-${stream_declaration.key}"
      dynamic "column" {
        for_each = stream_declaration.value
        content {
          name = column.key
          type = column.value
        }
      }
    }
  }

  dynamic "data_flow" {
    for_each = local.insight_tables
    content {
      streams       = ["Custom-${data_flow.key}"]
      destinations  = ["workspace"]
      transform_kql = "source"
      output_stream = "Custom-${data_flow.key}"
    }
  }

  depends_on = [azurerm_log_analytics_workspace_table_custom_log.insights]
}

# mpo_ops may send rows through this rule and nothing else.
resource "azurerm_role_assignment" "ops_ingestion" {
  count = local.ops_domain ? 1 : 0

  scope                = azurerm_monitor_data_collection_rule.insights[0].id
  role_definition_name = "Monitoring Metrics Publisher"
  principal_id         = module.domain["mpo_ops"].principal_id
  principal_type       = "ServicePrincipal"
}

resource "azurerm_application_insights_workbook" "insights" {
  count = local.ops_domain ? 1 : 0

  name                = uuidv5("url", "https://mpo/${var.environment}/workbook/insights")
  display_name        = "Dagster cost and reliability (${var.environment})"
  location            = var.location
  resource_group_name = var.resource_group_name
  source_id           = lower(azurerm_log_analytics_workspace.this.id)
  category            = "workbook"
  data_json = jsonencode({
    version = "Notebook/1.0"
    items = concat(
      [{
        type = 1
        name = "intro"
        content = {
          json = "Daily rows from the `mpo_cost` and `mpo_insights` jobs. Cost is attributed per asset from Snowflake query tags and Databricks job runs; `Unattributed` is cost of runs that wrote no asset."
        }
      }],
      [for name, query in local.workbook_queries : {
        type = 3
        name = name
        content = {
          version       = "KqlItem/1.0"
          title         = replace(name, "_", " ")
          query         = query
          size          = 0
          timeContext   = { durationMs = 2592000000 } # 30 days
          queryType     = 0
          resourceType  = "microsoft.operationalinsights/workspaces"
          visualization = "table"
        }
      }]
    )
    fallbackResourceIds = [lower(azurerm_log_analytics_workspace.this.id)]
    "$schema"           = "https://github.com/Microsoft/Application-Insights-Workbooks/blob/master/schema/workbook.json"
  })
}

# ---- Databricks: a SQL warehouse for mpo_ops's billing query (system.billing), unless an
# existing one is named. Terraform owns this one, so it may set its whole permission list.

locals {
  ops_databricks = local.ops_domain && try(local.domains["mpo_ops"].databricks, null) != null
}

resource "databricks_sql_endpoint" "cost" {
  count = local.ops_databricks && var.databricks_cost_warehouse_id == null ? 1 : 0

  name                      = "mpo-cost-${var.environment}"
  cluster_size              = "2X-Small"
  max_num_clusters          = 1
  enable_serverless_compute = true # starts in seconds for a once-a-day query
  warehouse_type            = "PRO"
  auto_stop_mins            = 10
}

resource "databricks_permissions" "cost_warehouse" {
  count = length(databricks_sql_endpoint.cost)

  sql_endpoint_id = databricks_sql_endpoint.cost[0].id
  access_control {
    service_principal_name = module.domain["mpo_ops"].identity.client_id
    permission_level       = "CAN_USE"
  }
}
