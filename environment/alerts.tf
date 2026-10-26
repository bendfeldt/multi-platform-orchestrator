# Alert delivery (solution repo: docs/PRO_FEATURES_PLAN.md §4). Dagster's sensors and health
# job write one JSON line per alert to stdout; Container Apps sends it to Log Analytics; these
# rules find the lines and notify the action groups named in alerts/<env>.yaml.

locals {
  alerts        = yamldecode(file(coalesce(var.alerts_file, "${path.module}/../alerts/${var.environment}.yaml")))
  action_groups = try(local.alerts.action_groups, {})
  alert_routes  = try(local.alerts.routes, [])

  alert_kinds      = ["run_failure", "check_failed", "freshness", "tick_failure", "code_location_error", "daemon_unhealthy"]
  alert_severities = { error = 1, warning = 2 } # Azure Monitor: 0 critical ... 4 verbose
  receiver_types = [
    "email", "sms", "voice", "push", "arm_role", "secure_webhook", "event_hub", "itsm",
    "webhook", "logic_app", "function", "runbook",
  ]

  # The URL of these receivers holds a key. It is read from its owner's Key Vault at plan time
  # with the deployer's Entra login, so it never sits in git or a pipeline variable.
  keyed_receivers = merge(flatten([
    for group, receivers in local.action_groups : [
      for type in ["webhook", "logic_app", "function", "runbook"] : {
        for r in try(receivers[type], []) : "${group}/${type}/${r.name}" => r.secret
      }
    ]
  ])...)

  # One rule per route and severity, because a rule has a single severity.
  alert_rules = merge(flatten([
    for i, route in local.alert_routes : [
      for severity in coalesce(try(route.severities, null), keys(local.alert_severities)) : {
        "${i}-${severity}" = merge(route, { severity = severity })
      }
    ]
  ])...)

  # Every alert line starts like this (Python's json.dumps puts the kind first).
  alert_lines = <<-KQL
    ContainerAppConsoleLogs_CL
    | where Log_s startswith '{"mpo_alert"'
    | extend a = parse_json(Log_s)
    | extend alert_kind = tostring(a["mpo_alert"]), alert_severity = tostring(a["severity"])
  KQL
}

resource "azurerm_monitor_action_group" "this" {
  for_each = local.action_groups

  name                = "ag-${local.name}-${each.key}"
  resource_group_name = var.resource_group_name
  short_name          = each.value.short_name

  dynamic "email_receiver" {
    for_each = try(each.value.email, [])
    content {
      name                    = email_receiver.value.name
      email_address           = email_receiver.value.address
      use_common_alert_schema = true
    }
  }
  dynamic "sms_receiver" {
    for_each = try(each.value.sms, [])
    content {
      name         = sms_receiver.value.name
      country_code = sms_receiver.value.country_code
      phone_number = sms_receiver.value.phone_number
    }
  }
  dynamic "voice_receiver" {
    for_each = try(each.value.voice, [])
    content {
      name         = voice_receiver.value.name
      country_code = voice_receiver.value.country_code
      phone_number = voice_receiver.value.phone_number
    }
  }
  dynamic "azure_app_push_receiver" {
    for_each = try(each.value.push, [])
    content {
      name          = azure_app_push_receiver.value.name
      email_address = azure_app_push_receiver.value.email_address
    }
  }
  dynamic "arm_role_receiver" {
    for_each = try(each.value.arm_role, [])
    content {
      name                    = arm_role_receiver.value.name
      role_id                 = arm_role_receiver.value.role_id
      use_common_alert_schema = true
    }
  }
  dynamic "event_hub_receiver" {
    for_each = try(each.value.event_hub, [])
    content {
      name                    = event_hub_receiver.value.name
      event_hub_namespace     = event_hub_receiver.value.namespace
      event_hub_name          = event_hub_receiver.value.event_hub
      subscription_id         = try(event_hub_receiver.value.subscription_id, null)
      tenant_id               = try(event_hub_receiver.value.tenant_id, null)
      use_common_alert_schema = true
    }
  }
  dynamic "itsm_receiver" {
    for_each = try(each.value.itsm, [])
    content {
      name                 = itsm_receiver.value.name
      workspace_id         = itsm_receiver.value.workspace_id
      connection_id        = itsm_receiver.value.connection_id
      ticket_configuration = itsm_receiver.value.ticket_configuration
      region               = itsm_receiver.value.region
    }
  }
  # Secure webhooks: Azure Monitor gets an Entra token for the target app (no key anywhere).
  dynamic "webhook_receiver" {
    for_each = try(each.value.secure_webhook, [])
    content {
      name                    = webhook_receiver.value.name
      service_uri             = webhook_receiver.value.service_uri
      use_common_alert_schema = true
      aad_auth {
        object_id      = webhook_receiver.value.object_id
        identifier_uri = try(webhook_receiver.value.identifier_uri, null)
        tenant_id      = try(webhook_receiver.value.tenant_id, var.tenant_id)
      }
    }
  }
  dynamic "webhook_receiver" {
    for_each = try(each.value.webhook, [])
    content {
      name                    = webhook_receiver.value.name
      service_uri             = data.azurerm_key_vault_secret.receiver["${each.key}/webhook/${webhook_receiver.value.name}"].value
      use_common_alert_schema = true
    }
  }
  dynamic "logic_app_receiver" {
    for_each = try(each.value.logic_app, [])
    content {
      name                    = logic_app_receiver.value.name
      resource_id             = logic_app_receiver.value.resource_id
      callback_url            = data.azurerm_key_vault_secret.receiver["${each.key}/logic_app/${logic_app_receiver.value.name}"].value
      use_common_alert_schema = true
    }
  }
  dynamic "azure_function_receiver" {
    for_each = try(each.value.function, [])
    content {
      name                     = azure_function_receiver.value.name
      function_app_resource_id = azure_function_receiver.value.function_app_resource_id
      function_name            = azure_function_receiver.value.function_name
      http_trigger_url         = data.azurerm_key_vault_secret.receiver["${each.key}/function/${azure_function_receiver.value.name}"].value
      use_common_alert_schema  = true
    }
  }
  dynamic "automation_runbook_receiver" {
    for_each = try(each.value.runbook, [])
    content {
      name                    = automation_runbook_receiver.value.name
      automation_account_id   = automation_runbook_receiver.value.automation_account_id
      runbook_name            = automation_runbook_receiver.value.runbook_name
      webhook_resource_id     = automation_runbook_receiver.value.webhook_resource_id
      is_global_runbook       = try(automation_runbook_receiver.value.is_global_runbook, false)
      service_uri             = data.azurerm_key_vault_secret.receiver["${each.key}/runbook/${automation_runbook_receiver.value.name}"].value
      use_common_alert_schema = true
    }
  }

  lifecycle {
    precondition {
      condition     = length(each.value.short_name) <= 12
      error_message = "Action group ${each.key}: short_name may have at most 12 characters."
    }
    precondition {
      condition     = length(setsubtract(keys(each.value), concat(["short_name"], local.receiver_types))) == 0
      error_message = "Action group ${each.key}: unknown receiver type; use ${join(", ", local.receiver_types)}."
    }
  }
}

data "azurerm_key_vault_secret" "receiver" {
  for_each = local.keyed_receivers

  key_vault_id = each.value.key_vault_id
  name         = each.value.name
}

# The rules query as this identity, which may only read the workspace. Without one, a rule
# would run with the rights of whoever last edited it.
resource "azurerm_user_assigned_identity" "alerts" {
  name                = "id-mpo-alerts-${var.environment}"
  location            = var.location
  resource_group_name = var.resource_group_name
}

resource "azurerm_role_assignment" "alerts_logs" {
  scope                = azurerm_log_analytics_workspace.this.id
  role_definition_name = "Log Analytics Reader"
  principal_id         = azurerm_user_assigned_identity.alerts.principal_id
  principal_type       = "ServicePrincipal"
}

resource "azurerm_monitor_scheduled_query_rules_alert_v2" "route" {
  for_each = local.alert_rules

  name                = "mpo-${var.environment}-alerts-${each.key}"
  description         = "Dagster ${each.value.severity} alerts${length(try(each.value.locations, [])) > 0 ? " of ${join(", ", each.value.locations)}" : ""} to ${join(", ", each.value.action_groups)}"
  location            = var.location
  resource_group_name = var.resource_group_name
  scopes              = [azurerm_log_analytics_workspace.this.id]
  severity            = local.alert_severities[each.value.severity]
  # Azure Monitor re-evaluates when logs arrive late, so equal window and frequency neither
  # miss nor repeat lines.
  evaluation_frequency    = "PT5M"
  window_duration         = "PT5M"
  auto_mitigation_enabled = false # every new failure notifies
  # The table only exists once the first container has logged.
  skip_query_validation = true

  criteria {
    # Without blank lines: the Log Analytics editor reads a blank line as the end of a query.
    query = join("\n", compact(split("\n", <<-KQL
      ${local.alert_lines}
      | where alert_kind != "health_check" and alert_severity == "${each.value.severity}"
      | extend alert_location = coalesce(tostring(a["location"]), "-"),
          alert_subject = coalesce(tostring(a["job"]), tostring(a["asset"]), tostring(a["instigator"]), tostring(a["daemon"]), "-")
      ${length(try(each.value.locations, [])) > 0 ? "| where alert_location in (${join(", ", [for l in each.value.locations : jsonencode(l)])})" : ""}
      ${length(try(each.value.kinds, [])) > 0 ? "| where alert_kind in (${join(", ", [for k in each.value.kinds : jsonencode(k)])})" : ""}
      | project TimeGenerated, alert_kind, alert_location, alert_subject,
          alert_check = tostring(a["check"]), alert_summary = tostring(a["summary"]),
          alert_url = tostring(a["url"]), alert_run_id = tostring(a["run_id"]),
          alert_launched_by = tostring(a["launched_by"])
    KQL
    )))
    time_aggregation_method = "Count"
    operator                = "GreaterThan"
    threshold               = 0
    # Stable values only: one alert per kind, code location and job/asset. Run IDs or URLs
    # would make each evaluation a new alert.
    dimension {
      name     = "alert_kind"
      operator = "Include"
      values   = ["*"]
    }
    dimension {
      name     = "alert_location"
      operator = "Include"
      values   = ["*"]
    }
    dimension {
      name     = "alert_subject"
      operator = "Include"
      values   = ["*"]
    }
    failing_periods {
      minimum_failing_periods_to_trigger_alert = 1
      number_of_evaluation_periods             = 1
    }
  }

  action {
    action_groups = [for g in each.value.action_groups : azurerm_monitor_action_group.this[g].id]
  }

  identity {
    type         = "UserAssigned"
    identity_ids = [azurerm_user_assigned_identity.alerts.id]
  }

  depends_on = [azurerm_role_assignment.alerts_logs]

  lifecycle {
    precondition {
      condition     = contains(keys(local.alert_severities), each.value.severity)
      error_message = "Route ${each.key}: severities may only be error and warning."
    }
    precondition {
      condition     = length(setsubtract(try(each.value.kinds, []), local.alert_kinds)) == 0
      error_message = "Route ${each.key}: kinds may only be ${join(", ", local.alert_kinds)}."
    }
    precondition {
      # Names go into the query; code location names are Python identifiers.
      condition     = alltrue([for l in try(each.value.locations, []) : can(regex("^[A-Za-z_][A-Za-z0-9_]*$", l))])
      error_message = "Route ${each.key}: locations must be code location names."
    }
    precondition {
      condition     = length(each.value.action_groups) > 0
      error_message = "Route ${each.key}: name at least one action group."
    }
  }
}

# The health job writes a health_check line every 5 minutes. No line for 15 minutes means it
# (or the whole environment) stopped. Only that job's own lines count, so no code location can
# fake them.
resource "azurerm_monitor_scheduled_query_rules_alert_v2" "health_missing" {
  count = length(try(local.alerts.health_action_groups, [])) > 0 ? 1 : 0

  name                    = "mpo-${var.environment}-health-missing"
  description             = "The Dagster health check in ${var.environment} has not reported for 15 minutes."
  location                = var.location
  resource_group_name     = var.resource_group_name
  scopes                  = [azurerm_log_analytics_workspace.this.id]
  severity                = 1
  evaluation_frequency    = "PT5M"
  window_duration         = "PT15M"
  auto_mitigation_enabled = true # resolves once the check reports again
  skip_query_validation   = true

  criteria {
    query = join("\n", compact(split("\n", <<-KQL
      ${local.alert_lines}
      | where alert_kind == "health_check" and ContainerJobName_s == "dagster-health"
    KQL
    )))
    time_aggregation_method = "Count"
    operator                = "LessThan"
    threshold               = 1
    failing_periods {
      minimum_failing_periods_to_trigger_alert = 1
      number_of_evaluation_periods             = 1
    }
  }

  action {
    action_groups = [for g in local.alerts.health_action_groups : azurerm_monitor_action_group.this[g].id]
  }

  identity {
    type         = "UserAssigned"
    identity_ids = [azurerm_user_assigned_identity.alerts.id]
  }

  depends_on = [azurerm_role_assignment.alerts_logs]
}

# Container Apps' own errors: failed revisions, crash loops, failing probes
# (ContainerAppSystemLogs_CL, LogLevel_s "Error"). One alert per app; to the health groups.
resource "azurerm_monitor_scheduled_query_rules_alert_v2" "platform_errors" {
  count = length(try(local.alerts.health_action_groups, [])) > 0 ? 1 : 0

  name                    = "mpo-${var.environment}-platform-errors"
  description             = "Container Apps reported errors for a Dagster app in ${var.environment} (revisions, restarts, probes)."
  location                = var.location
  resource_group_name     = var.resource_group_name
  scopes                  = [azurerm_log_analytics_workspace.this.id]
  severity                = 2
  evaluation_frequency    = "PT5M"
  window_duration         = "PT15M"
  auto_mitigation_enabled = true
  skip_query_validation   = true

  criteria {
    query = join("\n", compact(split("\n", <<-KQL
      ContainerAppSystemLogs_CL
      | where LogLevel_s == "Error"
      | extend app = coalesce(ContainerAppName_s, column_ifexists("ContainerJobName_s", ""), "-")
      | project TimeGenerated, app, RevisionName_s, Log_s
    KQL
    )))
    time_aggregation_method = "Count"
    operator                = "GreaterThan"
    threshold               = 0
    dimension {
      name     = "app"
      operator = "Include"
      values   = ["*"]
    }
    failing_periods {
      minimum_failing_periods_to_trigger_alert = 1
      number_of_evaluation_periods             = 1
    }
  }

  action {
    action_groups = [for g in local.alerts.health_action_groups : azurerm_monitor_action_group.this[g].id]
  }

  identity {
    type         = "UserAssigned"
    identity_ids = [azurerm_user_assigned_identity.alerts.id]
  }

  depends_on = [azurerm_role_assignment.alerts_logs]
}
