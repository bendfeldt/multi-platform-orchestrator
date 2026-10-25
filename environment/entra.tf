# One sign-in app per environment. Its app roles are what the webserver enforces per code
# location (solution repo: orchestrator_core/access.py):
#   Dagster.Viewer, Dagster.Admin, Dagster.Launcher.<domain>, Dagster.Editor.<domain>
# Groups get roles: viewers/admins from variables, launchers/editors from the domain files.
# Group-to-role assignment needs Entra ID P1 or P2.
locals {
  ui_roles = merge(
    {
      "Dagster.Viewer" = "Read everything in Dagster"
      "Dagster.Admin"  = "Everything, including workspace-wide settings"
    },
    { for name, _ in local.domains : "Dagster.Launcher.${name}" => "Launch and terminate runs of ${name}" },
    { for name, _ in local.domains : "Dagster.Editor.${name}" => "Change everything in ${name}" },
  )
  ui_assignments = merge(
    { for id in var.viewer_group_ids : "Dagster.Viewer/${id}" => { role = "Dagster.Viewer", group = id } },
    { for id in var.admin_group_ids : "Dagster.Admin/${id}" => { role = "Dagster.Admin", group = id } },
    merge(flatten([for name, domain in local.domains : [
      { for id in try(domain.launchers, []) : "Dagster.Launcher.${name}/${id}" => { role = "Dagster.Launcher.${name}", group = id } },
      { for id in try(domain.editors, []) : "Dagster.Editor.${name}/${id}" => { role = "Dagster.Editor.${name}", group = id } },
    ]])...),
  )
}

resource "azuread_application" "ui" {
  display_name     = "mpo-ui-${var.environment}"
  sign_in_audience = "AzureADMyOrg"

  web {
    redirect_uris = ["https://${azurerm_cdn_frontdoor_endpoint.ui.host_name}/.auth/login/aad/callback"]
    implicit_grant {
      # Container Apps sign-in without a client secret (Spike E confirms).
      id_token_issuance_enabled = true
    }
  }

  dynamic "app_role" {
    for_each = local.ui_roles
    content {
      allowed_member_types = ["User"]
      display_name         = app_role.key
      description          = app_role.value
      value                = app_role.key
      # Stable per role and environment, so re-plans never recreate a role.
      id = uuidv5("url", "https://mpo/${var.environment}/${app_role.key}")
    }
  }
}

resource "azuread_service_principal" "ui" {
  client_id                    = azuread_application.ui.client_id
  app_role_assignment_required = true # only users in an assigned group can sign in
}

resource "azuread_app_role_assignment" "ui" {
  for_each            = local.ui_assignments
  app_role_id         = azuread_application.ui.app_role_ids[each.value.role]
  principal_object_id = each.value.group
  resource_object_id  = azuread_service_principal.ui.object_id
}
