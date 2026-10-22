# Two sign-in apps per environment, so a viewer cannot even sign in to the full webserver.
locals {
  ui_apps = {
    full     = { name = "mpo-ui-${var.environment}", host = azurerm_cdn_frontdoor_endpoint.full.host_name }
    readonly = { name = "mpo-ui-ro-${var.environment}", host = azurerm_cdn_frontdoor_endpoint.readonly.host_name }
  }
  ui_groups = {
    full     = { operators = var.operators_group_id }
    readonly = { operators = var.operators_group_id, viewers = var.viewers_group_id }
  }
  ui_assignments = merge([
    for app, groups in local.ui_groups : { for name, id in groups : "${app}-${name}" => { app = app, group = id } }
  ]...)
}

resource "azuread_application" "ui" {
  for_each         = local.ui_apps
  display_name     = each.value.name
  sign_in_audience = "AzureADMyOrg"
  web {
    redirect_uris = ["https://${each.value.host}/.auth/login/aad/callback"]
    implicit_grant {
      # Container Apps sign-in without a client secret (Spike E confirms).
      id_token_issuance_enabled = true
    }
  }
}

resource "azuread_service_principal" "ui" {
  for_each                     = local.ui_apps
  client_id                    = azuread_application.ui[each.key].client_id
  app_role_assignment_required = true
}

resource "azuread_app_role_assignment" "ui" {
  for_each            = local.ui_assignments
  app_role_id         = "00000000-0000-0000-0000-000000000000" # default access
  principal_object_id = each.value.group
  resource_object_id  = azuread_service_principal.ui[each.value.app].object_id
}
