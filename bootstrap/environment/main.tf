# One environment: its resource group, the image policy, and the two deployers that log in
# with federated credentials (no secrets). Powers stay inside this environment.

resource "azurerm_resource_group" "this" {
  name     = "rg-${var.prefix}-${var.environment}"
  location = var.location
}

# Container Apps and Jobs may only use images from the given registry (PLAN R4). The
# environment root assigns it with its own ACR. Spike: confirm the aliases below.
resource "azurerm_policy_definition" "images" {
  name         = "${var.prefix}-images-${var.environment}"
  display_name = "Container Apps use images from the environment's registry only"
  policy_type  = "Custom"
  mode         = "All"
  parameters = jsonencode({
    registry = { type = "String", metadata = { displayName = "Registry login server" } }
  })
  policy_rule = jsonencode({
    if = {
      anyOf = [for type in ["containerApps", "jobs"] : {
        allOf = [
          { field = "type", equals = "Microsoft.App/${type}" },
          {
            count = {
              field = "Microsoft.App/${type}/template.containers[*]"
              where = {
                field   = "Microsoft.App/${type}/template.containers[*].image"
                notLike = "[concat(parameters('registry'), '/*')]"
              }
            }
            greater = 0
          },
        ]
      }]
    }
    then = { effect = "deny" }
  })
}

locals {
  deployers = {
    infra = { name = "${var.prefix}-infra-${var.environment}", repo = try(var.github.infra_repo, null) }
    app   = { name = "${var.prefix}-app-${var.environment}", repo = try(var.github.solution_repo, null) }
  }
  credentials = merge(
    var.github == null ? {} : { for key, d in local.deployers : "${key}-github" => {
      deployer = key
      issuer   = "https://token.actions.githubusercontent.com"
      subject  = "repo:${var.github.owner}/${d.repo}:environment:${var.environment}"
    } },
    var.azure_devops == null ? {} : { for key, d in local.deployers : "${key}-ado" => {
      deployer = key
      issuer   = "https://vstoken.dev.azure.com/${var.azure_devops.organization_id}"
      # The service connection is named like the deployer, e.g. mpo-infra-dev / mpo-app-dev.
      subject = "sc://${var.azure_devops.organization}/${var.azure_devops.project}/${d.name}"
    } },
  )
}

resource "azuread_application" "deployer" {
  for_each     = local.deployers
  display_name = each.value.name
}

resource "azuread_service_principal" "deployer" {
  for_each  = local.deployers
  client_id = azuread_application.deployer[each.key].client_id
}

resource "azuread_application_federated_identity_credential" "deployer" {
  for_each       = local.credentials
  application_id = azuread_application.deployer[each.value.deployer].id
  display_name   = each.key
  issuer         = each.value.issuer
  subject        = each.value.subject
  audiences      = ["api://AzureADTokenExchange"]
}

# ---- Infra deployer: builds this environment's resource group and nothing outside it.

resource "azurerm_role_assignment" "infra_rg" {
  for_each = toset([
    "Contributor",
    "Role Based Access Control Administrator", # grants the narrow roles in environment/
    "Resource Policy Contributor",             # assigns the image policy
  ])
  scope                = azurerm_resource_group.this.id
  role_definition_name = each.key
  principal_id         = azuread_service_principal.deployer["infra"].object_id
  principal_type       = "ServicePrincipal"
}

resource "azurerm_role_assignment" "infra_shared" {
  for_each = {
    state     = { scope = var.state_container_id, role = "Storage Blob Data Contributor" }
    contract  = { scope = var.contract_container_id, role = "Storage Blob Data Contributor" }
    frontdoor = { scope = var.frontdoor_profile_id, role = "CDN Profile Contributor" }
  }
  scope                = each.value.scope
  role_definition_name = each.value.role
  principal_id         = azuread_service_principal.deployer["infra"].object_id
  principal_type       = "ServicePrincipal"
}

# The infra deployer creates the two sign-in apps. Application.ReadWrite.OwnedBy limits it to
# apps it owns; it cannot grant itself other permissions.
data "azuread_service_principal" "graph" {
  client_id = "00000003-0000-0000-c000-000000000000"
}

resource "azuread_app_role_assignment" "infra_graph" {
  app_role_id         = data.azuread_service_principal.graph.app_role_ids["Application.ReadWrite.OwnedBy"]
  principal_object_id = azuread_service_principal.deployer["infra"].object_id
  resource_object_id  = data.azuread_service_principal.graph.object_id
}

# ---- App deployer: reads the contract here; environment/ grants its Container Apps rights.

resource "azurerm_role_assignment" "app_contract" {
  scope                = var.contract_container_id
  role_definition_name = "Storage Blob Data Reader"
  principal_id         = azuread_service_principal.deployer["app"].object_id
  principal_type       = "ServicePrincipal"
}
