locals {
  environments = toset(["dev", "test", "prod"])
}

resource "azurerm_resource_group" "shared" {
  name     = "rg-${var.prefix}-shared"
  location = var.location
}

# Terraform state and the environment contracts. Entra auth only; one container per
# environment so one environment's deployer cannot touch another's state or contract.
# shortcut: reachable from the internet because hosted CI runners have no fixed IPs; keys are
# disabled, so only Entra identities with a role on a container get in. Add network rules
# when the pipelines move to self-hosted runners with fixed IPs.
#trivy:ignore:AVD-AZU-0012
resource "azurerm_storage_account" "shared" {
  name                              = lower("st${var.prefix}shared${var.unique_suffix}")
  location                          = var.location
  resource_group_name               = azurerm_resource_group.shared.name
  account_tier                      = "Standard"
  account_replication_type          = "ZRS"
  min_tls_version                   = "TLS1_2"
  infrastructure_encryption_enabled = true
  shared_access_key_enabled         = false
  default_to_oauth_authentication   = true
  allow_nested_items_to_be_public   = false

  blob_properties {
    versioning_enabled = true
    delete_retention_policy {
      days = 30
    }
  }
}

resource "azurerm_storage_container" "state" {
  for_each           = setunion(local.environments, ["bootstrap"])
  name               = "tfstate-${each.key}"
  storage_account_id = azurerm_storage_account.shared.id
}

resource "azurerm_storage_container" "contract" {
  for_each           = local.environments
  name               = "contract-${each.key}"
  storage_account_id = azurerm_storage_account.shared.id
}

# One Premium profile for all environments ($330/month once); each environment adds its own
# endpoints, origins and WAF policy in environment/.
resource "azurerm_cdn_frontdoor_profile" "shared" {
  name                = "afd-${var.prefix}"
  resource_group_name = azurerm_resource_group.shared.name
  sku_name            = "Premium_AzureFrontDoor"
}

module "dev" {
  source    = "./environment"
  providers = { azurerm = azurerm.dev }

  environment           = "dev"
  location              = var.location
  prefix                = var.prefix
  github                = var.github
  azure_devops          = var.azure_devops
  state_container_id    = azurerm_storage_container.state["dev"].id
  contract_container_id = azurerm_storage_container.contract["dev"].id
  frontdoor_profile_id  = azurerm_cdn_frontdoor_profile.shared.id
}

module "test" {
  source    = "./environment"
  providers = { azurerm = azurerm.test }

  environment           = "test"
  location              = var.location
  prefix                = var.prefix
  github                = var.github
  azure_devops          = var.azure_devops
  state_container_id    = azurerm_storage_container.state["test"].id
  contract_container_id = azurerm_storage_container.contract["test"].id
  frontdoor_profile_id  = azurerm_cdn_frontdoor_profile.shared.id
}

module "prod" {
  source    = "./environment"
  providers = { azurerm = azurerm.prod }

  environment           = "prod"
  location              = var.location
  prefix                = var.prefix
  github                = var.github
  azure_devops          = var.azure_devops
  state_container_id    = azurerm_storage_container.state["prod"].id
  contract_container_id = azurerm_storage_container.contract["prod"].id
  frontdoor_profile_id  = azurerm_cdn_frontdoor_profile.shared.id
}

output "shared" {
  value = {
    state_storage_account  = azurerm_storage_account.shared.name
    resource_group_name    = azurerm_resource_group.shared.name
    frontdoor_profile_id   = azurerm_cdn_frontdoor_profile.shared.id
    contract_container_ids = { for env, c in azurerm_storage_container.contract : env => c.id }
    contract_urls = {
      for env in local.environments :
      env => "${azurerm_storage_account.shared.primary_blob_endpoint}contract-${env}/${env}.json"
    }
  }
}

output "environments" {
  value = { dev = module.dev.values, test = module.test.values, prod = module.prod.values }
}
