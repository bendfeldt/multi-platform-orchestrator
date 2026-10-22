# Who may log in as which deployer, and where the infra deployer's rights end.

mock_provider "azurerm" {
  mock_resource "azurerm_storage_container" {
    override_during = plan
    defaults        = { id = "/subscriptions/00000000-0000-0000-0000-000000000001/resourceGroups/rg-mpo-shared/providers/Microsoft.Storage/storageAccounts/st/blobServices/default/containers/c" }
  }
  mock_resource "azurerm_cdn_frontdoor_profile" {
    override_during = plan
    defaults        = { id = "/subscriptions/00000000-0000-0000-0000-000000000001/resourceGroups/rg-mpo-shared/providers/Microsoft.Cdn/profiles/afd-mpo" }
  }
}
mock_provider "azurerm" {
  alias = "dev"
  mock_resource "azurerm_resource_group" {
    override_during = plan
    defaults        = { id = "/subscriptions/00000000-0000-0000-0000-000000000001/resourceGroups/rg-mpo-env" }
  }
}
mock_provider "azurerm" {
  alias = "test"
  mock_resource "azurerm_resource_group" {
    override_during = plan
    defaults        = { id = "/subscriptions/00000000-0000-0000-0000-000000000001/resourceGroups/rg-mpo-env" }
  }
}
mock_provider "azurerm" {
  alias = "prod"
  mock_resource "azurerm_resource_group" {
    override_during = plan
    defaults        = { id = "/subscriptions/00000000-0000-0000-0000-000000000001/resourceGroups/rg-mpo-env" }
  }
}
mock_provider "azuread" {
  mock_data "azuread_service_principal" {
    defaults = {
      object_id    = "00000000-0000-0000-0000-0000000000a1"
      app_role_ids = { "Application.ReadWrite.OwnedBy" = "18a4783c-866b-4cc7-a460-3d5e5662c884" }
    }
  }
  mock_resource "azuread_application" {
    override_during = plan
    defaults        = { id = "/applications/00000000-0000-0000-0000-0000000000a2", client_id = "00000000-0000-0000-0000-0000000000a3" }
  }
  mock_resource "azuread_service_principal" {
    override_during = plan
    defaults        = { object_id = "00000000-0000-0000-0000-0000000000a4" }
  }
}

variables {
  tenant_id              = "00000000-0000-0000-0000-000000000000"
  shared_subscription_id = "00000000-0000-0000-0000-000000000001"
  subscriptions = {
    dev  = "00000000-0000-0000-0000-000000000001"
    test = "00000000-0000-0000-0000-000000000001"
    prod = "00000000-0000-0000-0000-000000000002"
  }
  unique_suffix = "x7k2"
  github        = { owner = "bendfeldt", infra_repo = "mpo-infra", solution_repo = "mpo" }
  azure_devops  = { organization_id = "11111111-1111-1111-1111-111111111111", organization = "org", project = "data" }
}

run "each_repo_logs_in_only_from_its_own_environment" {
  command = plan

  assert {
    condition     = module.prod.credentials["infra-github"].subject == "repo:bendfeldt/mpo-infra:environment:prod"
    error_message = "The prod infra deployer must trust only the infra repo's prod environment."
  }
  assert {
    condition     = module.dev.credentials["app-github"].subject == "repo:bendfeldt/mpo:environment:dev"
    error_message = "The dev app deployer must trust only the solution repo's dev environment."
  }
  assert {
    condition     = module.test.credentials["app-ado"].subject == "sc://org/data/mpo-app-test"
    error_message = "ADO service connections are named after their deployer."
  }
}

run "only_github_when_ado_is_unused" {
  command = plan
  variables {
    azure_devops = null
  }
  assert {
    condition     = toset(keys(module.dev.credentials)) == toset(["app-github", "infra-github"])
    error_message = "No ADO credential may exist when ADO is not configured."
  }
}
