# Run once by a person with Owner on the subscriptions and rights to create Entra apps and
# grant Graph app roles. Afterwards only the pipelines change anything.
terraform {
  required_version = ">= 1.11"

  required_providers {
    azurerm = { source = "hashicorp/azurerm", version = "~> 5.10" }
    azuread = { source = "hashicorp/azuread", version = "~> 3.10" }
  }

  # First run uses local state; then `terraform init -migrate-state -backend-config=backend.hcl`
  # moves it into the storage account this root creates.
  backend "local" {}
}

provider "azurerm" {
  features {}
  subscription_id     = var.shared_subscription_id
  storage_use_azuread = true
}

# dev, test and prod are fixed, so each gets a provider for its subscription (the same
# subscription for all is fine: resource groups separate them).
provider "azurerm" {
  alias = "dev"
  features {}
  subscription_id = var.subscriptions.dev
}

provider "azurerm" {
  alias = "test"
  features {}
  subscription_id = var.subscriptions.test
}

provider "azurerm" {
  alias = "prod"
  features {}
  subscription_id = var.subscriptions.prod
}

provider "azuread" {
  tenant_id = var.tenant_id
}
