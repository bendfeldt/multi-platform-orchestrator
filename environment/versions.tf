terraform {
  required_version = ">= 1.11"

  required_providers {
    azurerm    = { source = "hashicorp/azurerm", version = "~> 5.10" }
    azuread    = { source = "hashicorp/azuread", version = "~> 3.10" }
    databricks = { source = "databricks/databricks", version = "~> 1.139" }
    snowflake  = { source = "snowflakedb/snowflake", version = "~> 2.22" }
    fabric     = { source = "microsoft/fabric", version = "~> 1.14" }
  }

  # Per environment: -backend-config=envs/<env>.backend.hcl (Entra auth, no storage keys).
  backend "azurerm" {}
}

# Every provider logs in as the infra deployer: GitHub OIDC or the ADO service connection via
# `az login`, so no provider holds a secret.
provider "azurerm" {
  features {}
  subscription_id     = var.subscription_id
  storage_use_azuread = true
}

provider "azuread" {
  tenant_id = var.tenant_id
}

provider "databricks" {
  host      = var.databricks_host
  auth_type = "azure-cli"
}

provider "snowflake" {
  organization_name          = var.snowflake == null ? null : var.snowflake.organization
  account_name               = var.snowflake == null ? null : var.snowflake.account
  user                       = var.snowflake == null ? null : var.snowflake.infra_user
  role                       = var.snowflake == null ? null : var.snowflake.infra_role
  authenticator              = "WORKLOAD_IDENTITY"
  workload_identity_provider = "AZURE"
}

provider "fabric" {
  use_cli = true
}
