variable "environment" {
  type = string
  validation {
    condition     = contains(["dev", "test", "prod"], var.environment)
    error_message = "environment is dev, test or prod."
  }
}

variable "tenant_id" {
  type = string
}

variable "subscription_id" {
  description = "Own subscription per environment, or the same one for all (resource groups separate them)."
  type        = string
}

variable "resource_group_name" {
  description = "Created by bootstrap/, which also scopes the deployers' rights to it."
  type        = string
}

variable "location" {
  type    = string
  default = "westeurope"
}

variable "prefix" {
  type    = string
  default = "mpo"
}

variable "unique_suffix" {
  description = "Short lowercase string that makes global names (ACR, storage, Postgres) unique."
  type        = string
}

variable "vnet_address_space" {
  type    = string
  default = "10.40.0.0/16"
}

variable "postgres_sku" {
  type    = string
  default = "B_Standard_B1ms"
}

variable "log_retention_days" {
  type    = number
  default = 30
}

variable "waf_mode" {
  description = "Detection while tuning (dev/test), Prevention in prod."
  type        = string
  default     = "Prevention"
}

variable "frontdoor_profile_id" {
  description = "The shared Front Door Premium profile (shared/)."
  type        = string
}

variable "app_deployer_principal_id" {
  description = "Service principal of the solution repo's deployer for this environment (bootstrap/)."
  type        = string
}

variable "contract_container_id" {
  description = "Resource Manager ID of this environment's contract container (bootstrap/)."
  type        = string
}

variable "image_policy_definition_id" {
  description = "Policy that only allows images from this environment's ACR (bootstrap/)."
  type        = string
}

variable "acr_pull_principal_ids" {
  description = "dev only: the test and prod app deployers, which copy dev's images."
  type        = map(string)
  default     = {}
}

variable "viewer_group_ids" {
  description = "Entra groups that may sign in and read everything (role Dagster.Viewer)."
  type        = list(string)
}

variable "admin_group_ids" {
  description = "Entra groups with every right in Dagster (role Dagster.Admin)."
  type        = list(string)
}

variable "databricks_host" {
  description = "Existing Databricks workspace URL; null if no domain uses Databricks."
  type        = string
  default     = null
}

variable "snowflake" {
  description = "Existing Snowflake account and the infra deployer's Snowflake login (snowflake/bootstrap.sql); null if unused."
  type = object({
    organization = string
    account      = string
    infra_user   = string
    infra_role   = string
    # Your contract's price per credit, for cost in USD; without it cost is in credits.
    credit_price_usd = optional(number)
  })
  default = null
}

variable "databricks_cost_warehouse_id" {
  description = "Existing SQL warehouse for mpo_ops's billing query (its owner gives mpo_ops CAN_USE); null creates a small serverless one."
  type        = string
  default     = null
}

variable "alerts_file" {
  description = "Alert routing file; default alerts/<environment>.yaml at the repository root."
  type        = string
  default     = null
}

variable "fabric_capacity_events" {
  description = "Eventhouse with Fabric capacity operation events (made outside Terraform, fabric/capacity_operations.kql) for the cost report; null if unused."
  type = object({
    query_uri = string # the KQL database's Query URI
    database  = string
    # Your capacity's price per CU hour, for Fabric cost in USD; without it cost is in CU.
    cu_hour_price_usd = optional(number)
  })
  default = null
}
