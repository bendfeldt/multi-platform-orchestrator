variable "name" {
  description = "Domain name; equals the code location's name in the solution repo."
  type        = string
}

variable "environment" {
  type = string
}

variable "location" {
  type = string
}

variable "resource_group_name" {
  type = string
}

variable "tenant_id" {
  type = string
}

variable "access" {
  description = "The domain's YAML file (domains/<env>/<name>.yaml): what it may read and write."
  type = object({
    # Entra groups with the domain's Dagster roles (assigned in environment/entra.tf).
    launchers = optional(list(string), [])
    editors   = optional(list(string), [])
    databricks = optional(object({
      schemas = list(object({ name = string, access = string })) # name: catalog.schema
    }))
    fabric = optional(object({
      workspaces = list(object({ id = string, access = string }))
    }))
    snowflake = optional(object({
      warehouse = string
      schemas   = list(object({ name = string, access = string })) # name: DATABASE.SCHEMA
    }))
  })

  validation {
    condition = alltrue([
      for item in concat(
        try(var.access.databricks.schemas, []),
        try(var.access.fabric.workspaces, []),
        try(var.access.snowflake.schemas, []),
      ) : contains(["read", "write"], item.access)
    ])
    error_message = "Every access must be \"read\" or \"write\"."
  }
}

variable "acr_id" {
  type = string
}

variable "compute_log_container_id" {
  description = "Resource Manager ID of the Blob container runs write their logs to."
  type        = string
}

variable "identity_operator_principal_ids" {
  description = "May attach this domain's identity to containers: the daemon (run Jobs) and the app deployer."
  type        = map(string)
}
