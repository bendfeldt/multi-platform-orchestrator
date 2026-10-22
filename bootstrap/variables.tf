variable "tenant_id" {
  type = string
}

variable "shared_subscription_id" {
  description = "Holds Terraform state, the contracts and the shared Front Door profile."
  type        = string
}

variable "subscriptions" {
  description = "Subscription per environment; give the same ID three times for one subscription."
  type        = object({ dev = string, test = string, prod = string })
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
  description = "Short lowercase string that makes the storage account name globally unique."
  type        = string
}

variable "github" {
  type = object({
    owner         = string
    infra_repo    = string
    solution_repo = string
  })
  default = null
}

variable "azure_devops" {
  type = object({
    organization_id = string
    organization    = string
    project         = string
  })
  default = null
}
