variable "environment" {
  type = string
}

variable "location" {
  type = string
}

variable "prefix" {
  type = string
}

variable "github" {
  description = "Federated login for GitHub Actions; null if GitHub is not used."
  type = object({
    owner         = string
    infra_repo    = string
    solution_repo = string
  })
}

variable "azure_devops" {
  description = "Federated login for Azure DevOps service connections; null if ADO is not used."
  type = object({
    organization_id = string # the GUID in https://vstoken.dev.azure.com/<organization_id>
    organization    = string
    project         = string
  })
}

variable "state_container_id" {
  type = string
}

variable "contract_container_id" {
  type = string
}

variable "frontdoor_profile_id" {
  type = string
}
