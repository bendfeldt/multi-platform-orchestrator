# After the first apply: terraform init -migrate-state -backend-config=backend.hcl
# (change `backend "local"` to `backend "azurerm"` in versions.tf first).
resource_group_name  = "rg-mpo-shared"
storage_account_name = "stmposharedx7k2"
container_name       = "tfstate-bootstrap"
key                  = "bootstrap.tfstate"
use_azuread_auth     = true
