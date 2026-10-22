# State in the shared state account (shared/), one container per environment, Entra auth.
resource_group_name  = "rg-mpo-shared"
storage_account_name = "stmposharedx7k2"
container_name       = "tfstate-dev"
key                  = "environment.tfstate"
use_azuread_auth     = true
