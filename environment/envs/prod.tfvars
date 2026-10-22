# Example values: replace every ID with your own (outputs of bootstrap/ and shared/).
environment         = "prod"
tenant_id           = "00000000-0000-0000-0000-000000000000"
subscription_id     = "00000000-0000-0000-0000-000000000001"
resource_group_name = "rg-mpo-prod"
unique_suffix       = "x7k2"
postgres_sku        = "B_Standard_B2s"
waf_mode            = "Prevention"

frontdoor_profile_id       = "/subscriptions/00000000-0000-0000-0000-000000000001/resourceGroups/rg-mpo-shared/providers/Microsoft.Cdn/profiles/afd-mpo"
app_deployer_principal_id  = "00000000-0000-0000-0000-00000000000a"
contract_container_id      = "/subscriptions/00000000-0000-0000-0000-000000000001/resourceGroups/rg-mpo-shared/providers/Microsoft.Storage/storageAccounts/stmposharedx7k2/blobServices/default/containers/contract-prod"
image_policy_definition_id = "/subscriptions/00000000-0000-0000-0000-000000000001/providers/Microsoft.Authorization/policyDefinitions/mpo-images"
operators_group_id         = "00000000-0000-0000-0000-00000000000b"
viewers_group_id           = "00000000-0000-0000-0000-00000000000c"

databricks_host = "https://adb-0000000000000000.0.azuredatabricks.net"
snowflake = {
  organization = "myorg"
  account      = "sales_prod"
  infra_user   = "MPO_INFRA_PROD"
  infra_role   = "MPO_INFRA_PROD"
}
